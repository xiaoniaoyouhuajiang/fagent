use crate::config::StorageConfig;
use crate::errors::Result;
use crate::fetch::EntityCategory;
use crate::models::{ApiBudget, EntityReadiness, IngestionOffset, SourceAnchor};
use rusqlite::{params, Connection};
use serde_json;
use std::sync::{Arc, Mutex};

pub struct Catalog {
    conn: Arc<Mutex<Connection>>,
}

impl Catalog {
    pub fn new(config: &StorageConfig) -> Result<Self> {
        let conn = Connection::open(&config.catalog_path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn initialize_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS entity_readiness (
                entity_uri TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                last_synced_at INTEGER,
                ttl_seconds INTEGER,
                coverage_metrics TEXT
            );
            CREATE TABLE IF NOT EXISTS api_budget (
                api_endpoint TEXT PRIMARY KEY,
                requests_left INTEGER NOT NULL,
                reset_time INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS task_logs (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                start_time INTEGER NOT NULL,
                end_time INTEGER,
                status TEXT,
                details TEXT
            );
            CREATE TABLE IF NOT EXISTS ingestion_offsets (
                table_path TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                category TEXT NOT NULL,
                primary_keys TEXT NOT NULL,
                last_version INTEGER NOT NULL DEFAULT -1
            );
            CREATE TABLE IF NOT EXISTS source_anchors (
                entity_uri TEXT NOT NULL,
                fetcher TEXT NOT NULL,
                anchor_key TEXT NOT NULL,
                anchor_value TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (entity_uri, fetcher, anchor_key)
            );
            COMMIT;",
        )?;
        Ok(())
    }

    pub fn get_readiness(&self, entity_uri: &str) -> Result<Option<EntityReadiness>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT entity_uri, entity_type, last_synced_at, ttl_seconds, coverage_metrics FROM entity_readiness WHERE entity_uri = ?1",
        )?;
        let mut rows = stmt.query(params![entity_uri])?;

        if let Some(row) = rows.next()? {
            Ok(Some(EntityReadiness {
                entity_uri: row.get(0)?,
                entity_type: row.get(1)?,
                last_synced_at: row.get(2)?,
                ttl_seconds: row.get(3)?,
                coverage_metrics: row.get(4)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_readiness(&self, readiness: &EntityReadiness) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO entity_readiness (entity_uri, entity_type, last_synced_at, ttl_seconds, coverage_metrics)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(entity_uri) DO UPDATE SET
                entity_type = excluded.entity_type,
                last_synced_at = excluded.last_synced_at,
                ttl_seconds = excluded.ttl_seconds,
                coverage_metrics = excluded.coverage_metrics",
            params![
                readiness.entity_uri,
                readiness.entity_type,
                readiness.last_synced_at,
                readiness.ttl_seconds,
                readiness.coverage_metrics,
            ],
        )?;
        Ok(())
    }

    pub fn get_api_budget(&self, endpoint: &str) -> Result<Option<ApiBudget>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT api_endpoint, requests_left, reset_time FROM api_budget WHERE api_endpoint = ?1",
        )?;
        let mut rows = stmt.query(params![endpoint])?;

        if let Some(row) = rows.next()? {
            Ok(Some(ApiBudget {
                api_endpoint: row.get(0)?,
                requests_left: row.get(1)?,
                reset_time: row.get(2)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_api_budget(&self, budget: &ApiBudget) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO api_budget (api_endpoint, requests_left, reset_time)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(api_endpoint) DO UPDATE SET
                requests_left = excluded.requests_left,
                reset_time = excluded.reset_time",
            params![budget.api_endpoint, budget.requests_left, budget.reset_time],
        )?;
        Ok(())
    }

    pub fn create_task_log(&self, task_name: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let start_time = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO task_logs (task_name, start_time, status) VALUES (?1, ?2, 'RUNNING')",
            params![task_name, start_time],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn update_task_log_status(&self, task_id: i64, status: &str, details: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let end_time = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE task_logs SET status = ?1, details = ?2, end_time = ?3 WHERE task_id = ?4",
            params![status, details, end_time, task_id],
        )?;
        Ok(())
    }

    pub fn ensure_ingestion_offset(
        &self,
        table_path: &str,
        entity_type: &str,
        category: EntityCategory,
        primary_keys: &[String],
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let pk_json = serde_json::to_string(primary_keys)?;
        conn.execute(
            "INSERT INTO ingestion_offsets (table_path, entity_type, category, primary_keys)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(table_path) DO UPDATE SET
                entity_type = excluded.entity_type,
                category = excluded.category,
                primary_keys = excluded.primary_keys",
            params![table_path, entity_type, category.as_str(), pk_json],
        )?;
        Ok(())
    }

    pub fn get_ingestion_offset(&self, table_path: &str) -> Result<Option<IngestionOffset>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT table_path, entity_type, category, primary_keys, last_version
             FROM ingestion_offsets WHERE table_path = ?1",
        )?;
        let mut rows = stmt.query(params![table_path])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::map_ingestion_offset_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_ingestion_offsets(&self) -> Result<Vec<IngestionOffset>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT table_path, entity_type, category, primary_keys, last_version FROM ingestion_offsets",
        )?;
        let mut results = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            results.push(Self::map_ingestion_offset_row(row)?);
        }
        Ok(results)
    }

    pub fn update_ingestion_offset(&self, table_path: &str, last_version: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE ingestion_offsets SET last_version = ?1 WHERE table_path = ?2",
            params![last_version, table_path],
        )?;
        Ok(())
    }

    fn map_ingestion_offset_row(row: &rusqlite::Row<'_>) -> Result<IngestionOffset> {
        let table_path: String = row.get(0)?;
        let entity_type: String = row.get(1)?;
        let category_str: String = row.get(2)?;
        let primary_keys_json: String = row.get(3)?;
        let last_version: i64 = row.get(4)?;
        let primary_keys: Vec<String> = serde_json::from_str(&primary_keys_json)?;
        let category = category_str.parse()?;
        Ok(IngestionOffset {
            table_path,
            entity_type,
            category,
            primary_keys,
            last_version,
        })
    }

    pub fn get_source_anchor(
        &self,
        entity_uri: &str,
        fetcher: &str,
        anchor_key: &str,
    ) -> Result<Option<SourceAnchor>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT entity_uri, fetcher, anchor_key, anchor_value, updated_at
             FROM source_anchors
             WHERE entity_uri = ?1 AND fetcher = ?2 AND anchor_key = ?3",
        )?;
        let mut rows = stmt.query(params![entity_uri, fetcher, anchor_key])?;
        if let Some(row) = rows.next()? {
            Ok(Some(SourceAnchor {
                entity_uri: row.get(0)?,
                fetcher: row.get(1)?,
                anchor_key: row.get(2)?,
                anchor_value: row.get(3)?,
                updated_at: row.get(4)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_source_anchor(
        &self,
        entity_uri: &str,
        fetcher: &str,
        anchor_key: &str,
        anchor_value: Option<&str>,
        updated_at: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO source_anchors (entity_uri, fetcher, anchor_key, anchor_value, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(entity_uri, fetcher, anchor_key) DO UPDATE SET
                anchor_value = excluded.anchor_value,
                updated_at = excluded.updated_at",
            params![entity_uri, fetcher, anchor_key, anchor_value, updated_at],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::models::EntityReadiness;
    use tempfile::tempdir;

    fn setup() -> (Catalog, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        let catalog = Catalog::new(&config).unwrap();
        catalog.initialize_schema().unwrap();
        (catalog, dir)
    }

    #[test]
    fn test_readiness_crud() {
        let (catalog, _dir) = setup();

        // Test insert
        let readiness = EntityReadiness {
            entity_uri: "test_uri".to_string(),
            entity_type: "repo".to_string(),
            last_synced_at: Some(12345),
            ttl_seconds: Some(3600),
            coverage_metrics: "{}".to_string(),
        };
        let result = catalog.upsert_readiness(&readiness);
        assert!(result.is_ok());

        // Test get
        let fetched = catalog.get_readiness("test_uri").unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.entity_uri, "test_uri");
        assert_eq!(fetched.last_synced_at, Some(12345));

        // Test update
        let updated_readiness = EntityReadiness {
            entity_uri: "test_uri".to_string(),
            entity_type: "repo".to_string(),
            last_synced_at: Some(54321),
            ttl_seconds: Some(3600),
            coverage_metrics: "{}".to_string(),
        };
        let result = catalog.upsert_readiness(&updated_readiness);
        assert!(result.is_ok());

        let fetched_updated = catalog.get_readiness("test_uri").unwrap().unwrap();
        assert_eq!(fetched_updated.last_synced_at, Some(54321));
    }

    #[test]
    fn test_task_log_crud() {
        let (catalog, _dir) = setup();

        // Test create
        let task_id = catalog.create_task_log("test_task");
        assert!(task_id.is_ok());
        let task_id = task_id.unwrap();
        assert_eq!(task_id, 1);

        // Test update
        let result = catalog.update_task_log_status(task_id, "SUCCESS", "Done");
        assert!(result.is_ok());
    }

    #[test]
    fn test_ingestion_offsets_crud() {
        let (catalog, _dir) = setup();

        let ensure = catalog.ensure_ingestion_offset(
            "silver/entities/project",
            "project",
            crate::fetch::EntityCategory::Node,
            &vec!["url".to_string()],
        );
        assert!(ensure.is_ok());

        let offset = catalog
            .get_ingestion_offset("silver/entities/project")
            .unwrap()
            .unwrap();
        assert_eq!(offset.table_path, "silver/entities/project");
        assert_eq!(offset.entity_type, "project");
        assert_eq!(offset.category, crate::fetch::EntityCategory::Node);
        assert_eq!(offset.primary_keys, vec!["url".to_string()]);
        assert_eq!(offset.last_version, -1);

        let list = catalog.list_ingestion_offsets().unwrap();
        assert_eq!(list.len(), 1);

        catalog
            .update_ingestion_offset("silver/entities/project", 5)
            .unwrap();
        let updated = catalog
            .get_ingestion_offset("silver/entities/project")
            .unwrap()
            .unwrap();
        assert_eq!(updated.last_version, 5);
    }
}
