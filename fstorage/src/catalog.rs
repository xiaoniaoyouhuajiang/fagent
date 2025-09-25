use crate::config::StorageConfig;
use crate::errors::Result;
use crate::models::{ApiBudget, EntityReadiness};
use rusqlite::{params, Connection};
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
}
