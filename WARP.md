# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Commands

- Build (no top-level Cargo workspace; use per-crate manifest paths):
  ```bash path=null start=null
  cargo build --manifest-path fagent/Cargo.toml
  cargo build --manifest-path fstorage/Cargo.toml
  cargo build --manifest-path gitfetcher/Cargo.toml
  cargo build --manifest-path econfetcher/Cargo.toml
  ```

- Format and lint:
  ```bash path=null start=null
  cargo fmt --manifest-path fagent/Cargo.toml
  cargo fmt --manifest-path fstorage/Cargo.toml
  cargo fmt --manifest-path gitfetcher/Cargo.toml
  cargo fmt --manifest-path econfetcher/Cargo.toml
  
  cargo clippy --manifest-path fagent/Cargo.toml --all-targets --all-features -D warnings
  cargo clippy --manifest-path fstorage/Cargo.toml --all-targets --all-features -D warnings
  cargo clippy --manifest-path gitfetcher/Cargo.toml --all-targets --all-features -D warnings
  cargo clippy --manifest-path econfetcher/Cargo.toml --all-targets --all-features -D warnings
  ```

- Run unit/integration tests per crate:
  ```bash path=null start=null
  cargo test --manifest-path fstorage/Cargo.toml
  cargo test --manifest-path gitfetcher/Cargo.toml
  cargo test --manifest-path fagent/Cargo.toml
  cargo test --manifest-path econfetcher/Cargo.toml
  ```

- Run a single test (example):
  ```bash path=null start=null
  cargo test --manifest-path fstorage/Cargo.toml query_api_covers_hot_and_cold_paths
  ```

- Run ignored end-to-end sync test (requires network and GITHUB_TOKEN):
  ```bash path=null start=null
  GITHUB_TOKEN={{GITHUB_TOKEN}} cargo test --manifest-path fagent/Cargo.toml e2e_sync -- --ignored --nocapture
  ```

- Run the fagent dashboard HTTP service (from README):
  ```bash path=null start=null
  mkdir -p temp
  GITHUB_TOKEN={{GITHUB_TOKEN}} cargo run --manifest-path fagent/Cargo.toml -- dashboard --base-path ./temp --bind 127.0.0.1:3000
  ```

- Local debug run (enables LSP and uses temp as base path):
  ```bash path=null start=null
  mkdir -p temp
  USE_LSP=1 GITHUB_TOKEN={{GITHUB_TOKEN}} cargo run --release --manifest-path fagent/Cargo.toml -- dashboard --base-path ./temp
  # Open http://127.0.0.1:3000 in your browser
  ```

- fstorage CLI (inspect cold/hot stores):
  ```bash path=null start=null
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- --help
  # Examples
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- cold list --prefix silver/ --counts
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- hot stats
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- hot neighbors --id <UUID> --direction both --limit 10
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- catalog --json
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- summary --json
  ```

- gitfetcher capture utility (persist fetch responses as Arrow/JSON fixtures):
  ```bash path=null start=null
  GITHUB_TOKEN={{GITHUB_TOKEN}} \
  cargo run --manifest-path gitfetcher/Cargo.toml --bin capture -- \
    --params '{"mode":"repo_snapshot","repo":"owner/name","include_code":true}' \
    --output-dir ./captures --emit-json --token "$GITHUB_TOKEN"
  ```

- econfetcher example (World Bank API):
  ```bash path=null start=null
  cargo run --manifest-path econfetcher/Cargo.toml --example world_bank_example
  ```

## Architecture and repository structure

High-level overview of the multi-crate Rust project. There is no Cargo workspace; crates are siblings under the repo root.

- fagent (binary + library)
  - Purpose: HTTP dashboard service (Axum) that fronts the storage engine and exposes APIs for status, table listing, graph queries, readiness checks, and triggering syncs.
  - Key endpoints (all under /api):
    - GET /api/status, /api/fetchers, /api/tables
    - GET /api/graph/{overview,types,search,subgraph,node,visual}
    - POST /api/readiness, /api/sync
  - Static dashboard UI is bundled at compile time and served from the same process (index.html, graph.html, styles.css, app.js, graph.js).
  - Starts an in-process FStorage instance and may register fetchers (GitFetcher) if not disabled.

- fstorage (library + CLI binaries)
  - Role: Unified storage layer providing both a “cold” tabular store and a “hot” graph/vector store.
  - Data layout and config:
    - Base path is provided via StorageConfig::new(base_path) producing: lake/ (Delta tables), catalog.sqlite (SQLite metadata), engine/ (HelixDB storage).
  - Schema-driven codegen:
    - Build script parses helixdb-cfg/schema.hx and generates strongly-typed entity/edge/vector Rust structs under src/schemas.rs (via OUT_DIR include).
    - These types implement Fetchable, defining table names (e.g. silver/entities/<entity> or silver/vectors/<vector>), primary keys, and categories (node/edge/vector).
  - Cold path (Delta Lake via deltalake + DataFusion):
    - write_batches merges or overwrites into Delta tables.
    - Helper queries: list_tables(prefix), table_sql, query_table; plus a lightweight catalog (ingestion_offsets, readiness, budgets, logs).
  - Hot path (HelixDB):
    - FStorageSynchronizer updates the embedded graph engine incrementally from RecordBatch data; supports BM25 text search and HNSW vector search.
    - Embeddings: chooses provider at runtime
      - OPENAI_API_KEY set → OpenAI embeddings
      - else FastEmbed local model → vectors generated locally
      - else NullEmbeddingProvider → vectors empty but pipeline remains functional
  - Public surface:
    - FStorage::new(config) initializes catalog, lake, engine, synchronizer.
    - DataSynchronizer trait: register_fetcher, list_fetcher_capabilities, check_readiness, sync, process_graph_data, run_full_etl_from_lake.
  - CLI binaries:
    - fstorage_cli: inspect cold tables, hot graph stats/nodes/neighbors/subgraphs, and catalog/summary (JSON-capable).
    - id_debug: prints a stable UUID for a sample node key (dev utility).

- gitfetcher (library + binaries)
  - Role: Data ingestion from GitHub; returns unified GraphData/RecordBatch packages to fstorage.
  - Uses octocrab (rustls), supports repo snapshots and other datasets; integrates with fstorage schemas.
  - capture binary: executes a fetch using provided params/token and persists outputs as Arrow and optional JSON.

- econfetcher (library + examples)
  - Role: Async World Bank Data API client with types for countries, indicators, series, topics, sources, regions; examples in examples/.

- helixdb-cfg (config and DSL)
  - schema.hx: authoritative schema for nodes, edges, and vectors used by build-time codegen in fstorage.
  - queries.hx: HelixDB query definitions intended for traversal and batch operations.
  - config.hx.json: HelixDB engine configuration (vector/graph parameters) for deployments that consume it.

## Environment and runtime notes

- Required for GitHub-based flows: set GITHUB_TOKEN in the environment when using gitfetcher or running the dashboard with GitFetcher enabled.
- Optional for embeddings: set OPENAI_API_KEY to enable remote embeddings; otherwise FastEmbed local model is used if available.
- fagent dashboard requires an existing base directory (it will create lake/, catalog.sqlite, engine/ under it on first run). You can also supply FSTORAGE_BASE_PATH for the fstorage_cli tool.

## Testing notes

- Most tests are self-contained and operate on temporary directories; they do not require network access.
- fagent/tests/e2e_sync.rs is ignored by default and exercises a full sync pipeline against GitHub; enable with --ignored and provide GITHUB_TOKEN if needed.

## README highlights

- From README.md, a minimal run sequence for the dashboard:
  ```bash path=null start=null
  mkdir temp
  export GITHUB_TOKEN=...
  cargo run -p fagent -- dashboard --base-path ./temp
  ```
  Prefer the per-manifest form (no workspace):
  ```bash path=null start=null
  GITHUB_TOKEN={{GITHUB_TOKEN}} cargo run --manifest-path fagent/Cargo.toml -- dashboard --base-path ./temp
  ```

## Product vision and agent workflow (from references/prompt.txt)

- Goal: a Rust-based agent that acquires external data and analyzes it to answer user questions.
- Orchestrator: fagent (LLM-driven) plans tasks across fstorage capabilities and iterates until a satisfactory answer; follow the loop Introspect → Explore → DraftPlan → Resolve → Execute → Synthesize.
- UI: prioritize a dashboard-like component in fagent to exercise fstorage APIs early (graph exploration, capabilities, data queries).
- Storage strategy: fstorage as an “active data layer” with a data readiness probe strategy to minimize redundant fetching; uses cold Delta Lake as the source of truth plus a hot HelixDB engine for graph/vector, with BM25 + HNSW and mixed graph–vector search; helixc generates Rust structs from helixql schema for fetchers to target.

## Debugging and observability utilities

- gitfetcher capture: offline-capture FetchResponse to Arrow and optional JSON for quick inspection; outputs under captures/<name>/{nodes,edges,vectors} with metadata.json.
  ```bash path=null start=null
  GITHUB_TOKEN={{GITHUB_TOKEN}} \
  cargo run --manifest-path gitfetcher/Cargo.toml --bin capture -- \
    --params '{"mode":"repo_snapshot","repo":"Byron/dua-cli","include_code":true}' \
    --output-dir captures/dua-cli --emit-json --token "$GITHUB_TOKEN"
  ```
- fstorage_cli: one-stop “cold/hot/catalog” inspection for an existing base path; common flows:
  ```bash path=null start=null
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- --base-path temp cold list --counts
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- --base-path temp hot nodes --label project --limit 5 --json
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- --base-path temp hot neighbors --id <UUID>
  cargo run --manifest-path fstorage/Cargo.toml --bin fstorage_cli -- --base-path temp summary --json
  ```
