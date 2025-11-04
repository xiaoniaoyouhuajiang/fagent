# FStorage Module Overview

This document explains how the `fstorage` crate structures data, how its instance is composed, why it relies on Delta Lake and Helix Engine, and which public APIs it exposes. It also describes the fetcher abstraction and the role `fstorage` plays as the active data layer for `fagent`.

## Defining a Data Model

`fstorage` consumes declarative schemas instead of inlined Rust structs:

1. **Graph schemas** live in `helixdb-cfg/schema.hx`. Each update defines nodes, edges, vectors, and their properties. Running `cargo run -p fstorage --bin schema_gen` (or the project’s build script) regenerates strongly typed `Fetchable` structs in `target/.../generated_schemas.rs`.
2. **Vector rules** are declared in `fstorage/vector_rules.json`. They map vector entities to:
   - The edge label that links a vector back to its source node.
   - The index table (`silver/index_vector/<entity>`) used for embedding-id deduplication.
   - Optional metadata such as source node type.
3. **Catalog metadata** tracks ingestion offsets for every table so incremental syncs remain idempotent.

Together these layers keep the runtime generic: adding a new entity or edge is done through schema files, not by editing `fstorage`’s core logic.

## Instance Composition

Creating `FStorage::new(config)` wires the following components:

| Component | Responsibility |
| --- | --- |
| `StorageConfig` | File-system layout for the Lake (Delta tables), Engine (Helix LMDB), and Catalog (SQLite). |
| `Catalog` | Tracks ingestion offsets, source anchors, readiness metrics, and provides CRUD helpers. |
| `Lake` | Handles Delta reads/writes, cold-path lookups, and higher-level graph queries. |
| `HelixGraphEngine` | The hot-path graph store (LMDB + HNSW) used for low-latency traversal and vector search. |
| `FStorageSynchronizer` | Applies `Fetcher` output batches into the lake/engine, guaranteeing idempotent upserts. |
| Embedding provider | Chooses OpenAI, FastEmbed, or Null provider for vector generation. |

The layout on disk separates **cold** (`silver/*` Delta tables) and **hot** (Helix LMDB) data, enabling batch ETL, incremental replay, and constant-time graph traversals.

## Why Delta Lake + Helix Engine

*Delta Lake* offers:

- ACID guarantees and schema evolution for batch ingestion.
- Efficient parquet-backed storage that scales for historical snapshots.
- SQL access via DataFusion for analytical queries and ingestion offsets.

*Helix Engine* adds:

- Low-latency node/edge lookups with prefix iteration.
- Built-in traversal operators (`neighbors`, `subgraph_bfs`, `shortest_path`).
- Integrated HNSW vector index and BM25 inverted index for search.
- The ability to merge hot writes back to Delta via replay.

The combination allows `fstorage` to treat Delta as the system of record while Helix serves indexed, real-time graph queries.

## Core FStorage APIs

All APIs are asynchronous and return `Result<…>` in the crate’s error domain.

| API | Description | Typical Use |
| --- | --- | --- |
| `list_fetchers_capability()` | Enumerates registered fetchers with capability metadata. | Dashboard introspection. |
| `list_known_entities()` | Returns catalog offsets (table path, primary keys, version). | Auditing and readiness checks. |
| `list_tables(prefix)` | Lists Delta tables and their columns under a prefix. | Schema inspection tools. |
| `get_readiness(entities)` | Computes freshness and probe status for tasks. | Scheduling syncs. |
| `search_text_bm25(entity_type, query, limit)` | BM25 text search across a node type. | Keyword ranking. |
| `search_vectors(entity_type, vector, limit)` | Pure vector nearest-neighbour search. | Similarity lookup with external embeddings. |
| `search_vectors_by_text(entity_type, query, limit)` | Text → embedding → vector search pipeline. | Single-call semantic search. |
| `search_hybrid(entity_type, query, alpha, limit)` | BM25 + vector hybrid scoring for one entity type. | Balanced relevance retrieval. |
| `search_hybrid_multi(entity_types, query, alpha, limit)` | Hybrid search across multiple entity kinds with summary extraction. | Cross-entity answer generation. |
| `neighbors(node_id, edge_filters, direction, limit)` | Returns adjacent edges/nodes, with optional label filters. | Local graph exploration. |
| `subgraph_bfs(start_id, edge_types, depth, node_limit, edge_limit)` | Bounded breadth-first traversal with node/edge caps. | Graph visualization and inspection. |
| `shortest_path(from_id, to_id, edge_label)` | Helix shortest-path computation, optionally constrained to a label. | Finding connecting stories between entities. |
| `get_node_by_id(id, hint)` / `get_node_by_keys(entity, keys)` | Lookup via Helix or Delta index fallbacks. | Resolving user selections or stable IDs. |
| `embed_texts(texts)` | Batch embedding generation via the configured provider. | Query-time semantic search. |
| `register_fetcher(fetcher)` | Registers a new fetcher implementation with the synchronizer. | Extending the ETL pipeline. |

Most APIs are surfaced again through `fagent`’s HTTP routes (e.g. `/api/graph/subgraph`, `/api/search/hybrid_all`).

## Fetcher Abstraction

A fetcher describes a data source in three phases:

1. **Capability** — static metadata (`FetcherCapability`): parameter schema (JSON), datasets produced, TTL hints, and examples.
2. **Probe** — lightweight freshness check returning `ProbeReport`, usually comparing remote anchors with catalog anchors.
3. **Fetch** — produces `FetchResponse::GraphData` (nodes/edges/vectors) or `FetchResponse::PanelData` (Delta-ready record batch).

`GraphData` batches are schema-driven: each generated struct implements `Fetchable`, so fetchers simply push typed vectors into the batch. The synchronizer handles:

- Stable ID derivation (node primary keys, vector embedding IDs).
- Upserts into Delta and Helix in a single transaction.
- Index-table maintenance (`silver/index/*`, `silver/index_vector/*`).
- Vector → source edge construction based on `vector_rules.json`.

This keeps fetchers small and declarative, while guaranteeing ingestion semantics (idempotency, incremental replay).

## FStorage as FAgent’s Active Data Layer

`fagent` relies on `fstorage` for:

- **Incremental sync orchestration** — the synchronizer, catalog, and fetcher probes decide when data needs refreshing.
- **Rich query primitives** — BM25, vectors, hybrid search, neighbors, BFS, shortest path, and SQL-style table scans all available through one API surface.
- **Consistent IDs and indexes** — stable UUIDs for nodes, vector indices, and index tables simplify downstream linking.
- **Introspection APIs** — readiness checks, capability listings, and entity metadata power the dashboard and planner tooling.
- **Hot/cold cohesion** — Helix handles conversational latency, while Delta retains historical completeness.

The result is a storage layer that can ingest heterogeneous “facts” from multiple fetchers, serve them to LLM agents in milliseconds, and backfill cold storage without bespoke glue code.

## Further Reading

- `helixdb-cfg/schema.hx` — primary schema definitions.
- `fstorage/vector_rules.json` — vector edge/index rules.
- `fstorage/src/sync.rs` — synchronizer implementation.
- `README.md` and `README.zh-CN.md` — high-level project overview.
