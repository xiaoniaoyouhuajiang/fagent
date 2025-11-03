# fagent

[中文版本](README.zh-CN.md)

## Overview

- **Motivation**  
  Build an agent-friendly retrieval stack that understands source code and repository context, so LLM-based assistants can ingest Git data, synchronize it incrementally, and answer questions reliably.

- **Key Modules**
  - `fagent`: orchestrator hosting the HTTP API and dashboard, coordinating sync tasks, and exposing graph/query endpoints.
  - `gitfetcher`: fetch layer that talks to GitHub via Octocrab, extracts repository metadata, issues, pull requests, code graphs, and vectors, and emits `GraphData`.
  - `fstorage`: storage core that writes data into Delta Lake (cold) and Helix engine (hot), maintains schema metadata, catalog offsets, and search APIs.
  - Tooling (`capture`, `fstorage_cli`, dashboard): utilities for debugging, fixture generation, and interactive exploration.

## Architecture

- **Module Interaction**
  1. The dashboard or HTTP API (fagent) receives a sync request with fetch parameters.
  2. fagent delegates the job to `FStorageSynchronizer`, which invokes the selected fetcher (for example, `gitfetcher`) to produce `GraphData`.
  3. `fstorage` writes nodes, edges, and vectors to Delta (`Lake`) and Helix (`HelixGraphEngine`), keeping offsets and indexes in sync.
  4. The Catalog records ingestion metadata to support incremental replays.
  5. Query APIs (neighbors, subgraph, hybrid search) fuse hot and cold data for downstream consumers.

- **fstorage Internals**
  - **Lake**: Delta writer/reader that normalizes table paths such as `silver/entities/*`, `silver/edges/*`, and `silver/vectors/*`, writing batches atomically.
  - **Helix Engine**: hot-path graph store for low-latency node/edge/vector queries.
  - **Catalog**: tracks ingestion offsets, vector index tables, and external anchors for incremental sync planning.
  - **Schema Registry & `vector_rules.json`**: configuration-driven metadata that keeps vector IDs stable and the pipeline generic.
  - **Synchronizer**: orchestrates fetch → validate → write → offset update for each entity category.

## Current Capabilities

- **fagent-dashboard**  
  Web UI that triggers syncs, monitors progress, and explores the knowledge graph (search, graph visualization with filters).

- **capture** (`cargo run -p gitfetcher --bin capture`)  
  CLI to fetch real GitHub data and persist `FetchResponse` as Arrow/JSON fixtures for offline tests and reproducible debugging.

- **fstorage_cli** (`cargo run -p fstorage --bin fstorage_cli -- --base-path <path> …`)  
  Inspect hot/cold storage: list Delta tables, query Helix nodes/edges/vectors, and compare counts to validate sync consistency.

## Getting Started

1. Install dependencies (Rust toolchain, optional `cargo-instruments`, Helix prerequisites).
2. Prepare a workspace and set environment variables such as `GITHUB_TOKEN` (plus `USE_LSP`, proxy settings if needed).
3. Run `cargo run -p fagent -- dashboard --base-path ./temp` and open the dashboard to trigger syncs.
4. Use `capture` and `fstorage_cli` to generate fixtures or debug storage contents.
5. Explore graph data through `/graph.html`, `/api/graph` endpoints, or the search APIs.

---

For detailed design notes and ADRs, see the documents under `references/`.
