# FStorage 模块概览

本文档介绍 `fstorage` 的数据模型定义方式、实例组成结构、为何选择 Delta Lake 与 Helix Engine、向外暴露的核心 API，以及 fetcher 抽象和 `fstorage` 在 `fagent` 中作为活性数据层的作用。

## 如何定义数据模型

`fstorage` 通过声明式配置驱动数据模型：

1. **图数据 schema** 位于 `helixdb-cfg/schema.hx`。在其中定义节点、边、向量及其字段后，执行 `cargo run -p fstorage --bin schema_gen`（或项目的构建脚本）即可生成 `target/.../generated_schemas.rs` 中的强类型 `Fetchable` 结构体。
2. **向量规则** 存放于 `fstorage/vector_rules.json`，用于描述：
   - 向量实体与源节点之间的边标签。
   - 去重索引表 `silver/index_vector/<entity>` 与 `embedding_id` 的映射。
   - 可选的源节点类型等元信息。
3. **Catalog 元数据** 负责记录每张表的增量 offset，保证同步过程幂等。

因此，当新增实体或边时，只需修改 schema 文件即可，无需进入 `fstorage` 内部源码。

## FStorage 实例的组成

调用 `FStorage::new(config)` 会初始化以下组件：

| 组件 | 作用 |
| --- | --- |
| `StorageConfig` | 约定 Lake（Delta 表）、Engine（Helix LMDB）与 Catalog（SQLite）的目录结构。 |
| `Catalog` | 维护 ingestion offset、source anchor、readiness 指标，并提供读写接口。 |
| `Lake` | 负责 Delta 的读写、冷路径查找及高阶图查询。 |
| `HelixGraphEngine` | 热路径图存储（基于 LMDB + HNSW），提供低延迟遍历与搜索。 |
| `FStorageSynchronizer` | 将 fetcher 产出的批次写入 lake/engine，确保 upsert 幂等。 |
| 向量嵌入提供者 | 根据环境选择 OpenAI、FastEmbed 或 Null 后端，用于生成查询向量。 |

磁盘布局将 **冷数据**（`silver/*` Delta 表）与 **热数据**（Helix LMDB）分离，使批处理、增量回放与实时查询可以同时进行。

## 使用 Delta Lake 与 Helix Engine 的原因

**Delta Lake** 提供：

- 支持 ACID 的批量写入与 schema 演进能力。
- 基于 Parquet 的高效存储，适合持久化历史数据快照。
- 结合 DataFusion 的 SQL 能力，便于检查表结构与 ingestion offset。

**Helix Engine** 则带来：

- 低延迟的节点/边查询与前缀遍历。
- 内建的图遍历算子（`neighbors`、`subgraph_bfs`、`shortest_path`）。
- 集成 HNSW 向量索引与 BM25 倒排索引。
- 支持将热路径写入回放至 Delta，实现冷热数据闭环。

两者配合，让 Delta 作为事实存储，Helix 负责毫秒级查询。

## 核心 API

以下 API 均返回 `Result<…>`，详细实现在 `fstorage/src/lib.rs` 与 `fstorage/src/lake.rs`：

| API | 描述 | 常见用途 |
| --- | --- | --- |
| `list_fetchers_capability()` | 列出所有已注册 fetcher 的能力信息。 | 仪表盘、编排层能力发现。 |
| `list_known_entities()` | 返回 Catalog 中记录的实体/边表 offset。 | 数据健康检查、可视化。 |
| `list_tables(prefix)` | 列举 Delta 表及其字段。 | 查看存储结构、调试 schema。 |
| `get_readiness(entities)` | 根据 anchor 与 offset 判断需要刷新哪些数据。 | 调度同步计划。 |
| `search_text_bm25(entity_type, query, limit)` | 针对指定实体类型的 BM25 文本搜索。 | 关键词检索。 |
| `search_vectors(entity_type, vector, limit)` | 纯向量相似度搜索。 | 无需文本的语义匹配。 |
| `search_vectors_by_text(entity_type, query, limit)` | 文本 → 嵌入 → 向量检索的快捷链路。 | 单次调用完成语义搜索。 |
| `search_hybrid(entity_type, query, alpha, limit)` | BM25 与向量的单类型混合排序。 | 平衡词匹配与语义相似度。 |
| `search_hybrid_multi(entity_types, query, alpha, limit)` | 多实体类型的混合检索并生成摘要。 | QA、跨类型回答生成。 |
| `neighbors(node_id, edge_filters, direction, limit)` | 支持方向与标签过滤的邻居查询。 | 图谱局部扩展。 |
| `subgraph_bfs(start_id, edge_types, depth, node_limit, edge_limit)` | 带深度与节点/边上限的 BFS。 | 图谱可视化、探索。 |
| `shortest_path(from_id, to_id, edge_label)` | Helix 最短路径算法，可选过滤边标签。 | 追踪实体间的最短联系。 |
| `get_node_by_id(id, hint)` / `get_node_by_keys(entity, keys)` | 结合热路径与 Delta 索引的节点查找。 | 根据稳定 ID / 主键回表。 |
| `embed_texts(texts)` | 调用当前嵌入后端生成向量。 | 语义查询、数据增强。 |
| `register_fetcher(fetcher)` | 注册新的 fetcher 实现。 | 扩展数据来源。 |

这些 API 也通过 `fagent` 的 HTTP 路由开放，例如 `/api/graph/subgraph`、`/api/search/hybrid_all` 等。

## Fetcher 抽象

每个 fetcher 都遵循统一的生命周期：

1. **Capability**：返回 `FetcherCapability`，描述可支持的参数（JSON Schema）、产出数据集、默认 TTL，以及使用示例。
2. **Probe**：通过 `probe(params)` 获取远端锚点、缺口估计等，快速判断是否需要同步。
3. **Fetch**：返回 `FetchResponse::GraphData` 或 `FetchResponse::PanelData`。其中 `GraphData` 由生成的 `Fetchable` 结构体组成，实现者只需 push 对应实体即可。

同步器会负责：

- 根据主键或 `embedding_id` 推导稳定 ID，确保幂等写入。
- 在单事务内更新 Delta 与 Helix。
- 维护索引表（`silver/index/*`, `silver/index_vector/*`）。
- 按 `vector_rules.json` 构建向量与源节点之间的边。

因此 fetcher 可以专注于抓取“事实”，无须关心底层存储细节。

## 作为 fagent 活性数据层的理由

`fagent` 使用 `fstorage` 的原因包括：

- **增量同步能力**：Catalog / anchors / fetcher probe 共同决定何时刷新数据。
- **丰富查询算子**：BM25、向量、混合检索、邻居、BFS、最短路径、SQL 等统一由 `fstorage` 提供。
- **稳定 ID 与索引体系**：节点 UUID、向量索引和主键索引保证链路一致。
- **自描述接口**：`list_fetchers_capability`、`list_known_entities`、`get_readiness` 支撑仪表盘与编排器的可观测性。
- **冷热协同**：Helix 响应低延迟请求，Delta 持久化全量历史，两者通过同步器保持一致。

借助这些特性，`fstorage` 能够承载多种 fetcher 抓取的异构事实，用稳定的接口在毫秒级向 LLM 代理交付所需上下文。

## 延伸阅读

- `helixdb-cfg/schema.hx` — 图模型定义。
- `fstorage/vector_rules.json` — 向量边与索引规则。
- `fstorage/src/sync.rs` — 同步器与写入管线。
- `README.md` 与 `README.zh-CN.md` — 项目整体介绍。
