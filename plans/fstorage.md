# FStorage: 统一数据存储层设计与开发计划

## 1. 核心设计哲学

`fstorage` 是个人知识助理系统的核心数据基座。它不仅仅是一个被动的数据库或缓存，而是一个具备主动管理能力的、统一的数据存储与 Schema 管理层。其核心设计目标是：

1.  **单一事实来源 (Single Source of Truth)**: 将 `helixql` (`schema.hx`) 作为整个系统数据模型的唯一、权威的定义来源。所有其他组件的数据结构都必须从此派生。
2.  **双重查询路径**: 同时支持高性能的图/网络分析和大规模的表格/统计分析，让专门的引擎做专门的事。
3.  **动态数据就绪度**: 主动管理数据的时效性和覆盖度，在数据新鲜度与API成本之间取得动态平衡。
4.  **嵌入式与零依赖**: 作为嵌入式库存在，与主应用一同编译，无需用户部署和维护外部数据库服务。

## 2. 核心架构与数据流

`fstorage` 内部遵循数据工程的“奖牌架构”，并围绕 `helixql` 作为中心进行组织。

  <!-- 占位符，未来可替换为真实架构图 -->

### 2.1. 组件拆解

*   **HelixQL Schema (`schema.hx`)**: **系统的唯一蓝图**。它定义了所有节点（Entities）、边（Edges）及其属性。任何数据模型的变更都必须且只需在此文件中进行。

*   **编译时代码生成 (`build.rs`)**:
    *   **职责**: 在 `fstorage` crate 编译时，读取 `schema.hx` 文件。
    *   **动作**: 利用 `helixc` 的 `parser` 模块将 `schema.hx` 解析为AST。
    *   **产物**: 遍历AST，自动生成所有数据模型对应的Rust `struct`（例如 `pub struct Repo { ... }`），并将其写入 `src/generated_schemas.rs`。这些 `struct` 将被 `pub use` 导出，供 `fetcher` 和 `fagent` 使用。

*   **数据湖 (Data Lake - 基于 Delta Lake)**:
    *   **技术选型**: `deltalake-rs`。
    *   **Bronze层**: 存储从外部API拉取的、未经任何修改的原始数据（如JSON）。
    *   **Silver层**: 存储经过清洗、结构化、去重后的数据。此层的数据格式为 Delta Table，其**列结构严格根据 `build.rs` 生成的 `struct` 派生而来**。这是执行大规模表格分析（Panel Data Analysis）的主阵地。
    *   **Gold层**: 存储为特定分析任务预计算的高级指标或聚合结果。

*   **图引擎 (Graph Engine - `helix_engine`)**:
    *   **职责**: 存储数据的图谱范式，提供高性能的多跳、关系和混合查询。
    *   **Schema来源**: 其节点和边的定义同样派生自 `schema.hx`。

*   **元数据目录 (Metadata Catalog - `catalog.sqlite`)**:
    *   **职责**: 作为数据湖的“大脑”，追踪所有数据的元信息。
    *   **存储内容**:
        *   **数据就绪度 (Data Readiness)**: 每个数据实体（如一个GitHub仓库）的 `last_synced_at`, `ttl`, 以及更精细的覆盖度指标（如 `known_issue_count` vs `remote_issue_count`）。
        *   **API预算**: 外部API的请求配额和重置时间。
        *   **任务日志**: 所有同步和ETL任务的执行记录。

### 2.2. 核心API接口

`fstorage` 将向上层（主要是`fagent`）暴露以下核心接口：

*   `FStorage::new(config)`: 初始化所有组件，包括解析Schema、建立数据库连接等。
*   `fstorage.check_readiness(entities)`: 检查指定实体的数据就绪度报告。
*   `fstorage.sync(context, budget)`: 核心同步入口。根据上下文触发 `fetcher` 进行数据补全，并执行 Bronze -> Silver 的ETL流程。
*   `fstorage.run_graph_etl(target)`: 将Silver层的数据加载或更新到 `helix_engine` 中。
*   `fstorage.graph_query(query_name, params)`: 执行在 `queries.hx` 中预定义的、由`helixc`编译好的图查询。
*   `fstorage.lake_query(sql_or_dataframe_plan)`: 使用 `datafusion` 或 `polars` 对Silver层的Delta Table执行表格查询。

## 3. 开发计划

### 阶段一：奠定数据契约与核心管道 (Foundation)

*   **目标**: 打通一个最简数据流（GitHub Repo信息），验证核心架构。
*   **任务**:
    *   [ ] **确定统一的Schema管理策略**: 确立 `helixql` (`schema.hx`) 作为唯一的Schema来源。
    *   [ ] **实现`build.rs`代码生成**: 在`fstorage`的`build.rs`中，引入`helixc`作为开发依赖，解析`schema.hx`并为`Repo`实体生成对应的Rust `struct`到`src/generated_schemas.rs`。
    *   [ ] **集成Delta Lake**: 重构`lake.rs`，将简单的文件写入替换为使用`deltalake` crate对Delta Table进行`MERGE`或`WRITE`操作。确保写入的`RecordBatch`结构与生成的`Repo` struct匹配。
    *   [ ] **实现基本`insert`流程**: 在`fstorage`中实现一个`insert_repo`函数，该函数应能（概念上）调用`fetcher`获取一个`Repo`对象，并将其写入Silver层的`repos` Delta Table。

### 阶段二：实现图谱加载与查询 (Graph Loading & Query)

*   **目标**: 让数据可以被`helix_engine`查询，并打通双查询路径。
*   **任务**:
    *   [ ] **实现ETL到图谱**: 在`sync.rs`中实现`run_graph_etl`，使其能读取`silver/repos` Delta Table，将数据转换为`helix_engine`的Node格式，并调用`engine.add_node`。
    *   [ ] **实现基本图查询**: 在`queries.hx`中定义一个简单的`getRepoByName(name: String)`查询。在`fstorage`中暴露`graph_query`接口，使其能够调用由`helixc`生成的对应函数。
    *   [ ] **实现基本湖查询**: 在`fstorage`中暴露`lake_query`接口，使用`datafusion`实现一个简单的SQL查询，例如`SELECT COUNT(*) FROM repos`。

### 阶段三：构建Agent智能调度与高级功能 (Agent Orchestration & Advanced Features)

*   **目标**: 实现完整的动态数据管理闭环，并增强系统健壮性。
*   **任务**:
    *   [ ] **完善数据就绪度模型**: 扩展`catalog.sqlite`中的`entity_readiness`表，增加`known_count`, `expected_count`等字段，并实现相应的更新逻辑。
    *   [ ] **增强`sync`接口**: 完善`budgeted_complete`逻辑，实现真实的API预算检查和数据拉取。
    *   [ ] **提升并发性能**: 将`catalog.rs`中的`Arc<Mutex<Connection>>`替换为基于`r2d2`的SQLite连接池，以支持并发访问。
    *   [ ] **引入Schema迁移**: 研究`helix_engine`的`VersionInfo`和`Migration`机制，并尝试为`Repo`节点增加一个新字段，编写一个简单的`migration.hx`来完成数据迁移。
