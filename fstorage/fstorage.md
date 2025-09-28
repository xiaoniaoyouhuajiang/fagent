# FStorage: 动态数据湖存储层设计文档

## 1. 设计目标 (要解决什么问题)

`fstorage` 旨在为个人AI Agent构建一个**本地优先、动态同步、且具备分析能力的嵌入式数据存储层**。它核心要解决以下几个问题：

1.  **数据持久化与融合**: Agent需要处理和融合来自多个异构数据源的数据（初期为GitHub代码和事实数据，未来扩展至经济数据等）。`fstorage` 提供一个统一的本地存储基座，以开放格式持久化这些数据，避免重复拉取，并为跨源分析提供基础。

2.  **数据时效性与成本的平衡**: 对于一个依赖网络数据的Agent，本地数据很容易过时。但无限制地拉取最新数据会迅速耗尽API配额且效率低下。`fstorage` 通过引入**数据就绪度 (Data Readiness)** 和 **预算化补全 (Budgeted Completion)** 机制，在数据新鲜度、覆盖度与API成本之间取得动态平衡。它不是一个被动的缓存，而是一个能主动感知自身状态并按需更新的“活性”数据层。

3.  **为复杂分析提供高性能查询**: Agent的核心任务（如代码理解、网络分析）需要高性能的图、向量和文本混合查询。原始的数据湖文件（如Parquet/JSON）不适合直接进行此类查询。`fstorage` 的职责是作为**数据准备层**，将数据湖中经过清洗、结构化的数据，高效地加载到专门的分析引擎——`helix_engine`——中，从而将存储与计算分离，让上层应用能享受到高性能查询。

4.  **简洁的架构与部署**: 作为一个面向个人用户的单体应用，其架构必须简单、可靠。`fstorage` 被设计为一个嵌入式的Rust库，与应用代码编译在同一个二进制文件中，无需用户部署和维护独立的数据库服务，实现了“零部署负担”。

## 2. 工作机制 (理想工作状态)

`fstorage` 的内部工作流被设计为一个清晰、单向的数据管道，遵循数据工程中成熟的“奖牌架构”(Bronze/Silver/Gold)。

### 2.1. 核心组件

-   **Data Lake**: 基于本地文件系统，采用分层目录结构。
    -   **Bronze层**: 存储从外部API拉取的、未经任何修改的原始数据（如JSON Lines）。这是数据的“底片”，保证了可追溯性。
    -   **Silver层**: 存储经过清洗、去重、规范化和结构化的数据。数据在此层面被建模为逻辑上的**实体 (Entities)** 和 **关系 (Edges)**，为图谱构建做好准备。
    -   **Gold层**: 存储为特定分析任务预计算出的高级指标或特征（如PageRank值、社区划分结果）。
        - gold/metrics 记录算法名、参数哈希、scope_id、snapshot_ts/commit_sha；例如 pagerank(scope_id, node_id_int, pr_value, epsilon/escape, algo, params_hash, snapshot_ts)。
        - 在线结果可以同步写回 Helix 节点属性，但以 gold 为权威历史，便于版本对比和回滚。
-   **Metadata Catalog (`catalog.sqlite`)**: 一个嵌入式的SQLite数据库，是整个数据湖的“大脑”。它存储：
    -   **数据就绪度**: 每个数据实体的新鲜度(entity_or_scope, edge_type, last_synced_at, ttl_s, known_count, expected_count, coverage = known/expected, frontier_size, commit_sha, freshness_age_s)。
    -   **API预算**: 外部API的请求配额和重置时间。
    -   **任务日志**: 所有同步和ETL任务的执行记录。
-   **Helix Engine (`helix_engine`)**: 一个嵌入式的图-向量数据库实例。它是数据的使用方，负责索引Silver层的数据并提供高性能查询接口。
-   **Data Synchronizer**: 核心逻辑控制器，负责调度和执行从数据采集到加载进`helix_engine`的整个流程。

### 2.2. 理想数据流

1.  **触发 (Agent查询)**: AI Agent接收到一个需要外部知识的问题（例如：“找出`helix-db`项目中最近与性能相关的Issue”）。
2.  **就绪度检查**: Agent向`DataSynchronizer`查询相关实体（`helix-db`仓库）的数据就绪度。`DataSynchronizer`通过查询`catalog.sqlite`发现本地Issue数据已超过TTL（例如1小时），不够“新鲜”。
3.  **预算化补全**:
    -   `DataSynchronizer`启动一个数据补全任务，并从`catalog.sqlite`获取API预算。
    -   在预算（如10秒内或20个API请求）内，向GitHub API拉取最新的Issue数据。
    -   将拉取到的原始JSON数据写入`bronze`层。
    -   **立即**触发一个微型ETL流程：读取刚写入的Bronze数据，将其转换为结构化的Issue实体，并**合并(merge)**到`silver`层的`entities/issues`表中。
    -   更新`catalog.sqlite`中`helix-db`仓库的`last_synced_at`时间戳。
    -   在`task_logs`中记录本次同步任务的成功状态。
4.  **ETL到分析引擎**:
    -   `DataSynchronizer`触发`run_full_etl_from_lake`流程（或一个增量更新流程）。
    -   该流程读取`silver`层的`entities/issues`表。
    -   将新的Issue数据转换为`helix_engine`的节点格式。
    -   调用`helix_engine`的API将这些新节点添加到图中。
    增量机制：
    - 若近期无法恢复 Delta：在 catalog.sqlite 维护 per-table high_watermark（最大 updated_at 或自增版本），Silver 写入时带 snapshot_ts；Helix 同步时只拉 snapshot_ts > last_synced_snapshot_ts 的记录。
    - 恢复 Delta 后：用 change data feed（CDF）或 commit 版本号做增量投影。
    - 需要设计幂等 upsert：以 id_int 为主键，Helix 侧 MERGE 节点/边。
        - Bronze 写入带 source_etag/hash、pulled_at；Silver merge 时以自然键去重（如 repo_id + number 唯一）。
        - ETL 作业记录 jobs(job_id, input_ranges, output_versions, success)；失败可重跑，不产生重复。
        - Helix 投影批次带 batch_id，重复投影也不产生重复边（MERGE + unique constraints）。
5.  **最终查询与透明回答**: Agent再次向`helix_engine`发起查询，这次得到了包含最新数据的结果。Agent在生成回答时，可以附上数据的元信息：“根据刚刚同步的数据，最近与性能相关的Issue有...”。

## 3. 未来增强点 (TODO)

虽然当前的设计为`fstorage`奠定了坚实的基础，但距离一个工业级的、全功能的存储层还有一些可以增强的地方。这些可以作为后续开发的TODO列表：

-   **[ ] 实现健壮的ETL框架 (Bronze -> Silver -> Gold)**:
    -   **Schema管理**: 为Silver/Gold层的数据定义强类型的Schema，并实现Schema演进的支持。
    -   **数据转换逻辑**: 编写具体的、可配置的转换逻辑，用于将不同来源的Bronze层JSON数据解析为Silver层的实体和边。
    -   **幂等性与可重跑性**: 确保ETL任务是幂等的，可以安全地重复运行而不会产生副作用（例如重复数据）。

-   **[ ] 完善`DataSynchronizer`的核心逻辑**:
    -   **智能预算分配**: 实现更智能的预算分配策略，而不仅仅是固定的时间和请求次数。例如，根据查询的重要性和数据的“不新鲜”程度来动态调整预算。
    -   **增量ETL**: 实现从Silver层到`helix_engine`的**增量加载**机制，而不是每次都全量重建。这需要利用Delta Lake的时间旅行能力或`catalog.sqlite`中的版本信息来识别变更数据。
    -   **并发与容错**: 实现并发的数据拉取和处理，并为API请求、文件IO等增加重试和错误处理逻辑。

-   **[ ] 增强数据湖的开放性与互操作性**:
    -   **恢复Delta Lake支持**: 解决依赖冲突问题（例如，通过将`polars`和`deltalake`的依赖统一到一个独立的crate中，或者等待上游库版本统一），重新引入Delta Lake以获得ACID事务和版本控制能力。这是构建可靠数据湖的基石。
    -   **实现字典编码 (Dicts)**: 实现`dicts`层，为高基数的字符串ID（如URL、用户名）创建整数ID映射，并在加载到`helix_engine`时使用这些整数ID，以大幅提升图数据库的性能。

-   **[ ] 丰富元数据与数据质量监控**:
    -   **实现覆盖度指标**: 在`catalog.sqlite`中实现具体的覆盖度指标计算和更新逻辑。例如，对于一个代码仓库，可以计算其依赖关系图的覆盖广度。
    -   **数据质量检查 (DQ)**: 在ETL流程中加入数据质量检查步骤，例如检查关键字段是否为空、数据格式是否正确，并将DQ报告记录在`catalog.sqlite`中。

-   **[ ] 完善测试与文档**:
    -   **端到端测试**: 编写一个完整的端到端集成测试，模拟从用户提问 -> 触发同步 -> 拉取数据 -> ETL -> 加载到引擎 -> 最终查询的全过程。
    -   **API文档**: 为所有公共API（特别是`DataSynchronizer` trait）编写详细的Rustdoc，解释其行为、参数和前置条件。

## 4. Silver层Schema设计实例与扩展指南

Silver层是数据湖的核心，它将原始的、半结构化的Bronze层数据转化为干净、规范化的图结构（实体和边）。良好的Schema设计是后续所有分析和查询的基础。

### 4.1. GitHub数据源Schema实例

以下是针对GitHub数据源，将数据建模为实体（Nodes）和关系（Edges）的Schema实例。这些表最终会以Delta Lake格式存储在`silver/entities/`和`silver/edges/`目录下。

#### 4.1.1. 实体表 (Entities)

**`entities/repos`**
| 列名 | 类型 | 描述 | 主键 |
| :--- | :--- | :--- | :--- |
| `repo_id` | Int64 | 仓库的数字ID | **PK** |
| `node_id` | String | 仓库的GraphQL `node_id` | |
| `full_name` | String | 仓库全名，如 "HelixDB/helix-db" | |
| `description` | String | 仓库描述 | |
| `language` | String | 主要编程语言 | |
| `stargazers_count` | Int32 | Star数量 | |
| `forks_count` | Int32 | Fork数量 | |
| `created_at` | Timestamp | 创建时间 | |
| `updated_at` | Timestamp | 最后更新时间 | |

**`entities/users`**
| 列名 | 类型 | 描述 | 主键 |
| :--- | :--- | :--- | :--- |
| `user_id` | Int64 | 用户的数字ID | **PK** |
| `login` | String | 用户名 | |
| `avatar_url` | String | 头像URL | |
| `company` | String | 所属公司 | |
| `location` | String | 所在地 | |

**`entities/issues`**
| 列名 | 类型 | 描述 | 主键 |
| :--- | :--- | :--- | :--- |
| `issue_id` | Int64 | Issue的数字ID | **PK** |
| `repo_id` | Int64 | 所属仓库ID | FK(repos) |
| `number` | Int32 | Issue编号 | |
| `title` | String | 标题 | |
| `body` | String | 内容 | |
| `state` | String | 状态 ("open", "closed") | |
| `author_id` | Int64 | 作者ID | FK(users) |
| `created_at` | Timestamp | 创建时间 | |

**`entities/code_files`**
| 列名 | 类型 | 描述 | 主键 |
| :--- | :--- | :--- | :--- |
| `file_id` | String | 文件的唯一ID (e.g., SHA-1 hash) | **PK** |
| `repo_id` | Int64 | 所属仓库ID | FK(repos) |
| `path` | String | 文件在仓库中的路径 | |
| `content` | String | 文件内容（或其摘要/嵌入） | |
| `language` | String | 文件语言 | |

#### 4.1.2. 关系表 (Edges)

关系表用于连接实体，形成图。

**`edges/commits`** (建模为连接User和Repo的边)
| 列名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `commit_sha` | String | Commit的SHA哈希 |
| `author_id` | Int64 | 提交者ID |
| `repo_id` | Int64 | 仓库ID |
| `message` | String | 提交信息 |
| `committed_at` | Timestamp | 提交时间 |

**`edges/pull_requests`** (建模为连接两个Repo的边：`head` -> `base`)
| 列名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `pr_id` | Int64 | PR的数字ID |
| `author_id` | Int64 | 作者ID |
| `head_repo_id` | Int64 | 源仓库ID |
| `base_repo_id` | Int64 | 目标仓库ID |
| `state` | String | 状态 ("open", "closed", "merged") |
| `title` | String | 标题 |
| `created_at` | Timestamp | 创建时间 |

**`edges/pr_modifies_file`** (多对多关系)
| 列名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `pr_id` | Int64 | PR的ID |
| `file_id` | String | 文件的ID |
| `changes` | Int32 | 修改行数 |

### 4.2. Schema扩展指南

当需要引入新的数据源（如经济数据）或对现有数据源进行更深层次的建模时，遵循以下原则可以确保数据湖的可维护性和可扩展性。

**原则1：优先扩展，而非修改**

-   **增加新表**: 当引入一个全新的数据领域时（如公司财报），最佳实践是创建一个全新的实体表（如 `entities/companies`）和相关的关系表（如 `edges/company_operates_repo`），而不是试图将财报数据硬塞进现有的 `repos` 表中。
-   **增加新列**: 只有当新数据是现有实体的**内在属性**时，才考虑为现有表增加列。例如，为 `users` 表增加 `email` 字段是合理的。

**原则2：保持Silver层的规范化**

-   Silver层的核心目标是提供一个干净、结构化的图模型。应尽量避免在此层进行数据冗余。例如，不要在`issues`表中存储完整的作者信息（如`author_login`, `author_avatar`），只存储`author_id`。完整的作者信息可以通过`issues.author_id`与`users.user_id`进行连接（JOIN）查询获得。这使得用户信息的更新只需要在一个地方（`users`表）进行。

**原则3：将复杂计算和聚合推迟到Gold层**

-   Silver层应该只关注事实的结构化表示。所有派生的、聚合的或经过复杂计算的数据都应该放在Gold层。
-   **示例**:
    -   **错误做法**: 在`repos`表中增加一个`monthly_commit_count`列。这个数据是动态计算的，不属于仓库的内在属性。
    -   **正确做法**: 创建一个`gold/metrics/repo_monthly_activity`表，其中包含`repo_id`, `month`, `commit_count`等列。这个表可以通过对`edges/commits`表进行聚合计算来生成。

**扩展流程示例：引入公司财报数据**

1.  **数据采集**: `DataSynchronizer`从新的财经API拉取公司财报原始JSON，存入`bronze/financial_api/`。
2.  **创建新实体表**: 在ETL流程中，创建一个新的Silver表`silver/entities/companies`，包含`company_id`, `name`, `stock_symbol`, `market_cap`等字段。
3.  **创建新关系表**: 创建一个`silver/edges/operates`表，用于连接`companies`和`repos`。这可能需要一些逻辑来匹配公司名称和GitHub组织名。
4.  **更新ETL**: 修改`run_full_etl_from_lake`的逻辑，使其在加载图谱时，同时读取`companies`和`operates`表，创建`COMPANY`节点和`OPERATES`边。
5.  **更新`helix_engine` Schema**: 在`helix_engine`的Schema定义中，添加`N::COMPANY`节点类型和`E::OPERATES`边类型。

通过遵循这些原则，您的数据湖将能够灵活、清晰地向后扩展，以容纳任意复杂度和领域的数据，同时保持其核心结构的可维护性。

## 5. 核心设计决策释疑 (FAQ)

### 5.1. 为什么需要Silver层？它和HelixDB是否功能重复？

这是一个非常核心的问题。答案是：**Silver层和HelixDB在逻辑上的“重复”是刻意为之的，它并非无意义的冗余，而是为了实现“关注点分离”和“架构弹性”这一核心目标。**

我们可以将数据湖和`helix-db`看作两个有不同专长的“专家”：

| 特性 | Silver层 (数据湖) | HelixDB (查询引擎) |
| :--- | :--- | :--- |
| **核心职责** | 数据的**持久化**、**规范化**和**历史归档** | 数据的**索引**和**高性能查询** |
| **数据格式** | **开放** (Parquet, Delta Lake) | **专有** (为性能优化的内部格式) |
| **互操作性** | **高** (可被多种工具读取) | **低** (只能通过其API访问) |
| **角色** | **事实来源 (Source of Truth)** | **物化视图/索引 (Materialized View/Index)** |
| **可恢复性** | 数据丢失是灾难 | 可从Silver层快速重建 |

**简而言之，Silver层是数据的“通用档案库”，而HelixDB是高性能的“专业查询引擎”。** Silver层保证了数据的长期价值和互操作性，而HelixDB提供了实时查询的能力。这种“重复”是用存储空间换取了整个架构的灵活性、鲁棒性和未来的可扩展性。

### 5.2. 恢复时应该使用Bronze层还是Silver层？

**恢复策略应该是分层的：**

1.  **首选从Silver层恢复**：当`helix-db`实例损坏或需要重建索引时，应该从Silver层读取已经结构化的数据进行恢复。这个过程主要是I/O密集型的，速度快，计算成本低。
2.  **仅在必要时从Bronze层恢复**：当且仅当Silver层本身的数据出现问题，或者ETL（Bronze -> Silver）的逻辑需要重构时，才需要从最原始的Bronze层开始，重新执行整个昂贵的ETL过程。

**比喻来说：**
- **Bronze层**: 是所有的**原材料**（面粉、鸡蛋）。
- **ETL (Bronze -> Silver)**: 是耗时费力的**烘焙过程**，把原材料做成**蛋糕胚**。
- **Silver层**: 是您精心制作并冷冻起来的**蛋糕胚仓库**。
- **加载 (Silver -> HelixDB)**: 是快速的**装饰过程**，取出蛋糕胚，抹上奶油。
- **HelixDB**: 是摆在橱窗里随时可以出售的**成品蛋糕**。

如果成品蛋糕坏了，我们直接从蛋糕胚仓库里拿一个新的来装饰，而不是每次都从和面开始。
