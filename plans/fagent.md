# Fagent: 智能调度与分析引擎设计与开发计划

## 1. 核心职责与定位

`fagent` 是整个个人知识助理系统的**指挥中心和智能大脑**。它位于架构的最顶层，负责理解用户意图，并智能地调度 `fstorage` 和 `fetcher` 来完成复杂的知识获取、分析和响应任务。

`fagent` 的核心价值在于其**决策和编排能力**。它将底层的、确定性的数据操作（存储、抓取）与高层的、非确定性的语言理解（LLM）相结合，形成一个完整的、智能的闭环。

## 2. 设计原则

### 2.1. 意图驱动 (Intent-Driven)

*   `fagent` 的所有行为都由用户的自然语言查询驱动。它必须能够将模糊的用户请求（例如：“最近`helix-db`项目有什么性能相关的进展？”）分解为一系列具体、可执行的数据操作。

### 2.2. 状态感知与动态决策 (State-Aware & Dynamic)

*   `fagent` 必须时刻感知 `fstorage` 中数据的状态。在执行任何查询之前，它会首先通过 `fstorage.check_readiness()` 来评估相关数据的“新鲜度”和“覆盖度”。
*   基于就绪度报告，`fagent` 动态决策：
    *   **数据足够好**: 直接执行查询。
    *   **数据已过时或不完整**: 触发 `fstorage.sync()` 进行数据补全，等待同步完成后再执行查询。
    *   **数据完全缺失**: 触发完整的数据抓取和ETL流程。

### 2.3. LLM作为协处理器，而非运行时 (LLM as a Co-processor, not a Runtime)

*   `fagent` 将LLM用于其最擅长的领域：**理解、规划、总结和生成**。
*   **严禁**将LLM用于执行确定性的、高性能的ETL或数据转换任务。这些任务必须由原生Rust代码完成。
*   **合理使用场景**:
    1.  **任务规划 (Task Planning)**: 将用户请求解析成一个结构化的执行计划（JSON或YAML格式），例如：
        ```yaml
        - action: check_readiness
          entities: ["repo:helix-db/helix-db"]
        - action: graph_query
          query: findIssuesByLabel
          params: { repo_name: "helix-db/helix-db", label: "performance" }
        - action: lake_query
          sql: "SELECT author, COUNT(*) FROM issues WHERE repo_id = ... GROUP BY author"
        - action: synthesize
          prompt: "总结以上图查询和SQL查询的结果，回答用户关于性能进展的问题。"
        ```
    2.  **结果合成 (Result Synthesis)**: 将来自图查询（关系）和数据湖查询（统计）的结构化数据，整合成一段流畅、有洞察力的自然语言回答。
    3.  **代码生成辅助 (ETL Code Generation)**: 在开发阶段，`fagent`可以被用作一个开发工具，辅助开发者为新的数据源生成`fetcher`中的转换代码。

## 3. 开发计划

### 阶段一：构建基础编排骨架

*   **目标**: 创建 `fagent` crate，并实现一个不依赖LLM的、基于规则的简单调度流程。
*   **任务**:
    *   [ ] **创建 `fagent` Crate**: 初始化一个新的 `bin` crate。
    *   [ ] **添加依赖**: 引入 `fstorage` 和 `tokio`。
    *   [ ] **实现主循环 (main.rs)**:
        1.  初始化 `FStorage` 实例。
        2.  模拟一个硬编码的用户请求（例如，查询 "helix-db/helix-db" 的信息）。
        3.  调用 `fstorage.check_readiness()`。
        4.  根据返回结果（简单地判断 `is_fresh`），决定是否调用 `fstorage.sync()` 和 `fstorage.run_graph_etl()`。
        5.  调用 `fstorage.graph_query()` 执行一个预定义的查询。
        6.  将查询结果打印到控制台。

### 阶段二：集成LLM进行任务规划

*   **目标**: 让 `fagent` 能够理解自然语言，并生成结构化的执行计划。
*   **任务**:
    *   [ ] **引入LLM客户端**: 添加一个简单的HTTP客户端（如`reqwest`）来调用外部LLM API（如OpenAI, Groq等）。
    *   [ ] **设计Prompt模板**: 设计一个精巧的系统Prompt，指导LLM如何根据用户问题和可用的`fstorage`查询（可以动态提供给LLM）来生成一个JSON格式的执行计划。
    *   [ ] **实现计划解析与执行器**:
        *   编写代码来解析LLM返回的JSON计划。
        *   根据计划中的`action`（如`graph_query`, `lake_query`），依次调用`fstorage`的相应方法。

### 阶段三：实现结果合成与高级分析

*   **目标**: 利用LLM将多源数据融合成有价值的洞察。
*   **任务**:
    *   [ ] **数据序列化**: 将来自`graph_query`和`lake_query`的结构化结果序列化为紧凑的格式（如JSON或Markdown表格）。
    *   [ ] **设计合成Prompt**: 设计一个新的Prompt模板，将序列化后的数据和原始用户问题一起发送给LLM，要求它进行总结、分析并生成最终答案。
    *   [ ] **流式响应 (Streaming Response)**: （可选，高级功能）如果LLM API支持，实现流式响应，以获得更好的用户体验。

### 阶段四：迈向真正的“智能体”

*   **目标**: 赋予`fagent`更强的自主性和学习能力。
*   **任务**:
    *   [ ] **动态查询生成**: 探索让LLM在某些情况下，直接生成`helixql`或SQL查询语句，而不是仅仅选择预定义的查询。这需要非常严格的校验和沙箱机制。
    *   [ ] **自我学习与反馈**: 记录用户追问或不满意的交互，将其作为反馈，让LLM在未来的规划中进行调整和优化。
