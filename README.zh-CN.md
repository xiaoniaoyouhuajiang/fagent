# fagent

[English Version](README.md)

## 概览

- **项目初衷**  
  构建一个面向智能体的检索栈，让系统能够理解代码和仓库上下文，使得 LLM 助手能够增量获取 Git 数据并稳定回答问题。

- **核心模块**
  - `fagent`：编排层，提供 HTTP API 与仪表盘，负责触发同步任务并暴露图谱/检索接口。
  - `gitfetcher`：抓取层，通过 Octocrab 访问 GitHub，提取仓库元数据、Issue、PR、代码图和向量，输出 `GraphData`。
  - `fstorage`：存储核心，将数据写入 Delta 冷层与 Helix 热层，维护 schema 元数据、 Catalog offset 以及检索 API。
  - 配套工具（`capture`、`fstorage_cli`、dashboard）：用于调试、基准数据生成与交互式探索。

## 架构说明

- **模块交互流程**
  1. 仪表盘或 HTTP API（fagent）接收带参数的同步请求；
  2. fagent 调用 `FStorageSynchronizer`，由指定 fetcher（如 `gitfetcher`）产出 `GraphData`；
  3. `fstorage` 将节点、边、向量写入 Delta (`Lake`) 与 Helix (`HelixGraphEngine`)，同时维护 offset 和索引；
  4. Catalog 记录导入元数据，为后续增量同步提供依据；
  5. 查询接口（邻居、子图、向量/混合检索）整合冷热数据，对外提供服务。

- **fstorage 内部机制**
  - **Lake**：负责 Delta Lake 的写入与读取，统一使用 `silver/entities/*`、`silver/edges/*`、`silver/vectors/*` 等路径，以批量方式原子写入。
  - **Helix Engine**：热路径图存储，支持低延迟的节点/边/向量查询。
  - **Catalog**：追踪导入 offset、向量索引表与外部锚点，用于增量重放。
  - **Schema Registry 与 `vector_rules.json`**：以配置驱动向量与边的元数据，保证 ID 稳定并维持通用性。
  - **Synchronizer**：负责调度“抓取 → 校验 → 写入 → 更新 offset”的完整流程。

## 已具备的能力

- **fagent-dashboard**  
  Web 界面，用于触发同步、查看进度，并以搜索/图谱可视化的方式探索知识图谱。

- **capture**（`cargo run -p gitfetcher --bin capture`）  
  命令行工具，用于抓取真实 GitHub 数据，持久化 `FetchResponse` 为 Arrow/JSON 基准数据，支持离线测试与问题复现。

- **fstorage_cli**（`cargo run -p fstorage --bin fstorage_cli -- --base-path <路径> …`）  
  查看冷热层数据：列出 Delta 表、查询 Helix 节点/边/向量，并比较计数以校验证明同步一致性。

## 快速开始

1. 安装依赖（Rust 工具链，可选安装 `cargo-instruments`，以及 Helix 所需依赖）。
2. 准备工作目录并设置环境变量，例如 `GITHUB_TOKEN`（以及 `USE_LSP`、代理等可选项）。
3. 运行 `cargo run -p fagent -- dashboard --base-path ./temp`，打开仪表盘触发同步。
4. 使用 `capture`、`fstorage_cli` 生成基准数据或调试存储内容。
5. 通过 `/graph.html`、`/api/graph` 接口或检索 API 浏览图谱数据。

---

更详细的设计说明与决策记录，请参考 `references/` 目录下的文档。
