# Fetcher: 数据摄取层设计与开发计划

## 1. 核心职责与定位

`fetcher` 是个人知识助理系统的数据摄取层。其**唯一职责**是从各种外部数据源（如GitHub API、世界银行API等）获取原始数据，并将其转换为由 `fstorage` 定义的、统一的、规范化的数据结构。

`fetcher` 在系统中的定位是一个**纯粹的数据转换器和提供者**。它不关心数据如何存储、如何索引、是否新鲜，只关心如何准确地“抓取”和“翻译”数据。

## 2. 设计原则

### 2.1. Schema的消费者，而非定义者

这是 `fetcher` 最核心的设计原则。

*   **依赖 `fstorage`**: `fetcher` crate 将 `fstorage` 作为其依赖项。
*   **使用生成的数据结构**: `fetcher` **绝不**定义自己的数据模型（如 `MyRepo`, `MyIssue`）。它将直接 `use fstorage::schemas::Repo;` 来使用由 `fstorage` 的 `build.rs` 脚本根据 `schema.hx` 自动生成的权威 `struct`。
*   **编译时安全**: 这种强依赖关系保证了当上游 `schema.hx` 发生变化时，任何 `fetcher` 中不兼容的数据转换逻辑都会在编译时失败，从而避免了运行时的类型错误。

### 2.2. 模块化与可扩展性

*   **按数据源组织**: 每个数据源都应该在 `fetcher` crate 中实现为一个独立的Rust模块。例如：
    *   `src/github.rs`
    *   `src/world_bank.rs`
    *   `src/arxiv.rs` (未来扩展)
*   **统一的 Trait**: 可以设计一个通用的 `Fetcher` trait，所有特定数据源的实现都遵循这个接口，使得上层调用更加统一。

```rust
// 示例 Trait
use fstorage::schemas::Repo; // 使用 fstorage 生成的 struct
use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FetcherError {
    // ... 错误定义
}

#[async_trait]
pub trait Fetcher<T> {
    async fn fetch_by_id(&self, id: &str) -> Result<T, FetcherError>;
}

pub struct GitHubFetcher;

#[async_trait]
impl Fetcher<Repo> for GitHubFetcher {
    async fn fetch_by_id(&self, full_name: &str) -> Result<Repo, FetcherError> {
        // ... 调用 GitHub API，将返回的 JSON 转换为 Repo struct
        unimplemented!()
    }
}
```

### 2.3. 健壮性与礼貌性

*   **错误处理**: 为网络请求、API错误、数据解析等所有可能失败的环节实现健壮的错误处理。
*   **尊重API限制**: 严格遵守外部API的速率限制（Rate Limiting）。`fetcher` 自身可以包含简单的速率控制逻辑，但更复杂的预算管理（如“本次操作最多允许20次API调用”）由 `fstorage` 的 `sync` 模块来协调。
*   **幂等性支持**: `fetcher` 获取的数据应包含能够支持幂等操作的唯一标识符（自然键，如 `repo.full_name`），供 `fstorage` 在执行 `MERGE` 操作时使用。

## 3. 开发计划

### 阶段一：实现第一个Fetcher (GitHub)

*   **目标**: 能够从GitHub获取仓库信息，并将其转换为 `fstorage` 定义的 `Repo` 结构。
*   **任务**:
    *   [ ] **创建 `github.rs` 模块**: 在 `fetcher` crate 中创建 `src/github.rs`。
    *   [ ] **添加依赖**: 引入 `reqwest` 用于HTTP请求，`serde` 和 `serde_json` 用于JSON反序列化。
    *   [ ] **实现 `fetch_repo` 函数**:
        *   函数签名: `pub async fn fetch_repo(client: &reqwest::Client, full_name: &str) -> Result<fstorage::schemas::Repo, Error>`。
        *   函数逻辑:
            1.  向 `https://api.github.com/repos/{full_name}` 发送GET请求。
            2.  将返回的JSON响应反序列化到一个临时的、与API响应完全匹配的 `struct`（例如 `GitHubApiRepo`）。这个临时`struct`可以定义在 `github.rs` 内部。
            3.  编写转换逻辑，将 `GitHubApiRepo` 的字段映射到 `fstorage::schemas::Repo` 的对应字段上。
            4.  处理可能出现的错误（如网络错误、404 Not Found、API速率限制等）。

### 阶段二：扩展Fetcher能力

*   **目标**: 增加对GitHub其他数据类型（如Issues, PRs）的抓取能力。
*   **任务**:
    *   [ ] **实现 `fetch_issues` 函数**:
        *   函数签名: `pub async fn fetch_issues(client: &reqwest::Client, repo_full_name: &str, page: u32) -> Result<Vec<fstorage::schemas::Issue>, Error>`。
        *   函数逻辑: 实现对GitHub Issues API的分页拉取和转换。
    *   [ ] **实现 `fetch_pull_requests` 函数**: 类似地，实现对Pull Requests的拉取和转换。

### 阶段三：抽象与泛化

*   **目标**: 引入通用的 `Fetcher` Trait，为接入更多数据源做准备。
*   **任务**:
    *   [ ] **定义 `Fetcher` Trait**: 在 `fetcher/src/lib.rs` 中定义通用的 `Fetcher<T>` trait。
    *   [ ] **重构 `GitHubFetcher`**: 创建 `GitHubFetcher` 结构体，并为其实现 `Fetcher<Repo>`, `Fetcher<Vec<Issue>>` 等 trait。
    *   [ ] **（可选）实现第二个Fetcher**: 尝试实现一个全新的Fetcher，例如 `ArxivFetcher`，来验证 `Fetcher` Trait设计的合理性和可扩展性。
