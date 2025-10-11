use crate::errors::{Result, StorageError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f64>>>;
}

pub struct NullEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for NullEmbeddingProvider {
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f64>>> {
        Ok(vec![vec![]; texts.len()])
    }
}

#[derive(Serialize)]
struct OpenAIRequest {
    input: Vec<String>,
    model: String,
}

#[derive(Deserialize)]
struct OpenAIEmbedding {
    embedding: Vec<f64>,
}

#[derive(Deserialize)]
struct OpenAIResponse {
    data: Vec<OpenAIEmbedding>,
}

pub struct OpenAIProvider {
    api_key: String,
    model: String,
    client: reqwest::Client,
}

impl OpenAIProvider {
    pub fn new(model: String, api_key: String) -> Self {
        Self {
            api_key,
            model,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIProvider {
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f64>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let request_payload = OpenAIRequest {
            input: texts,
            model: self.model.clone(),
        };

        let response = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .bearer_auth(&self.api_key)
            .json(&request_payload)
            .send()
            .await
            .map_err(|e| StorageError::SyncError(format!("OpenAI API request failed: {}", e)))?;

        if !response.status().is_success() {
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(StorageError::SyncError(format!(
                "OpenAI API returned an error: {}",
                error_body
            )));
        }

        let openai_response = response.json::<OpenAIResponse>().await.map_err(|e| {
            StorageError::SyncError(format!("Failed to parse OpenAI response: {}", e))
        })?;

        let embeddings = openai_response
            .data
            .into_iter()
            .map(|data| data.embedding)
            .collect();

        Ok(embeddings)
    }
}
