use crate::errors::{Result, StorageError};
use async_trait::async_trait;
use fastembed::{InitOptions, TextEmbedding};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::task;

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

pub struct FastEmbedProvider {
    model: Arc<Mutex<TextEmbedding>>,
}

impl FastEmbedProvider {
    pub fn new_default() -> Result<Self> {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_model(model: fastembed::EmbeddingModel) -> Result<Self> {
        Self::new_with_options(InitOptions::new(model))
    }

    pub fn new_with_options(options: InitOptions) -> Result<Self> {
        let embedding = TextEmbedding::try_new(options).map_err(|e| {
            StorageError::SyncError(format!("Failed to initialize FastEmbed model: {}", e))
        })?;
        Ok(Self {
            model: Arc::new(Mutex::new(embedding)),
        })
    }
}

#[async_trait]
impl EmbeddingProvider for FastEmbedProvider {
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f64>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let model = Arc::clone(&self.model);
        let embeddings = task::spawn_blocking(move || -> Result<Vec<Vec<f64>>> {
            let mut guard = model
                .lock()
                .map_err(|_| StorageError::SyncError("FastEmbed model mutex poisoned".into()))?;
            let document_refs: Vec<_> = texts.iter().map(|s| s.as_str()).collect();
            let vectors = guard.embed(document_refs, None).map_err(|e| {
                StorageError::SyncError(format!("FastEmbed embedding failed: {}", e))
            })?;
            Ok(vectors
                .into_iter()
                .map(|vec| vec.into_iter().map(|value| value as f64).collect())
                .collect())
        })
        .await
        .map_err(|e| StorageError::SyncError(format!("FastEmbed task join error: {}", e)))??;

        Ok(embeddings)
    }
}
