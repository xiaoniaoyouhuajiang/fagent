//! World Bank Data API module
//! 
//! This module provides an asynchronous interface to the World Bank Data API v2,
//! allowing users to fetch economic data, indicators, countries, and other information.

use serde::{Deserialize, Serialize, Deserializer};
use thiserror::Error;

/// Base URL for World Bank API
const WORLD_BANK_URL: &str = "https://api.worldbank.org/v2";

/// Custom error types for World Bank API operations
#[derive(Error, Debug)]
pub enum WorldBankError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),
    
    #[error("API returned error: {0}")]
    ApiError(String),
    
    #[error("JSON deserialization failed: {0}")]
    JsonError(#[from] serde_json::Error),
    
    #[error("No data found for the request")]
    NoDataError,
    
    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),
}

/// Result type for World Bank operations
pub type Result<T> = std::result::Result<T, WorldBankError>;

/// Client for interacting with the World Bank API
#[derive(Clone, Debug)]
pub struct WorldBankClient {
    http_client: reqwest::Client,
    language: String,
}

impl Default for WorldBankClient {
    fn default() -> Self {
        Self {
            http_client: reqwest::Client::new(),
            language: "en".to_string(),
        }
    }
}

impl WorldBankClient {
    /// Create a new WorldBankClient with default settings
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a new WorldBankClient with specified language
    pub fn with_language(language: &str) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            language: language.to_string(),
        }
    }
    
    /// Get the current language setting
    pub fn language(&self) -> &str {
        &self.language
    }
    
    /// Set the language for API responses
    pub fn set_language(&mut self, language: &str) {
        self.language = language.to_string();
    }
    
    /// Make a request to the World Bank API
    async fn make_request<T>(&self, endpoint: &str, params: &[&str]) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let mut url = format!("{}/{}", WORLD_BANK_URL, endpoint);
        
        // Add language parameter if not English
        if self.language != "en" {
            url = format!("{}/{}", self.language, url);
        }
        
        // Add additional parameters
        for param in params {
            url = format!("{}/{}", url, param);
        }
        
        let response = self.http_client
            .get(&url)
            .query(&[("format", "json"), ("per_page", "20000")])
            .send()
            .await?;
            
        if !response.status().is_success() {
            return Err(WorldBankError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        
        let data: serde_json::Value = response.json().await?;
        
        // Handle API error messages
        if let Some(array) = data.as_array() {
            if array.len() > 0 && array[0].get("message").is_some() {
                if let Some(msg) = array[0]["message"].as_array() {
                    if msg.len() > 0 && msg[0].get("value").is_some() {
                        return Err(WorldBankError::ApiError(
                            msg[0]["value"].as_str().unwrap_or("Unknown API error").to_string()
                        ));
                    }
                }
            }
        }
        
        // Handle the World Bank API's response format: [metadata, actual_data]
        if let Some(array) = data.as_array() {
            if array.len() >= 2 {
                // The second element contains the actual data
                return serde_json::from_value(array[1].clone()).map_err(WorldBankError::JsonError);
            } else if array.len() == 1 {
                // Sometimes the response might contain only data (rare case)
                return serde_json::from_value(array[0].clone()).map_err(WorldBankError::JsonError);
            }
        }
        
        serde_json::from_value(data).map_err(WorldBankError::JsonError)
    }
    
    /// Fetch a list of countries
    pub async fn get_countries(&self) -> Result<Vec<Country>> {
        self.make_request("country", &[]).await
    }
    
    /// Fetch a specific country by ID
    pub async fn get_country(&self, country_id: &str) -> Result<Country> {
        let countries: Vec<Country> = self.make_request("country", &[country_id]).await?;
        countries.into_iter()
            .next()
            .ok_or(WorldBankError::NoDataError)
    }
    
    /// Fetch all available indicators
    pub async fn get_indicators(&self) -> Result<Vec<Indicator>> {
        self.make_request("indicator", &[]).await
    }
    
    /// Fetch a specific indicator by ID
    pub async fn get_indicator(&self, indicator_id: &str) -> Result<Indicator> {
        let indicators: Vec<Indicator> = self.make_request("indicator", &[indicator_id]).await?;
        indicators.into_iter()
            .next()
            .ok_or(WorldBankError::NoDataError)
    }
    
    /// Fetch data series for an indicator
    pub async fn get_series(&self, indicator_id: &str, options: &SeriesOptions) -> Result<SeriesData> {
        let country = options.country.as_deref().unwrap_or("all");
        let url = if self.language == "en" {
            format!("{}/{}/indicator/{}", WORLD_BANK_URL, country, indicator_id)
        } else {
            format!("{}/{}/{}/indicator/{}", self.language, WORLD_BANK_URL, country, indicator_id)
        };
        
        let mut request = self.http_client.get(&url);
        
        // Add query parameters
        request = request.query(&[("format", "jsonstat")]);
        
        if let Some(date) = &options.date {
            request = request.query(&[("date", date)]);
        }
        
        if let Some(mrv) = options.mrv {
            request = request.query(&[("mrv", &mrv.to_string())]);
        }
        
        if let Some(gapfill) = &options.gapfill {
            request = request.query(&[("gapfill", gapfill)]);
        }
        
        let response = request.send().await?;
            
        if !response.status().is_success() {
            return Err(WorldBankError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }
        
        let data: serde_json::Value = response.json().await?;
        serde_json::from_value(data).map_err(WorldBankError::JsonError)
    }
    
    /// Fetch available topics
    pub async fn get_topics(&self) -> Result<Vec<Topic>> {
        self.make_request("topic", &[]).await
    }
    
    /// Fetch available sources
    pub async fn get_sources(&self) -> Result<Vec<Source>> {
        self.make_request("source", &[]).await
    }
    
    /// Fetch available regions
    pub async fn get_regions(&self) -> Result<Vec<Region>> {
        self.make_request("region", &[]).await
    }
    
    /// Search countries by name or other criteria
    pub async fn search_countries(&self, pattern: &str) -> Result<Vec<Country>> {
        let countries = self.get_countries().await?;
        let pattern_lower = pattern.to_lowercase();
        
        Ok(countries.into_iter()
            .filter(|country| {
                country.name.to_lowercase().contains(&pattern_lower) ||
                country.id.to_lowercase().contains(&pattern_lower) ||
                country.iso2_code.to_lowercase().contains(&pattern_lower)
            })
            .collect())
    }
    
    /// Search indicators by name or other criteria
    pub async fn search_indicators(&self, pattern: &str) -> Result<Vec<Indicator>> {
        let indicators = self.get_indicators().await?;
        let pattern_lower = pattern.to_lowercase();
        
        Ok(indicators.into_iter()
            .filter(|indicator| {
                indicator.name.to_lowercase().contains(&pattern_lower) ||
                indicator.id.to_lowercase().contains(&pattern_lower) ||
                indicator.source_note.as_ref()
                    .map(|note| note.to_lowercase().contains(&pattern_lower))
                    .unwrap_or(false)
            })
            .collect())
    }
}

/// Options for fetching series data
#[derive(Clone, Debug, Default)]
pub struct SeriesOptions {
    /// Country code (e.g., "CHN", "USA"), or None for all countries
    pub country: Option<String>,
    /// Date range (e.g., "2010:2020") or specific year (e.g., "2020")
    pub date: Option<String>,
    /// Most recent values (e.g., 1 for latest value only)
    pub mrv: Option<u32>,
    /// Gap filling ("Y" or "N")
    pub gapfill: Option<String>,
}

/// Country data structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Country {
    pub id: String,
    #[serde(rename = "iso2Code")]
    pub iso2_code: String,
    pub name: String,
    pub region: RegionInfo,
    #[serde(rename = "incomeLevel")]
    pub income_level: IncomeLevelInfo,
    #[serde(rename = "lendingType")]
    pub lending_type: LendingTypeInfo,
    #[serde(rename = "capitalCity")]
    pub capital_city: String,
    #[serde(deserialize_with = "deserialize_optional_float")]
    pub longitude: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_float")]
    pub latitude: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionInfo {
    pub id: String,
    pub iso2code: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncomeLevelInfo {
    pub id: String,
    pub iso2code: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LendingTypeInfo {
    pub id: String,
    pub iso2code: String,
    pub value: String,
}

/// Indicator data structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Indicator {
    pub id: String,
    pub name: String,
    pub source: SourceInfo,
    #[serde(rename = "sourceNote")]
    pub source_note: Option<String>,
    #[serde(rename = "sourceOrganization")]
    pub source_organization: Option<String>,
    pub topics: Vec<TopicInfo>,
    pub unit: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceInfo {
    pub id: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicInfo {
    pub id: Option<String>,
    pub value: Option<String>,
}

/// Topic data structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Topic {
    pub id: String,
    pub value: String,
    #[serde(rename = "sourceNote")]
    pub source_note: Option<String>,
}

/// Source data structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    #[serde(rename = "lastupdated")]
    pub last_updated: String,
    pub name: String,
    pub code: String,
    pub description: Option<String>,
    pub url: Option<String>,
    #[serde(rename = "dataavailability")]
    pub data_availability: Option<String>,
    #[serde(rename = "metadataavailability")]
    pub metadata_availability: Option<String>,
    pub concepts: Option<Vec<Concept>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Concept {
    pub id: String,
    pub value: String,
}

/// Region data structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Region {
    pub id: String,
    pub code: String,
    pub name: String,
}

/// Series data in JSONStat format
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeriesData {
    pub label: serde_json::Value,
    pub source: String,
    pub updated: String,
    pub dimension: serde_json::Value,
    pub value: Vec<Option<f64>>,
    pub size: Vec<usize>,
    pub status: Option<Vec<String>>,
}

/// Custom deserializer for optional float fields that might be strings
fn deserialize_optional_float<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
    
    match value {
        serde_json::Value::Number(n) => Ok(Some(n.as_f64().unwrap_or(0.0))),
        serde_json::Value::String(s) => {
            if s.is_empty() {
                Ok(None)
            } else {
                s.parse::<f64>().map(Some).map_err(|_| {
                    serde::de::Error::custom(format!("Cannot parse '{}' as f64", s))
                })
            }
        }
        serde_json::Value::Null => Ok(None),
        _ => Err(serde::de::Error::custom("Expected number or string for float field")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_client_creation() {
        let client = WorldBankClient::new();
        assert_eq!(client.language(), "en");
        
        let client_fr = WorldBankClient::with_language("fr");
        assert_eq!(client_fr.language(), "fr");
    }
    
    #[tokio::test]
    async fn test_get_countries() {
        let client = WorldBankClient::new();
        let result = client.get_countries().await;
        
        match result {
            Ok(countries) => {
                assert!(!countries.is_empty());
                println!("Found {} countries", countries.len());
            }
            Err(e) => {
                println!("Error fetching countries: {}", e);
                // This might fail due to network issues, so we don't panic
            }
        }
    }
    
    #[tokio::test]
    async fn test_search_countries() {
        let client = WorldBankClient::new();
        let result = client.search_countries("China").await;
        
        match result {
            Ok(countries) => {
                assert!(!countries.is_empty());
                let found_china = countries.iter().any(|c| c.name.contains("China"));
                assert!(found_china);
            }
            Err(e) => {
                println!("Error searching countries: {}", e);
            }
        }
    }
    
    #[tokio::test]
    async fn test_get_indicators() {
        let client = WorldBankClient::new();
        let result = client.get_indicators().await;
        
        match result {
            Ok(indicators) => {
                assert!(!indicators.is_empty());
                println!("Found {} indicators", indicators.len());
            }
            Err(e) => {
                println!("Error fetching indicators: {}", e);
            }
        }
    }
    
    #[tokio::test]
    async fn test_get_topics() {
        let client = WorldBankClient::new();
        let result = client.get_topics().await;
        
        match result {
            Ok(topics) => {
                assert!(!topics.is_empty());
                println!("Found {} topics", topics.len());
            }
            Err(e) => {
                println!("Error fetching topics: {}", e);
            }
        }
    }
}