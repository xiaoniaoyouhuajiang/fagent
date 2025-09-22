# World Bank API Module

This module provides an asynchronous interface to the World Bank Data API v2, allowing users to fetch economic data, indicators, countries, and other information.

## Features

- **Asynchronous API**: All functions are async for optimal performance
- **Comprehensive coverage**: Supports countries, indicators, series data, topics, sources, and regions
- **Search functionality**: Built-in search for countries and indicators
- **Language support**: Support for multiple languages
- **Error handling**: Proper error types for robust error handling
- **Flexible options**: Configurable options for data retrieval

## Quick Start

```rust
use econfetcher::world_bank::{WorldBankClient, SeriesOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client
    let client = WorldBankClient::new();
    
    // Get all countries
    let countries = client.get_countries().await?;
    println!("Found {} countries", countries.len());
    
    // Get a specific country
    let china = client.get_country("CHN").await?;
    println!("Country: {}", china.name);
    
    // Search for countries
    let results = client.search_countries("China").await?;
    for country in results {
        println!("Found: {}", country.name);
    }
    
    // Get population data for China (most recent value)
    let options = SeriesOptions {
        country: Some("CHN".to_string()),
        mrv: Some(1),
        date: None,
        gapfill: None,
    };
    
    let population_data = client.get_series("SP.POP.TOTL", &options).await?;
    println!("Fetched {} data points", population_data.value.len());
    
    Ok(())
}
```

## API Reference

### WorldBankClient

The main client for interacting with the World Bank API.

#### Methods

- `new()` - Create a new client with default settings
- `with_language(lang)` - Create a client with specific language
- `get_countries()` - Get all countries
- `get_country(id)` - Get a specific country by ID
- `search_countries(pattern)` - Search countries by name or ID
- `get_indicators()` - Get all indicators
- `get_indicator(id)` - Get a specific indicator by ID
- `search_indicators(pattern)` - Search indicators by name or ID
- `get_series(indicator_id, options)` - Get series data for an indicator
- `get_topics()` - Get available topics
- `get_sources()` - Get data sources
- `get_regions()` - Get regions

### Data Structures

- `Country` - Country information
- `Indicator` - Indicator metadata
- `SeriesData` - Time series data in JSONStat format
- `Topic` - Topic information
- `Source` - Data source information
- `Region` - Region information
- `SeriesOptions` - Options for fetching series data

## Examples

See the `examples/` directory for complete usage examples.

## Dependencies

- `reqwest` - HTTP client
- `serde` - Serialization framework
- `tokio` - Async runtime
- `thiserror` - Error handling

## Error Handling

The module uses `WorldBankError` for error handling:

```rust
match client.get_countries().await {
    Ok(countries) => {
        // Process countries
    }
    Err(WorldBankError::RequestError(e)) => {
        // Handle HTTP request errors
    }
    Err(WorldBankError::ApiError(msg)) => {
        // Handle API errors
    }
    Err(e) => {
        // Handle other errors
    }
}
```

## Running Examples

```bash
cargo run --example world_bank_example
```

## Testing

```bash
cargo test
```