//! Example usage of the World Bank API module
//! 
//! This example demonstrates how to use the econfetcher library
//! to fetch data from the World Bank Data API.

use econfetcher::world_bank::{WorldBankClient, SeriesOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new client
    let client = WorldBankClient::new();
    
    println!("=== World Bank Data API Example ===\n");
    
    // Example 1: Get all countries
    println!("1. Fetching countries...");
    match client.get_countries().await {
        Ok(countries) => {
            println!("   Found {} countries", countries.len());
            if let Some(first_country) = countries.first() {
                println!("   First country: {} ({})", first_country.name, first_country.id);
            }
        }
        Err(e) => println!("   Error fetching countries: {}", e),
    }
    
    // Example 2: Get specific country
    println!("\n2. Fetching China's information...");
    match client.get_country("CHN").await {
        Ok(china) => {
            println!("   Country: {}", china.name);
            println!("   Capital: {}", china.capital_city);
            println!("   Region: {}", china.region.value);
            println!("   Income level: {}", china.income_level.value);
        }
        Err(e) => println!("   Error fetching China: {}", e),
    }
    
    // Example 3: Search countries
    println!("\n3. Searching for countries with 'United' in the name...");
    match client.search_countries("United").await {
        Ok(countries) => {
            println!("   Found {} matching countries:", countries.len());
            for country in countries.iter().take(3) {
                println!("   - {}", country.name);
            }
        }
        Err(e) => println!("   Error searching countries: {}", e),
    }
    
    // Example 4: Get indicators
    println!("\n4. Fetching indicators...");
    match client.get_indicators().await {
        Ok(indicators) => {
            println!("   Found {} indicators", indicators.len());
            if let Some(first_indicator) = indicators.first() {
                println!("   First indicator: {} ({})", first_indicator.name, first_indicator.id);
            }
        }
        Err(e) => println!("   Error fetching indicators: {}", e),
    }
    
    // Example 5: Search for population indicator
    println!("\n5. Searching for population indicators...");
    match client.search_indicators("population").await {
        Ok(indicators) => {
            println!("   Found {} population-related indicators", indicators.len());
            for indicator in indicators.iter().take(3) {
                println!("   - {}: {}", indicator.id, indicator.name);
            }
        }
        Err(e) => println!("   Error searching indicators: {}", e),
    }
    
    // Example 6: Get population data for China (most recent value)
    println!("\n6. Fetching China's population data (most recent)...");
    let options = SeriesOptions {
        country: Some("CHN".to_string()),
        mrv: Some(1),
        date: None,
        gapfill: None,
    };
    
    match client.get_series("SP.POP.TOTL", &options).await {
        Ok(series_data) => {
            println!("   Successfully fetched population data for China");
            println!("   Data label: {:?}", series_data.label);
            println!("   Number of data points: {}", series_data.value.len());
            println!("   Source: {}", series_data.source);
        }
        Err(e) => println!("   Error fetching population data: {}", e),
    }
    
    // Example 7: Get topics
    println!("\n7. Fetching available topics...");
    match client.get_topics().await {
        Ok(topics) => {
            println!("   Found {} topics", topics.len());
            for topic in topics.iter().take(5) {
                println!("   - {}: {}", topic.id, topic.value);
            }
        }
        Err(e) => println!("   Error fetching topics: {}", e),
    }
    
    // Example 8: Get sources
    println!("\n8. Fetching data sources...");
    match client.get_sources().await {
        Ok(sources) => {
            println!("   Found {} sources", sources.len());
            for source in sources.iter().take(3) {
                println!("   - {}: {}", source.id, source.name);
            }
        }
        Err(e) => println!("   Error fetching sources: {}", e),
    }
    
    // Example 9: Using a different language
    println!("\n9. Testing French language...");
    let client_fr = WorldBankClient::with_language("fr");
    match client_fr.get_country("FRA").await {
        Ok(france) => {
            println!("   Pays: {}", france.name);
            println!("   Capitale: {}", france.capital_city);
        }
        Err(e) => println!("   Error fetching France in French: {}", e),
    }
    
    println!("\n=== Example completed ===");
    
    Ok(())
}