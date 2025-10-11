use econfetcher::world_bank::Indicator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get raw JSON response
    let http_client = reqwest::Client::new();
    let url = "https://api.worldbank.org/v2/indicator?format=json&per_page=10";

    let response = http_client.get(url).send().await?;
    let text = response.text().await?;

    let json_value: serde_json::Value = serde_json::from_str(&text)?;

    if let Some(array) = json_value.as_array() {
        if array.len() >= 2 {
            if let Some(data_array) = array[1].as_array() {
                println!("Testing each indicator individually...");

                for (i, item) in data_array.iter().enumerate() {
                    match serde_json::from_value::<Indicator>(item.clone()) {
                        Ok(indicator) => {
                            println!("Indicator {}: {} - OK", i, indicator.id);
                        }
                        Err(e) => {
                            println!("Indicator {}: FAILED - {}", i, e);
                            println!("Raw data: {}", serde_json::to_string_pretty(item)?);
                            break;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
