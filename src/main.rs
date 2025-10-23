use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Alarm Service starting…");

    let endpoint =
        std::env::var("DEVDB_ADDR").unwrap_or_else(|_| "http://localhost:6802".to_string());
    let names_env = std::env::var("DEV_NAMES").unwrap_or_else(|_| "Z:TEST:DEV1".to_string());
    let devices: Vec<String> = names_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    println!("Connecting to DevDB at {endpoint} for {:?}", devices);

    match devdb_client::fetch_device_info(&endpoint, &devices).await {
        Ok(reply) => {
            println!("DevDB reply: {} item(s)", reply.set.len());
            for entry in reply.set.iter().take(5) {
                println!("• {} → {:?}", entry.name, entry.result.as_ref().map(|_| "ok or err"));
            }
        }
        Err(e) => eprintln!("DevDB call failed: {e}"),
    }

    Ok(())
}
