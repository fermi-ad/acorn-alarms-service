mod devdb_client;
mod dpm;
//mod ioc_alarms;
mod proto;

use anyhow::Result;
use devdb_client::client::DevDBClient;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Acorn Alarms Service starting…");

    let endpoint = "http://10.200.24.105:6802";
    let mut client = DevDBClient::connect(endpoint).await?;

    // --- Test Device Info ---
    let names = vec!["M:OUTTMP".to_string()];
    match client.get_device_info(names.clone()).await {
        Ok(summary) => println!("DEVICE INFO = {:#?}", summary),
        Err(e) => println!("DevDB error: {e:?}"),
    }

    // --- Test Alarm Info ---
    match client.get_all_alarm_info(names).await {
        Ok(alarms) => println!("ALARM INFO = {:#?}", alarms),
        Err(e) => println!("DevDB alarm error: {e:?}"),
    }

    Ok(())
}
