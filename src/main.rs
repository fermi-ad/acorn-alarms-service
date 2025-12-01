mod devdb_client;
mod dpm;
pub mod ioc_alarms_client;
mod proto;

use anyhow::Result;
use devdb_client::client::DevDBClient;
use ioc_alarms_client::client::IOCAlarmsClient;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Acorn Alarms Service starting…");

    let endpoint = "http://10.200.24.105:6802";
    let mut client = DevDBClient::connect(endpoint).await?;

    // --- Test Device Info ---
    let names = vec![];

    match client.get_device_info(names.clone()).await {
        Ok(summary) => println!("DEVICE INFO = {:#?}", summary),
        Err(e) => println!("DevDB error: {e:?}"),
    }

    // --- Test Alarm Info ---

    match client.get_all_alarm_info(names).await {
        Ok(alarms) => println!("ALARM INFO = {:#?}", alarms),
        Err(e) => println!("DevDB alarm error: {e:?}"),
    }

    // --- IOC Alarms test ---
    let ioc_endpoint = "http://10.200.24.128:6802";
    let mut ioc = IOCAlarmsClient::connect(ioc_endpoint).await?;

    match ioc
        .fetch_alarm("linac:area1:ioc-folder2:ioc3:quade-magnet1".to_string())
        .await?
    {
        Some(alarm) => println!("IOC Alarm = {:#?}", alarm),
        None => println!("No alarm returned for PV"),
    }

    Ok(())
}
