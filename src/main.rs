mod devdb_client;
mod proto;

use anyhow::Result;
use devdb_client::client::DevDBClient;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Acorn Alarms Service starting…");

    // NOTE: still hard-coded for now (as before)
    let endpoint = "http://10.200.24.105:6802";
    let mut client = DevDBClient::connect(endpoint).await?;

    let names = vec![];

    match client.get_device_info(names.clone()).await {
        Ok(summary) => println!("DEVICE INFO = {:#?}", summary),
        Err(e) => println!("DevDB error: {e:?}"),
    }

    match client.get_all_alarm_info(names).await {
        Ok(alarms) => println!("ALARM INFO = {:#?}", alarms),
        Err(e) => println!("DevDB alarm error: {e:?}"),
    }

    Ok(())
}
