mod devdb_client;
mod proto;
use anyhow::Result;
use devdb_client::client::DevDBClient;
use dpm::{DaqError, DpmData};
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

    // --- DPM (DAQ) test ---
    let dpm_endpoint = "http://[::1]:50051/";
    let drf_list = vec![
        "G:AMANDA".to_string(),
        "G|AMANDA".to_string(),
        "M:OUTTMP".to_string(),
    ];
    match dpm::fetch_readings(dpm_endpoint, drf_list).await {
        Ok(reading) => println!("DPM Reading: {:?}", reading),
        Err(e) => println!("DPM ERROR: {:?}", e),
    };

    // --- IOC Alarms test ---
    let ioc_endpoint = "http://10.200.24.128:6802";
    match ioc_alarms::fetch_ioc_alarm(ioc_endpoint, "myPV".into()).await {
        Ok(resp) => println!("IOC OK: {:?}", resp),
        Err(e) => println!("IOC error: {e:?}"),
    }

    Ok(())
}
