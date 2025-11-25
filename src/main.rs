mod devdb;
mod dpm;
mod ioc_alarms;
mod proto;

use dpm::{DaqError, DpmData};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Acorn Alarms Service starting…");

    // --- DevDB test ---
    let devdb_endpoint = "http://10.200.24.105:6802";
    let devices = vec!["M:OUTTMP".to_string()];
    match devdb::fetch_device_info(devdb_endpoint, devices).await {
        Ok(info) => println!("DevDB OK: {:?}", info),
        Err(e) => println!("DevDB error: {e:?}"),
    }

    // --- DPM (DAQ) test ---
    let dpm_endpoint = "http://[::1]:50051/";
    let drf_list = vec![
        "G:AMANDA".to_string(),
        "G|AMANDA".to_string(),
        "M:OUTTMP".to_string(),
    ];
    match dpm::fetch_readings(dpm_endpoint, drf_list).await {
        Ok(mut stream) => {
            while let Some(data) = stream.next().await {
                match data {
                    Ok(reading) => println!("DPM Reading: {:?}", dpm::parse_reply(&reading)),
                    Err(e) => println!("DPM stream error: {:?}", e),
                }
            }
        }
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
