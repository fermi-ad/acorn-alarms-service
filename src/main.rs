mod devdb_client;
mod proto;
use anyhow::Result;
use devdb_client::client::DevDBClient;
use dpm::{DaqError, DpmData};
use tokio_stream::StreamExt;

use crate::dpm::DpmData;

fn handle_daq_data(data: DpmData) {
    match data {
        DpmData::DpmReading(reading) => {
            println!("Reading!");
            println!("index: {:?}", reading.index);
            println!("timestamp: {:?}", reading.timestamp);
            println!("reading: {:?}", reading.data);
        }
        DpmData::DpmStatus(status) => {
            println!("Status!");
            println!("Index: {:?}", status.index);
            println!("Facility Code: {:?}", status.facility_code);
            println!("Status Code: {:?}", status.status_code);
            println!("Message: {:?}", status.message);
        }
    }
}

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
        "M:OUTTMP@1h".to_string(),
    ];
    match dpm::fetch_readings(dpm_endpoint, drf_list).await {
        Ok(mut stream) => {
            while let Some(data) = stream.next().await {
                match data {
                    Ok(reading) => {
                        println!("DATA: {:?}", reading);
                        handle_daq_data(dpm::parse_reply(&reading)?);
                    }
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
