mod devdb;
mod dpm;
mod ioc_alarms;
mod proto;

use anyhow::Result;
use tonic::transport::Channel;
//use crate::{devdb, dpm, ioc_alarms};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Acorn Alarms Service starting…");

    // --- DevDB test ---
    let devdb_endpoint = "http://10.200.24.105:6802";
    let devices = vec!["M:OUTTMP".to_string()];
    match devdb::fetch_device_info(devdb_endpoint, devices).await {
        Ok(info) => println!("DevDB OK: {:?}", info),
        Err(e) => println!("DevDB error: {e:?}"),
    }

    // --- DPM (DAQ) test ---
    let dpm_endpoint = "http://dce07.fnal.gov:50051/";
    let drf_list = vec!["G:AMANDA@1".to_string()];
    match dpm::fetch_readings(dpm_endpoint, drf_list).await {
        Ok(readings) => println!("DAQ OK: {:?}", readings),
        Err(e) => println!("DAQ error: {e:?}"),
    }

    // --- IOC Alarms test ---
    let ioc_endpoint = "http://10.200.24.128:6802";
    match ioc_alarms::fetch_ioc_alarm(ioc_endpoint, "myPV".into()).await {
        Ok(resp) => println!("IOC OK: {:?}", resp),
        Err(e) => println!("IOC error: {e:?}"),
    }

    Ok(())
}
