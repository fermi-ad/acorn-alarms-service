mod devdb;
mod dpm;
mod ioc_alarms;
mod proto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Acorn Alarms Service starting…");

    // --- DevDB test ---
    // let devdb_endpoint = "http://10.200.24.105:6802";
    // let devices = vec!["M:OUTTMP".to_string()];
    // match devdb::fetch_device_info(devdb_endpoint, devices).await {
    //     Ok(info) => println!("DevDB OK: {:?}", info),
    //     Err(e) => println!("DevDB error: {e:?}"),
    // }

    // --- DPM (DAQ) test ---
    let dpm_endpoint = "http://[::1]:50051/";
    let drf_list = vec!["G:AMANDA".to_string(), "M:OUTTMP".to_string()];
    dpm::fetch_readings(dpm_endpoint, drf_list).await?;

    // --- IOC Alarms test ---
    // let ioc_endpoint = "http://10.200.24.128:6802";
    // match ioc_alarms::fetch_ioc_alarm(ioc_endpoint, "myPV".into()).await {
    //     Ok(resp) => println!("IOC OK: {:?}", resp),
    //     Err(e) => println!("IOC error: {e:?}"),
    // }

    Ok(())
}
