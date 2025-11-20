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

/*use anyhow::Result;
use std::env;
use tokio;

// DevDB client (optional)

#[cfg(feature = "devdb")]
use devdb_client;

// DPM client (mock or live)

#[cfg(feature = "dpm-live")]
use dpm_client::fetch_readings as fetch_dpm_readings;

#[cfg(feature = "dpm-mock")]
use serde_json;

// IOC Alarms client

#[cfg(feature = "ioc-live")]
use ioc_alarms::fetch_ioc_alarm;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Alarm Service starting…");

    let endpoint = env::var("DEVDB_ADDR").unwrap_or_else(|_| "http://localhost:6802".to_string());

    let names_env =
        env::var("DEV_NAMES").unwrap_or_else(|_| "M:OUTTMP,G:BEAU,G:AMANDA".to_string());
    let devices: Vec<String> = names_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // DEVDB SECTION

    #[cfg(feature = "devdb")]
    {
        println!("Connecting to DevDB at {endpoint} for {:?}", devices);

        match devdb_client::fetch_device_info(&endpoint, devices.clone()).await {
            Ok(reply) => {
                println!("DevDB reply: {} item(s)", reply.set.len());
            }
            Err(e) => eprintln!("DevDB call failed: {e}"),
        }
    }

    // DPM SECTION (LIVE)

    #[cfg(feature = "dpm-live")]
    {
        let dpm_endpoint = env::var("DPM_ADDR").unwrap_or_else(|_| "http://localhost:6802".into());

        println!("Connecting to LIVE DPM at {}", dpm_endpoint);

        match fetch_dpm_readings(&dpm_endpoint, devices.clone()).await {
            Ok(responses) => {
                println!("Received {} readings from DPM", responses.len());
            }
            Err(e) => eprintln!("DPM live error: {e}"),
        }
    }

    // DPM SECTION (MOCK)

    #[cfg(feature = "dpm-mock")]
    {
        println!("Using mock DPM data (mocks/dpm.json)");
        let file = std::fs::read_to_string("mocks/dpm.json")?;
        let mock: serde_json::Value = serde_json::from_str(&file)?;
        println!("Loaded {} mock records", mock.as_array().unwrap().len());
    }

    // IOC ALARMS (LIVE)

    #[cfg(feature = "ioc-live")]
    {
        println!("Connecting to IOC Alarms service at {}", endpoint);

        for pv in &devices {
            match fetch_ioc_alarm(&endpoint, pv).await {
                Ok(resp) => {
                    println!("• {} => value={}", pv, resp.value);
                }
                Err(e) => {
                    println!("• {} → ERROR: {}", pv, e);
                }
            }
        }
    }

    Ok(())
}

// old block kept for reference, no longer used since load_mock removed: */

/*
match load_mock("mocks/dpm.json") {
    Ok(readings) => {
        let units_for = |name: &str| -> &str {
            if let Some(reply) = &_devdb_reply {
                if let Some(units) = reply.set.iter().find_map(|entry| {
                    if entry.name == name {
                        if let Some(devdb_client::devdb::info_entry::Result::Device(info)) =
                            &entry.result
                        {
                            return info
                                .reading
                                .as_ref()
                                .and_then(|p| p.primary_units.as_deref());
                        }
                    }
                    None
                }) {
                    return units;
                }
            }
            "-"
        };

        println!("\nDPM readings:");
        for name in &devices {
            let maybe = readings.iter().find(|entry| {
                entry
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s == name)
                    .unwrap_or(false)
            });

            if let Some(entry) = maybe {
                let value = entry.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let ts = entry.get("ts").and_then(|v| v.as_str()).unwrap_or("-");
                let quality = entry.get("quality").and_then(|v| v.as_str()).unwrap_or("-");
                let units = units_for(name);
                println!("• {name}  {value} {units}  @{ts}  [{quality}]");
            } else {
                println!("• {name}  <no mock reading>");
            }
        }
    }
    Err(e) => eprintln!("Could not load mocks/dpm.json: {e}"),
}
*/
