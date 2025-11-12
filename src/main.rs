#[cfg(feature = "dpm-live")]
// old: use dpm_client::fetch_dpm_readings;
use dpm_client::fetch_readings as fetch_dpm_readings;

#[cfg(feature = "ioc-live")]
use ioc_alarms_client::fetch_ioc_alarm;

// old: #[cfg(feature = "mock")]
// old: use dpm_client::load_mock;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Alarm Service starting…");

    // DevDB Connection
    let endpoint =
        std::env::var("DEVDB_ADDR").unwrap_or_else(|_| "http://localhost:6802".to_string());
    let names_env =
        std::env::var("DEV_NAMES").unwrap_or_else(|_| "M:OUTTMP,G:BEAU,G:AMANDA".to_string());
    let devices: Vec<String> = names_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    println!("Connecting to DevDB at {endpoint} for {:?}", devices);

    let mut _devdb_reply = None;

    match devdb_client::fetch_device_info(&endpoint, devices.clone()).await {
        Ok(reply) => {
            println!("DevDB reply: {} item(s)", reply.set.len());
            for entry in &reply.set {
                match &entry.result {
                    Some(devdb_client::devdb::info_entry::Result::Device(info)) => {
                        let desc = &info.description;
                        let r = info.reading.as_ref();
                        let s = info.setting.as_ref();

                        let r_units = r
                            .and_then(|p| p.primary_units.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("-");
                        let s_units = s
                            .and_then(|p| p.primary_units.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("-");

                        // old:
                        // let r_range = r
                        //     .map(|p| format!("[{}, {}]", p.min_val, p.max_val))
                        //     .unwrap_or_else(|| "-".into());
                        // let s_range = s
                        //     .map(|p| format!("[{}, {}]", p.min_val, p.max_val))
                        //     .unwrap_or_else(|| "-".into());
                        let r_range = "-";
                        let s_range = "-";

                        // old:
                        // let dig_bits = info.dig_status.as_ref().map(|d| d.primary_units.len()).unwrap_or(0);
                        // let ext_bits = info.dig_status.as_ref().map(|d| d.common_units.len()).unwrap_or(0);
                        let dig_bits = info.dig_status.as_ref().map(|d| d.bits.len()).unwrap_or(0);
                        let ext_bits = 0;

                        let cmds = info.dig_control.as_ref().map(|c| c.cmds.len()).unwrap_or(0);

                        println!("• {}", entry.name);
                        println!("  desc   : {}", desc);
                        println!("  read   : units={} range={}", r_units, r_range);
                        println!("  set    : units={} range={}", s_units, s_range);
                        println!("  bits   : {} legacy, {} ext", dig_bits, ext_bits);
                        println!("  control: {} cmds", cmds);
                    }
                    Some(devdb_client::devdb::info_entry::Result::ErrMsg(msg)) => {
                        println!("• {} → ERROR: {}", entry.name, msg);
                    }
                    None => {
                        println!("• {} → <no result>", entry.name);
                    }
                }
            }
            _devdb_reply = Some(reply);
        }
        Err(e) => {
            eprintln!("DevDB call failed: {e}");
        }
    }

    // dpm section
    #[cfg(feature = "dpm-live")]
    {
        let dpm_endpoint =
            std::env::var("DPM_ADDR").unwrap_or_else(|_| "http://adkube-pool40.fnal.gov".into());
        println!("Connecting to live DPM at {}", dpm_endpoint);
        //fetch_dpm_readings(&dpm_endpoint, devices.clone()).await?;
        fetch_dpm_readings(&dpm_endpoint, devices).await?;
    }

    #[cfg(all(not(feature = "dpm-live"), feature = "mock"))]
    {
        println!("Using mock DPM data (mocks/dpm.json)");
        // old block kept for reference, no longer used since load_mock removed:

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
    }
    #[cfg(feature = "ioc-live")]
    {
        let ioc_endpoint =
            std::env::var("IOC_ADDR").unwrap_or_else(|_| "http://localhost:6802".to_string());

        println!("Connecting to IOC Alarms service at {}", ioc_endpoint);

        // Example PVs 
        let pv_names = vec![
            "linac:area1:ioc-folder2:ioc3:quade-magnet1",
            "booster:area2:ioc-folder1:ioc1:dipole-magnet1",
            "booster:area2:ioc-folder1:ioc2:bpm3",
            "booster:area3:ioc-folder2:ioc1:octo-magnet1",
            "main-injector:area1:ioc-folder1:ioc2:tmp-sensor3",
        ];

        for pv in pv_names {
            match fetch_ioc_alarm(&ioc_endpoint, pv).await {
                Ok(resp) => println!("• {pv} → {:?}", resp),
                Err(e) => eprintln!("• {pv} → ERROR: {e}"),
            }
        }
    }

    Ok(())
}
