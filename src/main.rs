use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Alarm Service starting…");

    let endpoint =
        std::env::var("DEVDB_ADDR").unwrap_or_else(|_| "http://localhost:6802".to_string());
    let names_env = std::env::var("DEV_NAMES").unwrap_or_else(|_| "Z:TEST:DEV1".to_string());
    let devices: Vec<String> = names_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    println!("Connecting to DevDB at {endpoint} for {:?}", devices);

    match devdb_client::fetch_device_info(&endpoint, &devices).await {
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

                        let r_range = r
                            .map(|p| format!("[{}, {}]", p.min_val, p.max_val))
                            .unwrap_or_else(|| "-".into());
                        let s_range = s
                            .map(|p| format!("[{}, {}]", p.min_val, p.max_val))
                            .unwrap_or_else(|| "-".into());

                        let dig_bits = info.dig_status.as_ref().map(|d| d.bits.len()).unwrap_or(0);
                        let ext_bits = info
                            .dig_status
                            .as_ref()
                            .map(|d| d.ext_bits.len())
                            .unwrap_or(0);
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

            // Save snapshot
            let _ = std::fs::create_dir_all("mocks");
            let _ = std::fs::write("mocks/devdb_snapshot.txt", format!("{:#?}", &reply));
            println!("Saved DevDB snapshot to mocks/devdb_snapshot.txt");
        }
        Err(e) => {
            eprintln!("DevDB call failed: {e}");
        }
    }

    Ok(())
}
