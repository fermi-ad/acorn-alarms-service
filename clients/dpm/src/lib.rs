// clients/dpm/src/lib.rs

// ===== Generated proto modules =====
// DAQ service (package: services.daq)
pub mod services {
    pub mod daq {
        tonic::include_proto!("services.daq");
    }
}

// Common types used by DAQ (packages: common.device, common.status)
pub mod common {
    pub mod device {
        tonic::include_proto!("common.device");
    }
    pub mod status {
        tonic::include_proto!("common.status");
    }
}

// ===== Public API used by the root binary =====

#[cfg(feature = "mock")]
use anyhow::Result;

#[cfg(feature = "mock")]
use serde_json::Value;

#[cfg(feature = "mock")]
use std::fs;

/// Load mock readings from a JSON file (mocks/dpm.json).
/// Exposed as dpm_client::load_mock()
#[cfg(feature = "mock")]
pub fn load_mock(path: &str) -> Result<Vec<Value>> {
    let text = fs::read_to_string(path)?;
    let data: Vec<Value> = serde_json::from_str(&text)?;

    // Optional: print a quick preview (kept for compatibility with your main.rs)
    println!("\nDPM (mock) readings:");
    for entry in &data {
        if let Some(obj) = entry.as_object() {
            let name = obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>");
            let value = obj.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let ts = obj.get("ts").and_then(|v| v.as_str()).unwrap_or("-");
            let quality = obj.get("quality").and_then(|v| v.as_str()).unwrap_or("-");
            println!("• {}  {}  @{}  [{}]", name, value, ts, quality);
        }
    }

    Ok(data)
}

/// Connect to the DAQ (DPM) service and (for now) just establish channel connectivity.
/// Exposed as dpm_client::fetch_dpm_readings()
#[cfg(feature = "live")]
pub async fn fetch_dpm_readings(endpoint: &str, _devices: &[String]) -> anyhow::Result<()> {
    use services::daq::ReadingList;
    use services::daq::daq_client::DaqClient;
    use tonic::transport::Channel;

    // Build a gRPC channel from the endpoint string.
    // Accept both "http://host:port" and "host:port" forms.
    let channel: Channel = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Channel::from_shared(endpoint.to_string())?
            .connect()
            .await?
    } else {
        Channel::from_shared(format!("http://{}", endpoint))?
            .connect()
            .await?
    };

    let mut client = DaqClient::new(channel);

    // Minimal smoke test (non-blocking quick call):
    // Request a single "read once" DRF (example: 1 Hz for 1 sample "name@r:1")
    // If you have a known-good DRF string, you can swap it in here.
    let req = ReadingList {
        drf: vec!["M:OUTTMP@r:1".to_string()],
    };

    // We won't fully drain the stream here—just initiate to validate the method exists.
    let mut stream = client.read(req).await?.into_inner();

    // Pull at most one message if available (ok if none comes immediately).
    if let Some(_msg) = stream.message().await? {
        // Successfully received a frame; leave printing to the binary if desired.
    }

    Ok(())
}
