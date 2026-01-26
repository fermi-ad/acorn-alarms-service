use crate::dpm::{AlarmRequest, AlarmType};
use crate::proto::services::devdb::{AlarmInfoReply, DeviceList, dev_db_client::DevDbClient};
use anyhow::Result;
use tonic::{Request, transport::Channel};

/// Fetch alarm info
pub async fn get_alarm_info(
    client: &mut DevDbClient<Channel>,
    names: Vec<String>,
) -> Result<Vec<AlarmRequest>> {
    // logging for when data is requested from devdb
    tracing::info!(
        "Requesting alarm info from ACNET Device DB for {} devices: {:?}",
        names.len(),
        names
    );
    let request = Request::new(DeviceList { device: names });

    let reply: AlarmInfoReply = client.get_all_alarm_info(request).await?.into_inner();

    // Log how much data DevDB returned
    tracing::info!(
        "ACNET Device DB returned {} alarm info entries",
        reply.alarm_info.len()
    );

    let alarm_requests: Vec<AlarmRequest> = reply
        .alarm_info
        .iter()
        .map(|alarm_info| {
            // each alarm entry
            tracing::info!(
                "ACNET Device DB alarm entry -> device: {}, pi: {:?}",
                alarm_info.device_name,
                alarm_info.alarm_block.as_ref().map(|b| b.pi)
            );
            AlarmRequest {
                device: alarm_info.device_name.clone(),
                // 1 -> signifies analaog alarm block
                // 5 -> signifies digital alarm block
                // Anything else should result in an error
                alarm_type: match alarm_info.alarm_block.as_ref().unwrap().pi {
                    1 => AlarmType::Analog,
                    5 => AlarmType::Digital,
                    _ => panic!(),
                },
            }
        })
        .collect();

    // Log whats produced for DPM
    tracing::info!(
        "ACNET Device DB built {} AlarmRequests for DPM",
        alarm_requests.len()
    );

    Ok(alarm_requests)
}
