use crate::dpm::{AlarmRequest, AlarmType};
use crate::proto::services::devdb::{
    AlarmInfo, AlarmInfoReply, DeviceList, dev_db_client::DevDbClient,
};
use anyhow::Result;
use tonic::{Request, transport::Channel};

use std::collections::HashMap;

//fetch alarm info
pub async fn get_alarm_info(
    client: &mut DevDbClient<Channel>,
    names: Vec<String>,
) -> Result<Vec<AlarmRequest>> {
    // logging for when data is requested from devdb
    tracing::info!(
        "Requesting alarm info from DevDB for {} devices: {:?}",
        names.len(),
        names
    );
    let req = Request::new(DeviceList { device: names });
    let reply: AlarmInfoReply = client.get_all_alarm_info(req).await?.into_inner();

    // Log how much data DevDB returned
    tracing::info!(
        "DevDB returned {} alarm info entries",
        reply.alarm_info.len()
    );

    let alarm_requests: Vec<AlarmRequest> = reply
        .alarm_info
        .iter()
        .map(|alarm_info: &AlarmInfo| {
            tracing::debug!(
                "ACNET Device DB alarm entry -> device: {}, pi: {:?}",
                alarm_info.device_name,
                alarm_info.alarm_block.as_ref().map(|b| b.pi)
            );

            build_alarm_request(alarm_info)
        })
        .collect();

    tracing::info!(
        "Total alarm blocks returned from DevDB (raw): {}",
        alarm_requests.len()
    );

    // De-duplication
    let mut unique: HashMap<(String, AlarmType), AlarmRequest> = HashMap::new();
    let raw_count = alarm_requests.len();
    for alarm in alarm_requests {
        let key = (alarm.device.clone(), alarm.alarm_type);
        unique.entry(key).or_insert(alarm);
    }

    let deduped: Vec<AlarmRequest> = unique.into_values().collect();

    tracing::info!("Total alarm blocks after deduplication: {}", deduped.len());

    // Log whats produced for DPM
    tracing::info!("Built {} AlarmRequests for DPM", raw_count);

    Ok(deduped)
}

fn build_alarm_request(alarm_info: &AlarmInfo) -> AlarmRequest {
    AlarmRequest {
        device: alarm_info.device_name.clone(),
        // 1 -> signifies analaog alarm block
        // 5 -> signifies digital alarm block
        // Anything else should result in an error
        alarm_type: match alarm_info.alarm_block.as_ref().unwrap().pi {
            1 => AlarmType::Analog,
            5 => AlarmType::Digital,
            _ => panic!("Unknown alarm block type"),
        },
    }
}
