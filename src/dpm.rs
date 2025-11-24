use anyhow::Result;
use std::error::Error;

use crate::proto::common::device::value;
use crate::proto::services::daq::daq_client::DaqClient;
use crate::proto::services::daq::{ReadingList, ReadingReply, reading_reply};

#[derive(Debug)]
enum DpmData {
    DpmReading(DpmReading),
    DpmStatus(DpmStatus),
}
#[derive(Debug)]
struct DpmReading {
    index: u32,
    timestamp: f64,
    data: value::Value,
}

#[derive(Debug)]
struct DpmStatus {
    index: u32,
    facility_code: i32,
    status_code: i32,
    message: String,
}

pub async fn fetch_readings(endpoint: &str, drf_list: Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut client = DaqClient::connect(endpoint.to_string()).await?;

    let mut stream = client
        .read(ReadingList { drf: drf_list })
        .await?
        .into_inner();

    // let mut out = Vec::new();
    while let reply = stream.message().await? {
        match reply {
            Some(reply) => {
                println!("Reading: {:?}", parse_reply(&reply))
            }
            None => println!("No Value given"),
        }
    }

    Ok(())
}

fn parse_reply(reply: &ReadingReply) -> Option<DpmData> {
    let reply_index = reply.index;
    match &reply.value {
        Some(reading_reply::Value::Readings(readings)) => {
            let reading = &readings.reading;
            let rdg = &reading[0];
            Some(DpmData::DpmReading(DpmReading {
                index: reply_index,
                timestamp: rdg
                    .timestamp
                    .map(|v| v.seconds as f64 + v.nanos as f64 / 1_000_000_000.0)?,
                data: rdg.data.as_ref().map(|v| v.value.clone()).flatten()?,
            }))
        }
        Some(reading_reply::Value::Status(status)) => Some(DpmData::DpmStatus(DpmStatus {
            index: reply_index,
            facility_code: status.facility_code,
            status_code: status.status_code,
            message: status.message.clone(),
        })),
        None => None,
    }
}
