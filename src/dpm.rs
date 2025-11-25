use futures::Stream;
use tonic::Status;

use crate::proto::common::device::value;
use crate::proto::services::daq::daq_client::DaqClient;
use crate::proto::services::daq::{ReadingList, ReadingReply, reading_reply};

#[derive(Debug)]
pub struct DaqError {
    error_text: String,
}

impl std::fmt::Display for DaqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DaqError: {}", self.error_text)
    }
}

impl std::error::Error for DaqError {}

#[derive(Debug)]
pub enum DpmData {
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

pub async fn fetch_readings(
    endpoint: &str,
    drf_list: Vec<String>,
) -> Result<
    impl Stream<Item = Result<ReadingReply, Status>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut client = DaqClient::connect(endpoint.to_string()).await?;
    let stream = client
        .read(ReadingList { drf: drf_list })
        .await?
        .into_inner();
    Ok(stream)
}

pub fn parse_reply(
    reply: &ReadingReply,
) -> Result<DpmData, Box<dyn std::error::Error + Send + Sync>> {
    let reply_index = reply.index;
    match &reply.value {
        Some(reading_reply::Value::Readings(readings)) => {
            let reading = &readings.reading;
            let rdg = &reading[0];
            Ok(DpmData::DpmReading(DpmReading {
                index: reply_index,
                timestamp: rdg
                    .timestamp
                    .map(|v| v.seconds as f64 + v.nanos as f64 / 1_000_000_000.0)
                    .unwrap(),
                data: rdg
                    .data
                    .as_ref()
                    .map(|v| v.value.clone())
                    .flatten()
                    .unwrap(),
            }))
        }
        Some(reading_reply::Value::Status(status)) => Ok(DpmData::DpmStatus(DpmStatus {
            index: reply_index,
            facility_code: status.facility_code,
            status_code: status.status_code,
            message: status.message.clone(),
        })),
        None => Err(Box::new(DaqError {
            error_text: "Empty reply value".to_string(),
        })),
    }
}
