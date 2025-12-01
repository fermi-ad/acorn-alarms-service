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
pub struct DpmReading {
    pub index: u32,
    pub timestamp: f64,
    pub data: value::Value,
}

#[derive(Debug)]
pub struct DpmStatus {
    pub index: u32,
    pub facility_code: i32,
    pub status_code: i32,
    pub message: String,
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

pub fn parse_reply(reply: &ReadingReply) -> Result<DpmData, Box<dyn std::error::Error>> {
    let reply_index = reply.index;
    match &reply.value {
        Some(reading_reply::Value::Readings(readings)) => {
            let reading = &readings.reading;
            let rdg = &reading[0];
            let ts = rdg
                .timestamp
                .as_ref()
                .ok_or_else(|| "missing timestamp".to_string())?;
            Ok(DpmData::DpmReading(DpmReading {
                index: reply_index,
                timestamp: ts.seconds as f64 + ts.nanos as f64 / 1_000_000_000.0,
                data: rdg
                    .data
                    .as_ref()
                    .and_then(|v| v.value.clone())
                    .ok_or_else(|| "missing data value".to_string())?,
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

#[cfg(test)]
mod test {
    use crate::proto::common::status::Status;

    use super::*;

    #[test]
    fn status_reading_reply() {
        let status_reply = ReadingReply {
            index: 0,
            value: Some(reading_reply::Value::Status(Status {
                facility_code: 1,
                status_code: 2,
                message: "DPM PEND".to_string(),
            })),
        };
        let parsed = parse_reply(&status_reply);
        match parsed {
            Ok(DpmData::DpmStatus(status)) => {
                assert_eq!(status.index, 0, "Incorrect index");
                assert_eq!(status.facility_code, 1, "Incorrect facility code");
                assert_eq!(status.status_code, 2, "Incorrect status code");
                assert_eq!(status.message, "DPM PEND".to_string(), "Incorrect message");
            }
            _ => panic!("Expected parsed data to be Status"),
        }
    }
}
