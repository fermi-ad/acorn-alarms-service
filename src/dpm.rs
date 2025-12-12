use chrono::{DateTime, TimeZone, Utc};
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

#[derive(Debug, PartialEq)]
pub enum DpmData {
    DpmReading(DpmReading),
    DpmStatus(DpmStatus),
}
#[derive(Debug, PartialEq)]
pub struct DpmReading {
    pub index: u32,
    pub timestamp: DateTime<Utc>,
    pub data: value::Value,
}

#[derive(Debug, PartialEq)]
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
            let reading = readings.reading.first().ok_or("No readings in reply")?;
            let raw_timestamp = reading
                .timestamp
                .as_ref()
                .ok_or_else(|| "missing timestamp".to_string())?;
            Ok(DpmData::DpmReading(DpmReading {
                index: reply_index,
                timestamp: Utc
                    .timestamp_opt(raw_timestamp.seconds, raw_timestamp.nanos as u32)
                    .single()
                    .ok_or_else(|| DaqError {
                        error_text: "Invalid timestamp value".to_string(),
                    })?,
                data: reading
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

    use crate::proto::{
        common::{
            self,
            device::{self, value::AnalogAlarm},
            status::Status,
        },
        services::daq::{Reading, Readings},
    };

    use super::*;

    #[test]
    fn test_status_reading_reply() {
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

    #[test]
    fn test_analog_alarm_reply_parsed() {
        let now = Utc::now();
        let analog_reply = ReadingReply {
            index: 0,
            value: Some(reading_reply::Value::Readings(Readings {
                reading: vec![Reading {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    data: Some(device::Value {
                        value: Some(device::value::Value::AnaAlarm(AnalogAlarm {
                            minimum: 1.0,
                            maximum: 5.0,
                            alarm_enable: true,
                            alarm_status: false,
                            abort: false,
                            abort_inhibit: false,
                            tries_needed: 10,
                            tries_now: 2,
                        })),
                    }),
                    status: Some(common::status::Status {
                        facility_code: 1,
                        status_code: 1,
                        message: "Error".to_string(),
                    }),
                }],
            })),
        };

        let parsed_data = DpmData::DpmReading(DpmReading {
            index: 0,
            timestamp: now,
            data: value::Value::AnaAlarm(AnalogAlarm {
                minimum: 1.0,
                maximum: 5.0,
                alarm_enable: true,
                alarm_status: false,
                abort: false,
                abort_inhibit: false,
                tries_needed: 10,
                tries_now: 2,
            }),
        });

        assert_eq!(parse_reply(&analog_reply).unwrap(), parsed_data);
    }

    #[test]
    fn test_daq_error_fmt_impl() {
        let err = DaqError {
            error_text: "This is an error".to_string(),
        };
        assert_eq!(format!("{}", err), "DaqError: This is an error");
    }
}
