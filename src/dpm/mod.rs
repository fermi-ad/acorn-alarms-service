use chrono::{DateTime, TimeZone, Utc};
use futures::{Stream, StreamExt};

use crate::proto::common::device::value;
use crate::proto::services::daq::{
    ReadingList, ReadingReply, daq_client::DaqClient, reading_reply,
};

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
    Reading(DpmReading),
    Status(DpmStatus),
}
#[derive(Debug, PartialEq)]
pub struct DpmReading {
    pub index: u32,
    pub device: String,
    pub alarm_type: AlarmType,
    pub timestamp: DateTime<Utc>,
    pub data: value::Value,
}

#[derive(Debug, PartialEq)]
pub struct DpmStatus {
    pub index: u32,
    pub device: String,
    pub alarm_type: AlarmType,
    pub facility_code: u8,
    pub status_code: i8,
    pub message: String,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum AlarmType {
    Analog,
    Digital,
    Value,
}

pub struct AlarmRequest {
    pub device: String,
    pub alarm_type: AlarmType,
}

/// Takes AlarmRequest information and builds out DRF String
///
/// # DRF String being used
/// `DEVICE.{DA|AA}@Q`
///
/// - Device: The device name
/// - DA|AA: The property requested. Digital alarm or Analog
/// - Q: The event data will be returned. This will be periodic, and only be returned when the alarm block changes
fn build_drf(request: &AlarmRequest) -> String {
    let request_properties = match request.alarm_type {
        AlarmType::Analog => ".AA@Q",
        AlarmType::Digital => ".DA@Q",
        AlarmType::Value => ".SEVR@Q",
    };
    format!("{}{}", request.device, request_properties)
}

pub async fn fetch_alarms(
    endpoint: String,
    device_list: Vec<AlarmRequest>,
) -> Result<
    impl Stream<Item = Result<DpmData, Box<dyn std::error::Error + Send + Sync>>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let drf_list = device_list.iter().map(build_drf).collect::<Vec<_>>();

    tracing::info!("Starting DAQ read for {} alarm requests", device_list.len());

    tracing::info!("DRFs sent to DAQ: {:?}", drf_list);

    let mut client = DaqClient::connect(endpoint).await?;
    let stream = client
        .read(ReadingList { drf: drf_list })
        .await?
        .into_inner();

    let parsed_stream = stream.map(move |res| {
        tracing::info!("Received reply from DAQ stream");

        match res {
            Ok(reply) => {
                let device = device_list.get(reply.index as usize).unwrap();
                parse_reply(&reply, device)
            }
            Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
        }
    });

    Ok(parsed_stream)
}

pub fn parse_reply(
    reply: &ReadingReply,
    device_request: &AlarmRequest,
) -> Result<DpmData, Box<dyn std::error::Error + Send + Sync>> {
    let reply_index = reply.index;
    match &reply.value {
        Some(reading_reply::Value::Readings(readings)) => {
            tracing::info!(
                "Parsing DAQ reading for device {} (alarm type {:?})",
                device_request.device,
                device_request.alarm_type
            );

            let reading = readings.reading.first().ok_or("No readings in reply")?;
            let raw_timestamp = reading
                .timestamp
                .as_ref()
                .ok_or_else(|| "missing timestamp".to_string())?;
            Ok(DpmData::Reading(DpmReading {
                index: reply_index,
                device: device_request.device.clone(),
                alarm_type: device_request.alarm_type,
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
        Some(reading_reply::Value::Status(status)) => {
            tracing::error!(
                "DAQ status reply for device {}: facility={}, status={}, message={}",
                device_request.device,
                status.facility_code,
                status.status_code,
                status.message
            );

            Ok(DpmData::Status(DpmStatus {
                index: reply_index,
                device: device_request.device.clone(),
                alarm_type: device_request.alarm_type,
                facility_code: status.facility_code as u8,
                status_code: (status.facility_code + status.status_code * 256) as i8,
                message: status.message.clone(),
            }))
        }
        None => Err(Box::new(DaqError {
            error_text: "Empty reply value".to_string(),
        })),
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::proto::{
        common::{
            device::{
                self,
                value::{AnalogAlarm, DigitalAlarm},
            },
            status::Status,
        },
        services::daq::{Reading, Readings},
    };

    #[test]
    fn test_build_analog_drf() {
        let analog_request = AlarmRequest {
            device: "G:DEVICE".to_string(),
            alarm_type: AlarmType::Analog,
        };
        assert_eq!(build_drf(&analog_request), "G:DEVICE.AA@Q");
    }

    #[test]
    fn test_build_digital_drf() {
        let digital_request = AlarmRequest {
            device: "G:DEVICE".to_string(),
            alarm_type: AlarmType::Digital,
        };
        assert_eq!(build_drf(&digital_request), "G:DEVICE.DA@Q");
    }

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
        let request = AlarmRequest {
            device: "M:OUTTMP".to_string(),
            alarm_type: AlarmType::Analog,
        };
        let parsed = parse_reply(&status_reply, &request);
        match parsed {
            Ok(DpmData::Status(status)) => {
                assert_eq!(status.index, 0, "Incorrect index");
                assert_eq!(status.device, "M:OUTTMP".to_string(), "Incorrect Device");
                assert_eq!(status.facility_code, 1, "Incorrect facility code");
                assert_eq!(status.status_code, 1, "Incorrect status code");
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
                    // this field is deprecated
                    #[allow(deprecated)]
                    status: Default::default(),
                }],
            })),
        };

        let request = AlarmRequest {
            device: "test_device".to_string(),
            alarm_type: AlarmType::Analog,
        };

        let parsed_data = DpmData::Reading(DpmReading {
            index: 0,
            device: "test_device".to_string(),
            alarm_type: AlarmType::Analog,
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

        assert_eq!(parse_reply(&analog_reply, &request).unwrap(), parsed_data);
    }

    #[test]
    fn test_digital_alarm_reply_parsed() {
        let now = Utc::now();
        let digital_reply = ReadingReply {
            index: 0,
            value: Some(reading_reply::Value::Readings(Readings {
                reading: vec![Reading {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    data: Some(device::Value {
                        value: Some(device::value::Value::DigAlarm(DigitalAlarm {
                            nominal: 1,
                            mask: 1,
                            alarm_enable: true,
                            alarm_status: true,
                            abort: false,
                            abort_inhibit: false,
                            tries_needed: 10,
                            tries_now: 11,
                        })),
                    }),
                    // this field is deprecated
                    #[allow(deprecated)]
                    status: Default::default(),
                }],
            })),
        };

        let request = AlarmRequest {
            device: "M:OUTTMP".to_string(),
            alarm_type: AlarmType::Digital,
        };

        let parsed_data = DpmData::Reading(DpmReading {
            index: 0,
            device: "M:OUTTMP".to_string(),
            alarm_type: AlarmType::Digital,
            timestamp: now,
            data: value::Value::DigAlarm(DigitalAlarm {
                nominal: 1,
                mask: 1,
                alarm_enable: true,
                alarm_status: true,
                abort: false,
                abort_inhibit: false,
                tries_needed: 10,
                tries_now: 11,
            }),
        });

        assert_eq!(parse_reply(&digital_reply, &request).unwrap(), parsed_data);
    }

    #[test]
    fn test_daq_error_fmt_impl() {
        let err = DaqError {
            error_text: "This is an error".to_string(),
        };
        assert_eq!(format!("{}", err), "DaqError: This is an error");
    }
}
