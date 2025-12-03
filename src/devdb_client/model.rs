use crate::proto::services::devdb::{AlarmInfo, DeviceAnalogAlarm, DeviceDigitalAlarm, InfoEntry};

use crate::proto::services::devdb::info_entry;

/// DeviceSummary – summary of getDeviceInfo results

#[derive(Debug, Clone)]
pub struct DeviceSummary {
    pub name: String,
    pub description: String,
    pub has_reading: bool,
    pub has_setting: bool,
    pub control_cmd_count: usize,
    pub status_bit_count: usize,
}

impl DeviceSummary {
    pub fn from_proto(entry: &InfoEntry) -> Option<Self> {
        let name = entry.name.clone();

        match &entry.result {
            Some(info_entry::Result::Device(dev)) => Some(Self {
                name,
                description: dev.description.clone(),
                has_reading: dev.reading.is_some(),
                has_setting: dev.setting.is_some(),
                control_cmd_count: dev.control.as_ref().map(|c| c.cmds.len()).unwrap_or(0),
                status_bit_count: dev.status.as_ref().map(|s| s.bits.len()).unwrap_or(0),
            }),
            _ => None,
        }
    }
}

/// ------------------------------------------------------------
/// AlarmInfoExpanded – structured result for getAllAlarmInfo
/// ------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct AlarmInfoExpanded {
    pub device_index: i32,
    pub property_index: u32,
    pub status_word_hex: Option<String>,
    pub tries_needed: u32,
    pub tries_now: u32,

    pub analog: Option<DeviceAnalogAlarm>,
    pub digital: Option<DeviceDigitalAlarm>,
}

impl AlarmInfoExpanded {
    pub fn from_proto(info: &AlarmInfo) -> Self {
        let blk = info.alarm_block.as_ref();

        let status_hex = blk.map(|b| {
            b.status
                .iter()
                .map(|byte| format!("{:02X}", byte))
                .collect::<String>()
        });

        Self {
            device_index: blk.map(|b| b.di).unwrap_or_default(),
            property_index: blk.map(|b| b.pi).unwrap_or_default(),
            status_word_hex: status_hex,
            tries_needed: blk.map(|b| b.tries_needed).unwrap_or_default(),
            tries_now: blk.map(|b| b.tries_now).unwrap_or_default(),

            analog: info.device_analog_alarm.clone(),
            digital: info.device_digital_alarm.clone(),
        }
    }
}
