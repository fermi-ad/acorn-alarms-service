// src/devdb_client/model.rs

use crate::proto::services::devdb::{
    info_entry, AlarmInfo, DeviceAnalogAlarm, DeviceDigitalAlarm, DeviceInfo,
};

/// DeviceSummary

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
    pub fn from_proto(entry: &crate::proto::services::devdb::InfoEntry) -> Option<Self> {
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

/// AlarmInfoExpanded

#[derive(Debug, Clone)]
pub struct AlarmInfoExpanded {
    pub device_index: i32,
    pub property_index: u32,
    pub tries_needed: u32,
    pub tries_now: u32,
    pub analog: DeviceAnalogAlarm,
    pub digital: DeviceDigitalAlarm,
}

impl AlarmInfoExpanded {
    pub fn from_proto(info: &AlarmInfo) -> Self {
        Self {
            device_index: info.alarm_block.di,
            property_index: info.alarm_block.pi,
            tries_needed: info.alarm_block.tries_needed,
            tries_now: info.alarm_block.tries_now,
            analog: info.device_analog_alarm.clone(),
            digital: info.device_digital_alarm.clone(),
        }
    }
}
