use crate::proto::services::ioc_alarms::{
    AlarmInfo, AlarmSeverity, DisplayDescription, IocAlarmsResponse, TimeStamp, ValueAlarm,
};

#[derive(Debug, Clone)]
pub struct IocAlarm {
    pub value: i32,
    pub severity: i32,
    pub status: i32,
    pub timestamp_sec: i64,
    pub timestamp_ns: i32,
    pub description: String,
    pub low_alarm: i32,
    pub low_warning: i32,
    pub high_warning: i32,
    pub high_alarm: i32,
}

impl IocAlarm {
    pub fn from_proto(resp: IocAlarmsResponse) -> Option<Self> {
        // alarm_info is a Vec<AlarmInfo>
        let info: &AlarmInfo = resp.alarm_info.first()?;

        let AlarmSeverity { severity, status } =
            info.alarm.as_ref().cloned().unwrap_or(AlarmSeverity {
                severity: 0,
                status: 0,
            });

        let TimeStamp {
            seconds_past_epoch,
            nanoseconds,
        } = info.time_stamp.as_ref().cloned().unwrap_or(TimeStamp {
            seconds_past_epoch: 0,
            nanoseconds: 0,
        });

        let DisplayDescription { description } =
            info.display
                .as_ref()
                .cloned()
                .unwrap_or(DisplayDescription {
                    description: String::new(),
                });

        let ValueAlarm {
            low_alarm_limit,
            low_warning_limit,
            high_warning_limit,
            high_alarm_limit,
        } = info.value_alarm.as_ref().cloned().unwrap_or(ValueAlarm {
            low_alarm_limit: 0,
            low_warning_limit: 0,
            high_warning_limit: 0,
            high_alarm_limit: 0,
        });

        Some(Self {
            value: info.value,
            severity,
            status,
            timestamp_sec: seconds_past_epoch,
            timestamp_ns: nanoseconds,
            description,
            low_alarm: low_alarm_limit,
            low_warning: low_warning_limit,
            high_warning: high_warning_limit,
            high_alarm: high_alarm_limit,
        })
    }
}
