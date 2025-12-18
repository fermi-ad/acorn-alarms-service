use std::fmt;

use chrono::{DateTime, Utc};
use rust_pubsub_lib::Message;

#[derive(Debug)]
pub enum PhoebusSeverity {
    // TODO Calculate the true severity of an alarm. Allowing these unused values for now.
    #[allow(dead_code)]
    Unknown,
    Ok,
    #[allow(dead_code)]
    Minor,
    Major,
}
impl fmt::Display for PhoebusSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct PhoebusAlarmState {
    pub device: String,
    pub severity: PhoebusSeverity,
    pub latch: Option<bool>,
    pub message: Option<String>,
    pub value: Option<String>,
    pub time: Option<DateTime<Utc>>,
    pub current_severity: Option<PhoebusSeverity>,
    pub current_message: Option<String>,
}
impl PhoebusAlarmState {
    fn check_and_set<T: fmt::Display>(field: &str, val: Option<T>) -> String {
        match val {
            Some(content) => format!(", \"{}\": \"{}\"", field, content),
            None => String::default(),
        }
    }

    pub fn into_message(self) -> Message {
        let mut contents = format!("\"severity\": \"{}\"", self.severity);
        contents += &Self::check_and_set("latch", self.latch);
        contents += &Self::check_and_set("message", self.message);
        contents += &Self::check_and_set("value", self.value);
        contents += &Self::check_and_set("time", self.time);
        contents += &Self::check_and_set("current_severity", self.current_severity);
        contents += &Self::check_and_set("current_message", self.current_message);

        Message::new(
            Some(format!("state:{}", self.device)),
            format!("{{ {} }}", contents),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_full_print() {
        let time = Utc::now();
        let str_time = format!("{}", time.clone());
        let state = PhoebusAlarmState {
            device: String::from("test"),
            severity: PhoebusSeverity::Major,
            latch: Some(true),
            message: Some(String::from("message")),
            value: Some(String::from("123")),
            time: Some(time),
            current_message: Some(String::from("cur msg")),
            current_severity: Some(PhoebusSeverity::Minor),
        };
        let message = state.into_message();
        assert_eq!(message.key, Some(String::from("state:test")));
        assert_eq!(
            message.value,
            String::from(
                "{ \"severity\": \"Major\", \"latch\": \"true\", \"message\": \"message\", \"value\": \"123\", \"time\": \""
            ) + str_time.as_str()
                + "\", \"current_severity\": \"Minor\", \"current_message\": \"cur msg\" }"
        );
    }

    #[test]
    fn check_partial_print() {
        let state = PhoebusAlarmState {
            device: String::from("test"),
            severity: PhoebusSeverity::Unknown,
            latch: None,
            message: None,
            value: None,
            time: None,
            current_message: None,
            current_severity: None,
        };
        let message = state.into_message();
        assert_eq!(message.key, Some(String::from("state:test")));
        assert_eq!(message.value, String::from("{ \"severity\": \"Unknown\" }"));
    }
}
