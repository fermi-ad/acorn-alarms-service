use crate::proto::common::alarm::status::Severity;
use crate::proto::common::alarm::{
    Status,
    status::{Source, State},
};
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{Message, Publisher};
use serde::Serialize;
use std::collections::HashMap;
use tracing::error;

const CONTROLS_KAFKA_HOST: &str = "CONTROLS_KAFKA_HOST";
const DEFAULT_CONTROLS_HOST: &str = "kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092";

const CONTROLS_ALARMS_TOPIC: &str = "CONTROLS_ALARMS_TOPIC";
const DEFAULT_CONTROLS_TOPIC: &str = "alarms";

fn get_publisher<P: Publisher>() -> P {
    let host = env_var::get(CONTROLS_KAFKA_HOST).or_else(|| String::from(DEFAULT_CONTROLS_HOST));
    let topic =
        env_var::get(CONTROLS_ALARMS_TOPIC).or_else(|| String::from(DEFAULT_CONTROLS_TOPIC));

    P::new(host, topic)
}

fn alarm_to_message(status: &Status, message_body: String) -> Message {
    Message {
        key: Some(format!("{}#{}", status.device, map_source(status.source()))),
        value: message_body,
    }
}

#[derive(Serialize)]
struct KafkaAlarmPayload {
    #[serde(rename = "Device")]
    device: String,

    #[serde(rename = "Source")]
    source: String,

    #[serde(rename = "State")]
    state: String,

    #[serde(rename = "Severity")]
    severity: String,

    #[serde(rename = "Acknowledgeable")]
    acknowledgeable: String,

    #[serde(rename = "Time")]
    time: Option<TimestampPayload>,

    #[serde(rename = "Detail")]
    detail: Option<u32>,

    #[serde(rename = "User")]
    user: Option<String>,

    #[serde(rename = "Wake")]
    wake: Option<TimestampPayload>,
}

#[derive(Serialize)]
struct TimestampPayload {
    seconds: i64,
    nanos: i32,
}
fn map_source(source: Source) -> String {
    match source {
        Source::Analog => "Analog",
        Source::Digital => "Digital",
        Source::Epics => "Epics",
        _ => "Unknown",
    }
    .to_string()
}

fn map_state(state: State) -> String {
    match state {
        State::Ok => "Ok",
        State::Alarmed => "Alarmed",
        State::Bypassed => "Bypassed",
        State::Latched => "Latched",
        State::Acknowledged => "Acknowledged",
        _ => "Unknown",
    }
    .to_string()
}

fn map_severity(sev: Severity) -> String {
    match sev {
        Severity::Low => "Low",
        Severity::High => "High",
        _ => "Unknown",
    }
    .to_string()
}

fn build_kafka_payload(status: &Status) -> KafkaAlarmPayload {
    KafkaAlarmPayload {
        device: status.device.clone(),
        source: map_source(status.source()),
        state: map_state(status.state()),
        severity: map_severity(status.severity()),
        acknowledgeable: status.acknowledgeable.to_string(),

        time: status.time.as_ref().map(|t| TimestampPayload {
            seconds: t.seconds,
            nanos: t.nanos,
        }),

        detail: None,

        user: if status.user.is_empty() {
            None
        } else {
            Some(status.user.clone())
        },

        wake: status.wake.as_ref().map(|t| TimestampPayload {
            seconds: t.seconds,
            nanos: t.nanos,
        }),
    }
}
pub struct AlarmsReporter<P: Publisher> {
    controls_publisher: P,
    known_alarms: HashMap<Source, HashMap<String, State>>,
}
impl<P: Publisher> AlarmsReporter<P> {
    pub fn new() -> Self {
        Self {
            controls_publisher: get_publisher(),
            known_alarms: HashMap::new(),
        }
    }

    pub fn report(&mut self, alarm: Status) {
        let payload = build_kafka_payload(&alarm);
        let serialized = serde_json::to_string(&payload);
        if let Err(err) = serialized {
            error!(
                "Failed to serialize alarm object for {}:{:?}\n{}",
                alarm.device,
                alarm.source(),
                err
            );
            return;
        }

        let message_body = serialized.unwrap();

        tracing::info!(target = "kafka", payload = %message_body, "Kafka payload");
        let cur_state = alarm.state();
        let devices_opt = self.known_alarms.get(&alarm.source());

        if devices_opt.is_none_or(|devices| {
            devices
                .get(&alarm.device)
                .is_none_or(|state| cur_state != *state)
        }) {
            let message = alarm_to_message(&alarm, message_body);
            if self.handle_publish(message) {
                let devices = self.known_alarms.entry(alarm.source()).or_default();
                devices.insert(alarm.device, cur_state);
            }
        }
    }

    fn handle_publish(&mut self, message: Message) -> bool {
        match self.controls_publisher.publish(message) {
            Ok(_) => {
                tracing::info!(target = "kafka", "Published alarm to Kafka");
                true
            }
            Err(err) => {
                error!("{err:?}");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{common::alarm::status::Severity, google::protobuf::Timestamp};
    use rust_pubsub_lib::{PubSubError, kafka_impl::KafkaPublisher};

    #[derive(Debug)]
    struct TestPub {
        pub latest: Option<Message>,
        throw_err: bool,
    }
    impl TestPub {
        fn init_throwing() -> Self {
            Self {
                latest: None,
                throw_err: true,
            }
        }
    }
    impl Publisher for TestPub {
        fn new(_host: String, _topic: String) -> Self {
            Self {
                latest: None,
                throw_err: false,
            }
        }

        fn publish(&mut self, message: Message) -> Result<(), PubSubError> {
            if self.throw_err {
                return Err(PubSubError::default());
            }

            self.latest = Some(message);
            Ok(())
        }
    }

    fn get_test_alarm(device: &str, state: State, source: Source) -> Status {
        Status {
            device: String::from(device),
            state: state as i32,
            severity: Severity::Low as i32,
            acknowledgeable: false,
            time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            epics_type: String::default(),
            user: String::default(),
            wake: None,
            source: source as i32,
        }
    }

    #[test]
    fn call_alarms_reporter_new_with_kafka_publisher() {
        let result = AlarmsReporter::<KafkaPublisher>::new();
        assert_eq!(HashMap::new(), result.known_alarms);
    }

    #[test]
    fn report_alarm_not_active() {
        let mut test_reporter = AlarmsReporter::<TestPub>::new();

        let mut analog_alarms = HashMap::new();
        analog_alarms.insert("test device".to_string(), State::Alarmed);
        test_reporter
            .known_alarms
            .insert(Source::Analog, analog_alarms);

        test_reporter.report(get_test_alarm("test device", State::Ok, Source::Analog));
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(Source::Analog))
                .unwrap()
                .get("test device"),
            Some(&(State::Ok))
        );
        assert!(test_reporter.controls_publisher.latest.is_some());
    }

    #[test]
    fn handles_err_on_pub() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: TestPub::init_throwing(),
            known_alarms: HashMap::new(),
        };

        test_reporter.report(get_test_alarm(
            "test device",
            State::Alarmed,
            Source::Digital,
        ));
        assert!(test_reporter.known_alarms.is_empty());
        assert!(test_reporter.controls_publisher.latest.is_none());
    }

    #[test]
    fn handles_subset_of_devices_independently() {
        let mut test_reporter = AlarmsReporter::<TestPub>::new();

        // Raise alarms for a subset of non contiguous devices
        test_reporter.report(get_test_alarm("device 2", State::Alarmed, Source::Analog));
        test_reporter.report(get_test_alarm("device 7", State::Alarmed, Source::Epics));

        assert!(
            test_reporter
                .known_alarms
                .get(&(Source::Analog))
                .unwrap()
                .contains_key("device 2")
        );
        assert!(
            test_reporter
                .known_alarms
                .get(&(Source::Epics))
                .unwrap()
                .contains_key("device 7")
        );
        assert_eq!(test_reporter.known_alarms.len(), 2);

        // Clear alarm for only one device
        test_reporter.report(get_test_alarm("device 2", State::Ok, Source::Analog));

        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(Source::Analog))
                .unwrap()
                .get("device 2"),
            Some(&(State::Ok))
        );
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(Source::Epics))
                .unwrap()
                .get("device 7"),
            Some(&(State::Alarmed))
        );
        assert_eq!(test_reporter.known_alarms.len(), 2);
    }

    #[test]
    fn report_new_alarm() {
        let mut test_reporter = AlarmsReporter::<TestPub>::new();

        let source = Source::Analog;
        test_reporter.report(get_test_alarm("test device", State::Ok, source));
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(source))
                .unwrap()
                .get("test device"),
            Some(&(State::Ok))
        );
        test_reporter.report(get_test_alarm("test device", State::Alarmed, source));
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(source))
                .unwrap()
                .get("test device"),
            Some(&(State::Alarmed))
        );
    }

    #[test]
    fn report_same_alarm_does_not_throw_err() {
        let mut test_reporter = AlarmsReporter::<TestPub>::new();

        let source = Source::Analog;
        test_reporter.report(get_test_alarm("test device", State::Ok, source));
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(source))
                .unwrap()
                .get("test device"),
            Some(&(State::Ok))
        );
        test_reporter.report(get_test_alarm("test device", State::Ok, source));
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&(source))
                .unwrap()
                .get("test device"),
            Some(&(State::Ok))
        );
    }
}
