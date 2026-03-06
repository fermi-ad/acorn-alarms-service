use crate::proto::common::alarm::{
    Status,
    status::{Severity, Source, State},
};
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{Message, Publisher};
use std::collections::HashMap;
use tracing::error;

const CONTROLS_KAFKA_HOST: &str = "CONTROLS_KAFKA_HOST";
const DEFAULT_CONTROLS_HOST: &str = "kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092";

const CONTROLS_ALARMS_TOPIC: &str = "CONTROLS_ALARMS_TOPIC";
const DEFAULT_CONTROLS_TOPIC: &str = "alarms";

fn get_publisher<P: Publisher>() -> P {
    let host = env_var::get(CONTROLS_KAFKA_HOST).or_else(|| DEFAULT_CONTROLS_HOST.to_string());

    let topic = env_var::get(CONTROLS_ALARMS_TOPIC).or_else(|| DEFAULT_CONTROLS_TOPIC.to_string());

    P::new(host, topic)
}

fn alarm_to_message(status: &Status, message_body: String) -> Message {
    Message {
        key: Some(format!("{}#{:?}", status.device, status.source())),
        value: message_body,
    }
}

pub struct AlarmsReporter<P: Publisher> {
    controls_publisher: P,
    known_alarms: HashMap<Source, HashMap<String, (State, Severity)>>,
}

impl<P: Publisher> AlarmsReporter<P> {
    pub fn new() -> Self {
        Self {
            controls_publisher: get_publisher(),
            known_alarms: HashMap::new(),
        }
    }

    fn transition_allowed(prev: State, next: State) -> bool {
        matches!(
            (prev, next),
            (State::Ok, State::Alarmed)
                | (State::Alarmed, State::Acknowledged)
                | (State::Alarmed, State::Ok)
                | (State::Acknowledged, State::Ok)
        )
    }

    fn should_publish(&self, alarm: &Status) -> bool {
        let prev = self
            .known_alarms
            .get(&alarm.source())
            .and_then(|devices| devices.get(&alarm.device));

        let changed = match prev {
            None => true,
            Some((state, severity)) => {
                Self::transition_allowed(*state, alarm.state()) || *severity != alarm.severity()
            }
        };

        if !changed {
            tracing::debug!(
                target = "alarm_transition",
                device = %alarm.device,
                source = ?alarm.source(),
                "Ignoring invalid or duplicate alarm transition"
            );
        }

        if changed {
            tracing::debug!(
                target = "alarm_transition",
                device = %alarm.device,
                source = ?alarm.source(),
                previous = ?prev,
                current = ?(alarm.state(), alarm.severity()),
                "Alarm state transition detected"
            );
        }

        changed
    }
    pub fn report(&mut self, alarm: Status) {
        let cur_state = alarm.state();
        let cur_severity = alarm.severity();

        if self.should_publish(&alarm) {
            let message_body = match serde_json::to_string(&alarm) {
                Ok(body) => body,
                Err(err) => {
                    error!(
                        "Failed to serialize alarm object for {}#{:?}\n{}",
                        alarm.device,
                        alarm.source(),
                        err
                    );
                    return;
                }
            };

            tracing::debug!(target = "kafka", payload = %message_body, "Kafka payload");

            let message: Message = alarm_to_message(&alarm, message_body);

            if self.handle_publish(message) {
                if cur_state == State::Ok {
                    if let Some(devices) = self.known_alarms.get_mut(&alarm.source()) {
                        devices.remove(&alarm.device);
                    }
                } else {
                    let devices = self.known_alarms.entry(alarm.source()).or_default();
                    devices.insert(alarm.device, (cur_state, cur_severity));
                }
            }
        }
    }

    fn handle_publish(&mut self, message: Message) -> bool {
        let key = message.key.clone(); 

        match self.controls_publisher.publish(message) {
            Ok(_) => {
                tracing::debug!(
                    target = "kafka",
                    key = ?key,
                    "Published alarm to Kafka"
                );
                true
            }
            Err(err) => {
                tracing::error!(
                    target = "kafka",
                    error = ?err,
                    key = ?key,
                    "Kafka publish failed"
                );
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

        let mut analog_alarms: HashMap<String, (State, Severity)> = HashMap::new();
        analog_alarms.insert("test device".to_string(), (State::Alarmed, Severity::Low));

        test_reporter
            .known_alarms
            .insert(Source::Analog, analog_alarms);

        test_reporter.report(get_test_alarm("device 2", State::Ok, Source::Analog));

        assert!(
            test_reporter
                .known_alarms
                .get(&Source::Analog)
                .is_none_or(|devices| !devices.contains_key("device 2"))
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
                .get(&Source::Analog)
                .unwrap()
                .contains_key("device 2")
        );
        assert!(
            test_reporter
                .known_alarms
                .get(&Source::Epics)
                .unwrap()
                .contains_key("device 7")
        );
        assert_eq!(test_reporter.known_alarms.len(), 2);

        // Clear alarm for only one device
        test_reporter.report(get_test_alarm("device 2", State::Ok, Source::Analog));

        assert!(
            test_reporter
                .known_alarms
                .get(&Source::Analog)
                .is_none_or(|devices| !devices.contains_key("device 2"))
        );
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&Source::Epics)
                .unwrap()
                .get("device 7"),
            Some(&(State::Alarmed, Severity::Low))
        );
        assert_eq!(test_reporter.known_alarms.len(), 2);
    }

    #[test]
    fn report_new_alarm() {
        let mut test_reporter = AlarmsReporter::<TestPub>::new();

        let source = Source::Analog;
        test_reporter.report(get_test_alarm("test device", State::Ok, source));

        assert!(!test_reporter.known_alarms.contains_key(&source));
        test_reporter.report(get_test_alarm("test device", State::Alarmed, source));
        assert_eq!(
            test_reporter
                .known_alarms
                .get(&source)
                .unwrap()
                .get("test device"),
            Some(&(State::Alarmed, Severity::Low))
        );
    }

    #[test]
    fn report_same_alarm_does_not_throw_err() {
        let mut test_reporter = AlarmsReporter::<TestPub>::new();

        let source = Source::Analog;
        test_reporter.report(get_test_alarm("test device", State::Ok, source));
        assert!(!test_reporter.known_alarms.contains_key(&source));
    }

    #[test]
    fn test_should_publish_logic() {
        let reporter = AlarmsReporter::<TestPub>::new();

        // New alarm should publish
        let alarm = get_test_alarm("dev1", State::Ok, Source::Analog);
        assert!(reporter.should_publish(&alarm));

        // Simulate stored previous state
        let mut reporter = AlarmsReporter::<TestPub>::new();
        reporter
            .known_alarms
            .entry(Source::Analog)
            .or_default()
            .insert("dev1".to_string(), (State::Ok, Severity::Low));

        let same_alarm = get_test_alarm("dev1", State::Ok, Source::Analog);
        assert!(!reporter.should_publish(&same_alarm));

        let state_change = get_test_alarm("dev1", State::Alarmed, Source::Analog);
        assert!(reporter.should_publish(&state_change));
    }
}
