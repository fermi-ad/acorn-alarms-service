use std::collections::HashSet;

use chrono::{DateTime, Utc};
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{Message, PubSubError, Publisher};

use crate::epics::{PhoebusAlarmState, PhoebusSeverity};

use tracing::error;

const PIP_II_ALARMS_TOPIC: &str = "PIP_II_ALARMS_TOPIC";
const DEFAULT_PIP_II_TOPIC: &str = "ACsys";

const PIP_II_KAFKA_HOST: &str = "PIP_II_KAFKA_HOST";
const DEFAULT_PIP_II_HOST: &str = "acsys-services.fnal.gov:9092";

const CONTROLS_KAFKA_HOST: &str = "CONTROLS_KAFKA_HOST";
const DEFAULT_CONTROLS_HOST: &str = "kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092";

const CONTROLS_ALARMS_TOPIC: &str = "CONTROLS_ALARMS_TOPIC";
const DEFAULT_CONTROLS_TOPIC: &str = "alarms";

fn handle_publish(result: Result<(), PubSubError>) -> bool {
    match result {
        Ok(_) => true,
        Err(err) => {
            error!("{err:?}");
            false
        }
    }
}

fn get_publisher<P: Publisher>(
    host_env_var: &str,
    default_host: &str,
    topic_env_var: &str,
    default_topic: &str,
) -> P {
    let host = env_var::get(host_env_var).or(String::from(default_host));
    let topic = env_var::get(topic_env_var).or(String::from(default_topic));
    P::new(host, topic)
}

fn get_pip_publisher<P: Publisher>() -> P {
    get_publisher(
        PIP_II_KAFKA_HOST,
        DEFAULT_PIP_II_HOST,
        PIP_II_ALARMS_TOPIC,
        DEFAULT_PIP_II_TOPIC,
    )
}

fn get_controls_publisher<P: Publisher>() -> P {
    get_publisher(
        CONTROLS_KAFKA_HOST,
        DEFAULT_CONTROLS_HOST,
        CONTROLS_ALARMS_TOPIC,
        DEFAULT_CONTROLS_TOPIC,
    )
}

pub struct AlarmsReporter<P: Publisher> {
    controls_publisher: P,
    known_alarms: HashSet<u32>,
    pip2_publisher: P,
}
impl<P: Publisher> AlarmsReporter<P> {
    pub fn new() -> Self {
        let known_alarms = HashSet::new();
        let pip2_publisher = get_pip_publisher();
        let controls_publisher = get_controls_publisher();

        Self {
            controls_publisher,
            known_alarms,
            pip2_publisher,
        }
    }

    pub fn report(&mut self, device_index: u32, timestamp: DateTime<Utc>, active_alarm: bool) {
        let mut message: Option<Message> = None;
        if self.known_alarms.contains(&device_index) {
            if !active_alarm {
                let alarm_state = PhoebusAlarmState {
                    device: format!("{}", device_index),
                    severity: PhoebusSeverity::Ok,
                    message: Some(String::from("Device is no longer in alarm")),
                    latch: None,
                    value: None,
                    time: Some(timestamp),
                    current_severity: None,
                    current_message: None,
                };
                message = Some(alarm_state.into_message());
            }
        } else if active_alarm {
            let alarm_state = PhoebusAlarmState {
                device: format!("{}", device_index),
                severity: PhoebusSeverity::Major,
                message: Some(String::from("Device is in alarm")),
                latch: None,
                value: None,
                time: Some(timestamp),
                current_severity: None,
                current_message: None,
            };
            message = Some(alarm_state.into_message());
        }

        if let Some(msg) = message
            && self.try_publish(msg)
        {
            if self.known_alarms.contains(&device_index) {
                self.known_alarms.remove(&device_index);
            } else {
                self.known_alarms.insert(device_index);
            }
        }
    }

    fn try_publish(&mut self, message: Message) -> bool {
        let controls_published = handle_publish(self.controls_publisher.publish(message.clone()));
        let pip_published = handle_publish(self.pip2_publisher.publish(message));

        // Breaking it out into temp variables ensures both services are tried.
        // Publishing to PIP would be skipped if the calls to transmit_with_fallback were combined
        // into one statement and the call to controls failed.
        controls_published && pip_published
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_pubsub_lib::kafka_impl::KafkaPublisher;

    #[test]
    fn call_alarms_reporter_new_with_kafka_publisher() {
        let result = AlarmsReporter::<KafkaPublisher>::new();
        assert_eq!(HashSet::new(), result.known_alarms);
    }

    #[derive(Debug)]
    struct TestPub {
        pub latest: Option<Message>,
        throw_err: bool,
    }
    impl TestPub {
        fn init() -> Self {
            Self::new(String::default(), String::default())
        }

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

    #[test]
    fn report_new_alarm() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: TestPub::init(),
            known_alarms: HashSet::new(),
            pip2_publisher: TestPub::init(),
        };

        let cur_time = Utc::now();
        test_reporter.report(0, cur_time, false);
        assert!(test_reporter.known_alarms.is_empty());

        test_reporter.report(0, cur_time, true);

        assert!(test_reporter.known_alarms.contains(&0));

        let controls_val = test_reporter.controls_publisher.latest.clone().unwrap();
        assert_eq!(controls_val.key, Some(String::from("state:0")));
        let pip_val = test_reporter.pip2_publisher.latest.clone().unwrap();
        assert_eq!(pip_val.key, controls_val.key);
        assert_eq!(pip_val.value, controls_val.value);

        let prev_alarm = Message {
            key: pip_val.key.clone(),
            value: pip_val.value.clone(),
        };

        test_reporter.report(0, Utc::now(), true);

        assert!(test_reporter.known_alarms.contains(&0));

        let controls_val = test_reporter.controls_publisher.latest.unwrap();
        assert_eq!(controls_val.key, Some(String::from("state:0")));

        let pip_val = test_reporter.pip2_publisher.latest.unwrap();
        assert_eq!(pip_val.key, controls_val.key);
        assert_eq!(pip_val.value, controls_val.value);

        assert_eq!(prev_alarm.key, pip_val.key);
        assert_eq!(prev_alarm.value, pip_val.value);
    }

    #[test]
    fn report_alarm_not_active() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: TestPub::init(),
            known_alarms: HashSet::new(),
            pip2_publisher: TestPub::init(),
        };

        test_reporter.known_alarms.insert(0);

        let cur_time = Utc::now();
        test_reporter.report(0, cur_time, false);
        assert!(test_reporter.known_alarms.is_empty());
        assert!(test_reporter.controls_publisher.latest.is_some());
        assert!(test_reporter.pip2_publisher.latest.is_some());
    }

    #[test]
    fn handles_err_on_pub() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: TestPub::init_throwing(),
            known_alarms: HashSet::new(),
            pip2_publisher: TestPub::init(),
        };

        test_reporter.report(0, Utc::now(), true);
        assert!(!test_reporter.known_alarms.contains(&0));
        assert!(test_reporter.controls_publisher.latest.is_none());
        assert!(test_reporter.pip2_publisher.latest.is_some());

        test_reporter.controls_publisher = TestPub::init();
        test_reporter.report(0, Utc::now(), true);
        assert!(test_reporter.known_alarms.contains(&0));
    }
    #[test]
    fn handles_subset_of_devices_independently() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: TestPub::init(),
            known_alarms: HashSet::new(),
            pip2_publisher: TestPub::init(),
        };

        let cur_time = Utc::now();

        // Raise alarms for a subset of non contiguous devices
        test_reporter.report(2, cur_time, true);
        test_reporter.report(7, cur_time, true);

        assert!(test_reporter.known_alarms.contains(&2));
        assert!(test_reporter.known_alarms.contains(&7));
        assert_eq!(test_reporter.known_alarms.len(), 2);

        // Clear alarm for only one device
        test_reporter.report(2, cur_time, false);

        assert!(!test_reporter.known_alarms.contains(&2));
        assert!(test_reporter.known_alarms.contains(&7));
        assert_eq!(test_reporter.known_alarms.len(), 1);
    }
}
