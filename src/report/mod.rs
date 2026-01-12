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

fn handle_pubsub_err<P>(err: PubSubError) -> Option<P> {
    error!("{:?}", err);
    None
}

fn get_publisher<P: Publisher>(
    host_env_var: &str,
    default_host: &str,
    topic_env_var: &str,
    default_topic: &str,
) -> Option<P> {
    let host = env_var::get(host_env_var).or(String::from(default_host));
    let topic = env_var::get(topic_env_var).or(String::from(default_topic));
    P::new(host, topic).map_or_else(handle_pubsub_err, |publisher| Some(publisher))
}

fn get_pip_publisher<P: Publisher>() -> Option<P> {
    get_publisher(
        PIP_II_KAFKA_HOST,
        DEFAULT_PIP_II_HOST,
        PIP_II_ALARMS_TOPIC,
        DEFAULT_PIP_II_TOPIC,
    )
}

fn get_controls_publisher<P: Publisher>() -> Option<P> {
    get_publisher(
        CONTROLS_KAFKA_HOST,
        DEFAULT_CONTROLS_HOST,
        CONTROLS_ALARMS_TOPIC,
        DEFAULT_CONTROLS_TOPIC,
    )
}

pub struct AlarmsReporter<P: Publisher> {
    controls_publisher: Option<P>,
    known_alarms: HashSet<u32>,
    pip2_publisher: Option<P>,
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
        let controls_published = Self::transmit_with_fallback(
            &mut self.controls_publisher,
            message.clone(),
            get_controls_publisher,
        );
        let pip_published =
            Self::transmit_with_fallback(&mut self.pip2_publisher, message, get_pip_publisher);

        // Breaking it out into temp variables ensures both services are tried.
        // Publishing to PIP would be skipped if the calls to transmit_with_fallback were combined
        // into one statement and the call to controls failed.
        controls_published && pip_published
    }

    fn transmit_with_fallback<F: Fn() -> Option<P>>(
        publisher_opt: &mut Option<P>,
        message: Message,
        fallback: F,
    ) -> bool {
        match publisher_opt {
            Some(publisher) => match publisher.publish(message) {
                Err(err) => {
                    *publisher_opt = handle_pubsub_err(err);
                    false
                }
                _ => true,
            },
            None => match fallback() {
                Some(mut publisher) => match publisher.publish(message) {
                    Ok(()) => {
                        *publisher_opt = Some(publisher);
                        true
                    }
                    Err(_) => false,
                },
                None => false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_pubsub_lib::kafka_impl::KafkaPublisher;

    #[test]
    fn call_alarms_reporter_new_with_kafka_publisher() {
        let result = AlarmsReporter::<KafkaPublisher>::new();
        match result.controls_publisher {
            Some(_) => panic!("A publisher was created where one should not have been possible."),
            _ => (),
        };
        match result.pip2_publisher {
            Some(_) => panic!("A publisher was created where one should not have been possible."),
            _ => (),
        };
    }

    #[derive(Debug)]
    struct TestPub {
        pub latest: Option<Message>,
        throw_err: bool,
    }
    impl TestPub {
        fn init() -> Self {
            Self::new(String::default(), String::default()).unwrap()
        }

        fn init_throwing() -> Self {
            Self {
                latest: None,
                throw_err: true,
            }
        }
    }
    impl Publisher for TestPub {
        fn new(_host: String, _topic: String) -> Result<Self, PubSubError> {
            Ok(Self {
                latest: None,
                throw_err: false,
            })
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
            controls_publisher: Some(TestPub::init()),
            known_alarms: HashSet::new(),
            pip2_publisher: Some(TestPub::init()),
        };

        let cur_time = Utc::now();
        test_reporter.report(0, cur_time, false);
        assert!(
            test_reporter.known_alarms.is_empty(),
            "An index was added to known alarms when it should not have been"
        );

        test_reporter.report(0, cur_time, true);

        assert!(test_reporter.known_alarms.contains(&0));

        let prev_alarm;
        if let Some(val) = test_reporter
            .controls_publisher
            .as_ref()
            .unwrap()
            .latest
            .as_ref()
        {
            assert_eq!(val.key, Some(String::from("state:0")));
            if let Some(pip2_val) = test_reporter
                .pip2_publisher
                .as_ref()
                .unwrap()
                .latest
                .as_ref()
            {
                assert_eq!(pip2_val.key, val.key);
                assert_eq!(pip2_val.value, val.value);
                prev_alarm = Message {
                    key: val.key.clone(),
                    value: val.value.clone(),
                };
            } else {
                panic!(
                    "Expected a message to have been sent to the PIP-II publisher, but none was found"
                )
            }
        } else {
            panic!(
                "Expected a message to have been sent to the Controls publisher, but none was found"
            );
        }

        test_reporter.report(0, Utc::now(), true);

        assert!(test_reporter.known_alarms.contains(&0));

        if let Some(new_val) = test_reporter
            .controls_publisher
            .as_ref()
            .unwrap()
            .latest
            .as_ref()
        {
            assert_eq!(new_val.key, Some(String::from("state:0")));
            if let Some(new_pip2_val) = test_reporter
                .pip2_publisher
                .as_ref()
                .unwrap()
                .latest
                .as_ref()
            {
                assert_eq!(new_pip2_val.key, new_val.key);
                assert_eq!(new_pip2_val.value, new_val.value);

                assert_eq!(prev_alarm.key, new_val.key);
                assert_eq!(prev_alarm.value, new_val.value);
            } else {
                panic!(
                    "Expected a message to have been sent to the PIP-II publisher, but none was found"
                )
            }
        } else {
            panic!(
                "Expected a message to have been sent to the Controls publisher, but none was found"
            );
        }
    }

    #[test]
    fn report_alarm_not_active() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: Some(TestPub::init()),
            known_alarms: HashSet::new(),
            pip2_publisher: Some(TestPub::init()),
        };

        test_reporter.known_alarms.insert(0);

        let cur_time = Utc::now();
        test_reporter.report(0, cur_time, false);
        assert!(
            test_reporter.known_alarms.is_empty(),
            "The test index was not removed from the set of known alarms, when it should have been"
        );
        if let Some(_) = test_reporter.controls_publisher.as_ref().unwrap().latest {
            if let None = test_reporter.pip2_publisher.as_ref().unwrap().latest {
                panic!(
                    "A message regarding the alarm going back into range should have been sent to PIP-II, but was not"
                );
            }
        } else {
            panic!(
                "A message regarding the alarm going back into range should have been sent, but was not"
            );
        }
    }

    #[test]
    fn handles_err_on_pub() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: Some(TestPub::init_throwing()),
            known_alarms: HashSet::new(),
            pip2_publisher: Some(TestPub::init()),
        };

        test_reporter.report(0, Utc::now(), true);
        assert!(!test_reporter.known_alarms.contains(&0));
        if let None = test_reporter.pip2_publisher.as_ref().unwrap().latest {
            panic!("An error to one service should still report to the other");
        }
        if let Some(_) = test_reporter.controls_publisher {
            panic!("Controls publisher was not dropped after error")
        }

        // Make call again. As test_reporter is using type TestPub for its generic fields,
        // the get_publisher function in this module will call the ::new method of TestPub. This means
        // we should see a fresh instance of TestPub spun up on this second call.
        test_reporter.report(0, Utc::now(), true);
        assert!(test_reporter.known_alarms.contains(&0));
    }
    #[test]
    fn handles_subset_of_devices_independently() {
        let mut test_reporter = AlarmsReporter {
            controls_publisher: Some(TestPub::init()),
            known_alarms: HashSet::new(),
            pip2_publisher: Some(TestPub::init()),
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
