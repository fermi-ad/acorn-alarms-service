//! Integration tests for AlarmsReporter using KafkaTestHarness.
//!
//! These tests verify that AlarmsReporter correctly publishes alarm state
//! changes to Kafka by consuming from the same mock cluster.

use std::time::Duration;

use rust_pubsub_lib::{KafkaPublisher, KafkaSubscriber, KafkaTestHarness, Subscriber};
use tokio::time::timeout;
use tokio_stream::StreamExt;

use super::*;

fn make_alarm(device: &str, state: State, source: Source) -> Status {
    Status {
        device: device.to_string(),
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

#[tokio::test]
async fn report_alarmed_publishes_serialized_alarm_to_kafka() {
    let (harness, topic) = KafkaTestHarness::with_new_topic("alarms-integration").await;
    let host = harness.host().await;

    let mut reporter = AlarmsReporter::new(KafkaPublisher::new(host.clone(), topic.clone()));
    let mut subscriber = KafkaSubscriber::new(host, topic);
    let mut stream = subscriber.get_stream::<StringMessage>().await.unwrap();

    let alarm = make_alarm("M:BEAM", State::Alarmed, Source::Analog);
    reporter.report(alarm).await;

    let received = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for Kafka message")
        .unwrap()
        .unwrap();

    // The published payload should be a JSON-serialized Status containing the device name
    assert!(received.value_ref().contains("M:BEAM"));
    // The message key should be in DEVICE#Source format
    assert_eq!(Some("M:BEAM#Analog"), received.key_ref());
}

#[tokio::test]
async fn report_ok_state_does_not_publish_without_prior_alarm() {
    let (harness, topic) = KafkaTestHarness::with_new_topic("alarms-ok-no-prior").await;
    let host = harness.host().await;

    let mut reporter = AlarmsReporter::new(KafkaPublisher::new(host.clone(), topic.clone()));
    let mut subscriber = KafkaSubscriber::new(host, topic);
    let mut stream = subscriber.get_stream::<StringMessage>().await.unwrap();

    // Reporting Ok with no prior alarm should not publish (duplicate suppression)
    let alarm = make_alarm("M:BEAM", State::Ok, Source::Analog);
    reporter.report(alarm).await;

    // No message should arrive within a short window
    let result = timeout(Duration::from_millis(500), stream.next()).await;

    assert!(
        result.is_err(),
        "Expected no Kafka message for Ok state with no prior alarm"
    );
}
