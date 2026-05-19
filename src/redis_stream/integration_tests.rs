//! Integration tests for the redis_stream module using RedisTestHarness.
//!
//! These tests exercise [`run_alarm_stream`] end-to-end by injecting a real
//! Redis stream (via [`RedisTestHarness`]) and a fake Kafka publisher (via
//! [`TestPub`](crate::test_utils::TestPub)), verifying that the full
//! Redis → parse → [`AlarmsReporter::report`] pipeline behaves correctly.

use std::time::Duration;

use rust_pubsub_lib::{RedisStreamPublisher, RedisTestHarness};
use tokio::time::timeout;

use super::*;
use crate::report::AlarmsReporter;
use crate::test_utils::TestPub;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn alarm_msg(device: &str, severity: &str, source: &str) -> MapMessage {
    MapMessage::from_fields(HashMap::from([
        ("device".to_string(), device.to_string()),
        ("severity".to_string(), severity.to_string()),
        ("source".to_string(), source.to_string()),
    ]))
}

/// Builds a reporter wrapped in `Arc<Mutex<_>>` backed by a [`TestPub`].
fn make_reporter() -> Arc<Mutex<AlarmsReporter<TestPub>>> {
    Arc::new(Mutex::new(AlarmsReporter::new(TestPub::init())))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A valid alarm published to Redis is parsed and forwarded to the reporter,
/// which in turn caches it — proving the full Redis → parse → report pipeline.
#[tokio::test]
async fn valid_alarm_is_forwarded_to_reporter() {
    let harness = RedisTestHarness::new(None).await;
    let host = harness.get_host();
    let topic = "acorn:alarms-valid".to_string();

    let mut subscriber = RedisStreamSubscriber::new(host.clone(), topic.clone());
    let stream = subscriber.get_stream::<MapMessage>().await.unwrap();
    let reporter = make_reporter();

    let publisher = RedisStreamPublisher::new(host.clone(), topic.clone());
    publisher
        .publish_stream(alarm_msg("M:BEAM", "HIGH", "ANALOG"))
        .await
        .unwrap();

    // Drive the loop for exactly one message.
    timeout(
        Duration::from_secs(5),
        run_alarm_stream(Arc::clone(&reporter), stream.take(1)),
    )
    .await
    .expect("timed out waiting for alarm to be processed")
    .unwrap();

    // The alarm should now be in the reporter's cache, proving the full
    // Redis → parse → reporter.report() pipeline executed successfully.
    let snapshot = reporter.lock().await.get_snapshot();
    assert_eq!(snapshot.len(), 1, "expected exactly one alarm in the cache");
    assert_eq!(
        snapshot[0].device, "M:BEAM",
        "cached alarm should have the correct device name"
    );
}

/// An entry whose `device` field is absent is silently skipped; the reporter
/// cache must remain empty.
#[tokio::test]
async fn entry_without_device_is_skipped() {
    let harness = RedisTestHarness::new(None).await;
    let host = harness.get_host();
    let topic = "acorn:alarms-no-device".to_string();

    let mut subscriber = RedisStreamSubscriber::new(host.clone(), topic.clone());
    let stream = subscriber.get_stream::<MapMessage>().await.unwrap();
    let reporter = make_reporter();

    // Publish a message with no "device" field.
    let publisher = RedisStreamPublisher::new(host.clone(), topic.clone());
    publisher
        .publish_stream(MapMessage::from_fields(HashMap::from([
            ("severity".to_string(), "HIGH".to_string()),
            ("source".to_string(), "ANALOG".to_string()),
        ])))
        .await
        .unwrap();

    timeout(
        Duration::from_secs(5),
        run_alarm_stream(Arc::clone(&reporter), stream.take(1)),
    )
    .await
    .expect("timed out waiting for stream item")
    .unwrap();

    // Nothing should have been cached — the entry was skipped.
    assert!(
        reporter.lock().await.get_snapshot().is_empty(),
        "reporter cache must be empty when device field is missing"
    );
}

/// A stream error item is logged and skipped; the loop continues and processes
/// the next valid message.
#[tokio::test]
async fn stream_error_is_skipped_and_loop_continues() {
    // Build a synthetic stream: Err first, then a valid Ok message.
    let err_item: Result<MapMessage, PubSubError> = Err(PubSubError::default());
    let ok_item: Result<MapMessage, PubSubError> = Ok(alarm_msg("Z:ACLTST", "LOW", "DIGITAL"));

    let stream = tokio_stream::iter(vec![err_item, ok_item]);
    let reporter = make_reporter();

    run_alarm_stream(Arc::clone(&reporter), stream)
        .await
        .unwrap();

    // The valid message after the error must have been cached by the reporter.
    let snapshot = reporter.lock().await.get_snapshot();
    assert_eq!(
        snapshot.len(),
        1,
        "the valid message after the error must still be processed"
    );
    assert_eq!(
        snapshot[0].device, "Z:ACLTST",
        "cached alarm should have the correct device name"
    );
}

/// Full pipeline: multiple alarms published to Redis are all forwarded to the
/// reporter in order.
#[tokio::test]
async fn multiple_alarms_are_all_forwarded() {
    let harness = RedisTestHarness::new(None).await;
    let host = harness.get_host();
    let topic = "acorn:alarms-multi".to_string();

    let mut subscriber = RedisStreamSubscriber::new(host.clone(), topic.clone());
    let stream = subscriber.get_stream::<MapMessage>().await.unwrap();
    let reporter = make_reporter();

    let publisher = RedisStreamPublisher::new(host.clone(), topic.clone());
    publisher
        .publish_stream(alarm_msg("M:BEAM", "HIGH", "ANALOG"))
        .await
        .unwrap();
    publisher
        .publish_stream(alarm_msg("Z:ACLTST", "LOW", "DIGITAL"))
        .await
        .unwrap();

    timeout(
        Duration::from_secs(5),
        run_alarm_stream(Arc::clone(&reporter), stream.take(2)),
    )
    .await
    .expect("timed out waiting for alarms to be processed")
    .unwrap();

    // Both alarms should be in the reporter's cache.
    let snapshot = reporter.lock().await.get_snapshot();
    assert_eq!(snapshot.len(), 2, "both alarms should be cached");

    let devices: Vec<&str> = snapshot.iter().map(|s| s.device.as_str()).collect();
    assert!(devices.contains(&"M:BEAM"), "M:BEAM should be in snapshot");
    assert!(
        devices.contains(&"Z:ACLTST"),
        "Z:ACLTST should be in snapshot"
    );
}
