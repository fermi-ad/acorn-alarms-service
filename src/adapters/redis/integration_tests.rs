//! Integration tests for the Redis stream adapter using `RedisTestHarness`.
//!
//! These tests exercise `run_alarm_stream` end-to-end by injecting a real
//! Redis stream (via [`RedisTestHarness`]) and verifying that the full
//! Redis → parse → [`DomainInput`] pipeline behaves correctly.

use std::{collections::HashMap, time::Duration};

use rust_pubsub_lib::{Publisher, RedisStreamPublisher, RedisTestHarness};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::{
    engine::messages::DomainInput, metrics::Metrics, test_utils::DEFAULT_TEST_QUEUE_CONFIG,
};

use super::*;

fn alarm_msg(device: &str, severity: &str, source: &str) -> MapMessage {
    MapMessage::from_fields(HashMap::from([
        ("device".to_string(), device.to_string()),
        ("severity".to_string(), severity.to_string()),
        ("source".to_string(), source.to_string()),
    ]))
}

#[tokio::test]
async fn valid_alarm_is_forwarded_to_state_channel() {
    let harness = RedisTestHarness::new(None).await;
    let host = harness.get_host();
    let topic = "acorn:alarms-valid".to_string();

    let mut subscriber = RedisStreamSubscriber::new(host.clone(), topic.clone());
    let stream = subscriber.get_stream::<MapMessage>().await.unwrap();

    let (tx, mut rx) = mpsc::channel(4);

    let publisher = RedisStreamPublisher::new(host.clone(), topic.clone());
    publisher
        .publish_stream(alarm_msg("M:BEAM", "HIGH", "ANALOG"))
        .await
        .unwrap();

    timeout(
        Duration::from_secs(5),
        run_alarm_stream(
            AutomatedIngressHandle::new(tx, Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)),
            stream.take(1),
        ),
    )
    .await
    .expect("timed out waiting for alarm to be processed")
    .unwrap();

    let action = rx
        .try_recv()
        .expect("expected one coordinator message on the channel");
    match action {
        DomainInput::AutomatedUpdate(status) => {
            assert_eq!(status.device, "M:BEAM", "device name must match");
            assert_eq!(status.state(), State::Alarmed);
            assert_eq!(status.severity(), Severity::High);
            assert_eq!(status.source(), Source::Analog);
        }
        _ => panic!("expected automated domain input"),
    }
}

#[tokio::test]
async fn entry_without_device_is_skipped() {
    let harness = RedisTestHarness::new(None).await;
    let host = harness.get_host();
    let topic = "acorn:alarms-no-device".to_string();

    let mut subscriber = RedisStreamSubscriber::new(host.clone(), topic.clone());
    let stream = subscriber.get_stream::<MapMessage>().await.unwrap();

    let (tx, mut rx) = mpsc::channel(4);

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
        run_alarm_stream(
            AutomatedIngressHandle::new(tx, Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)),
            stream.take(1),
        ),
    )
    .await
    .expect("timed out waiting for stream item")
    .unwrap();

    assert!(
        rx.try_recv().is_err(),
        "channel must be empty when device field is missing"
    );
}

#[tokio::test]
async fn stream_error_is_skipped_and_loop_continues() {
    let err_item: Result<MapMessage, PubSubError> = Err(PubSubError::default());
    let ok_item: Result<MapMessage, PubSubError> = Ok(alarm_msg("Z:ACLTST", "LOW", "DIGITAL"));

    let stream = tokio_stream::iter(vec![err_item, ok_item]);
    let (tx, mut rx) = mpsc::channel(4);

    run_alarm_stream(
        AutomatedIngressHandle::new(tx, Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)),
        stream,
    )
    .await
    .unwrap();

    let action = rx
        .try_recv()
        .expect("expected one coordinator message after the stream error");
    match action {
        DomainInput::AutomatedUpdate(status) => {
            assert_eq!(
                status.device, "Z:ACLTST",
                "device name must match the valid message"
            );
            assert_eq!(status.state(), State::Alarmed);
            assert_eq!(status.severity(), Severity::Low);
            assert_eq!(status.source(), Source::Digital);
        }
        _ => panic!("expected automated domain input"),
    }

    assert!(
        rx.try_recv().is_err(),
        "only one message should have been forwarded"
    );
}

#[tokio::test]
async fn multiple_alarms_are_all_forwarded() {
    let harness = RedisTestHarness::new(None).await;
    let host = harness.get_host();
    let topic = "acorn:alarms-multi".to_string();

    let mut subscriber = RedisStreamSubscriber::new(host.clone(), topic.clone());
    let stream = subscriber.get_stream::<MapMessage>().await.unwrap();

    let (tx, mut rx) = mpsc::channel(4);

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
        Duration::from_secs(10),
        run_alarm_stream(
            AutomatedIngressHandle::new(tx, Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)),
            stream.take(2),
        ),
    )
    .await
    .expect("timed out waiting for alarms to be processed")
    .unwrap();

    let mut statuses = Vec::new();
    while let Ok(action) = rx.try_recv() {
        match action {
            DomainInput::AutomatedUpdate(status) => statuses.push(status),
            _ => panic!("expected only automated domain inputs"),
        }
    }

    assert_eq!(statuses.len(), 2, "both alarms should be forwarded");
    assert!(
        statuses.iter().any(|status| {
            status.device == "M:BEAM"
                && status.state() == State::Alarmed
                && status.severity() == Severity::High
                && status.source() == Source::Analog
        }),
        "M:BEAM analog high alarm must be translated into the domain input"
    );
    assert!(
        statuses.iter().any(|status| {
            status.device == "Z:ACLTST"
                && status.state() == State::Alarmed
                && status.severity() == Severity::Low
                && status.source() == Source::Digital
        }),
        "Z:ACLTST digital low alarm must be translated into the domain input"
    );
}

#[tokio::test]
async fn no_alarm_is_translated_into_ok_state() {
    let stream = tokio_stream::iter(vec![Ok(alarm_msg("M:BEAM", "NO_ALARM", "EPICS"))]);
    let (tx, mut rx) = mpsc::channel(4);

    run_alarm_stream(
        AutomatedIngressHandle::new(tx, Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)),
        stream,
    )
    .await
    .unwrap();

    let action = rx
        .try_recv()
        .expect("expected translated coordinator message for NO_ALARM");
    match action {
        DomainInput::AutomatedUpdate(status) => {
            assert_eq!(status.device, "M:BEAM");
            assert_eq!(status.state(), State::Ok);
            assert_eq!(status.severity(), Severity::Unknown);
            assert_eq!(status.source(), Source::Epics);
        }
        _ => panic!("expected automated domain input"),
    }
}

#[tokio::test]
async fn dropped_receiver_does_not_panic() {
    let ok_item: Result<MapMessage, PubSubError> = Ok(alarm_msg("M:BEAM", "HIGH", "ANALOG"));
    let stream = tokio_stream::iter(vec![ok_item]);

    let (tx, rx) = mpsc::channel(1);
    drop(rx);

    let result = run_alarm_stream(
        AutomatedIngressHandle::new(tx, Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)),
        stream,
    )
    .await;
    assert!(
        result.is_ok(),
        "run_alarm_stream must return Ok even when receiver is dropped"
    );
}
