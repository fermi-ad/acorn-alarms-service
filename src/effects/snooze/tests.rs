//! Tests for the snooze scheduler.

use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};

use super::*;
use crate::{
    metrics::Metrics,
    model::{
        key::Key,
        snooze::{Snooze, SnoozeOutcome},
    },
    proto::google::protobuf::Timestamp,
    test_utils::DEFAULT_TEST_QUEUE_CONFIG,
};

const CHAN_CAP: usize = 8;
const TIMEOUT_SECS: u64 = 2;

fn make_key(s: &str) -> Key {
    Key::try_from(s).unwrap()
}

/// Returns a timestamp ~60 seconds in the future — well within tokio's DelayQueue range.
fn future_wake() -> Timestamp {
    let soon = chrono::Utc::now() + chrono::Duration::seconds(60);
    Timestamp {
        seconds: soon.timestamp(),
        nanos: 0,
    }
}

/// Returns a timestamp that `DateTime::from_timestamp_secs` cannot represent (out-of-range),
/// causing `timestamp_secs_to_instant` to return `None` → `SnoozeOutcome::InvalidWake`.
fn invalid_wake() -> Timestamp {
    Timestamp {
        seconds: i64::MIN,
        nanos: 0,
    }
}

fn make_port() -> (
    mpsc::Sender<Snooze>,
    mpsc::Receiver<SnoozeOutcome>,
    SnoozeEffectPort,
) {
    let (snooze_tx, snooze_rx) = mpsc::channel(CHAN_CAP);
    let (snooze_outcome_tx, snooze_outcome_rx) = mpsc::channel(CHAN_CAP);
    let port = SnoozeEffectPort {
        snooze_rx,
        snooze_outcome_tx,
    };
    (snooze_tx, snooze_outcome_rx, port)
}

fn test_metrics() -> Metrics {
    Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG)
}

#[tokio::test]
async fn set_snooze_returns_accepted() {
    let (snooze_tx, mut outcome_rx, port) = make_port();
    tokio::spawn(run_snooze_scheduler(port, test_metrics()));

    let key = make_key("M:BEAM#Analog");
    snooze_tx
        .send(Snooze::Set {
            key: key.clone(),
            wake: future_wake(),
        })
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for snooze outcome")
        .expect("outcome channel closed");

    assert!(
        matches!(outcome, SnoozeOutcome::Accepted { key: k } if k == key),
        "expected Accepted outcome for a future wake timestamp"
    );
}

#[tokio::test]
async fn cancel_snooze_returns_accepted() {
    let (snooze_tx, mut outcome_rx, port) = make_port();
    tokio::spawn(run_snooze_scheduler(port, test_metrics()));

    let key = make_key("M:BEAM#Analog");
    snooze_tx
        .send(Snooze::Cancel { key: key.clone() })
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for snooze outcome")
        .expect("outcome channel closed");

    assert!(
        matches!(outcome, SnoozeOutcome::Accepted { key: k } if k == key),
        "expected Accepted outcome for Cancel"
    );
}

#[tokio::test]
async fn set_snooze_with_past_timestamp_returns_invalid_wake() {
    let (snooze_tx, mut outcome_rx, port) = make_port();
    tokio::spawn(run_snooze_scheduler(port, test_metrics()));

    let key = make_key("M:BEAM#Analog");
    snooze_tx
        .send(Snooze::Set {
            key: key.clone(),
            wake: invalid_wake(),
        })
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for snooze outcome")
        .expect("outcome channel closed");

    assert!(
        matches!(outcome, SnoozeOutcome::InvalidWake { key: k } if k == key),
        "expected InvalidWake for an out-of-range timestamp"
    );
}

#[tokio::test]
async fn set_snooze_replaces_existing_timer() {
    let (snooze_tx, mut outcome_rx, port) = make_port();
    tokio::spawn(run_snooze_scheduler(port, test_metrics()));

    let key = make_key("M:BEAM#Analog");

    let first_wake = {
        let t = chrono::Utc::now() + chrono::Duration::seconds(120);
        Timestamp {
            seconds: t.timestamp(),
            nanos: 0,
        }
    };

    // First set
    snooze_tx
        .send(Snooze::Set {
            key: key.clone(),
            wake: first_wake,
        })
        .await
        .unwrap();

    let first = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for first Accepted")
        .expect("outcome channel closed");
    assert!(matches!(first, SnoozeOutcome::Accepted { .. }));

    // Second set for same key — replaces the first timer
    snooze_tx
        .send(Snooze::Set {
            key: key.clone(),
            wake: future_wake(),
        })
        .await
        .unwrap();

    let second = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for second Accepted")
        .expect("outcome channel closed");
    assert!(matches!(second, SnoozeOutcome::Accepted { .. }));

    // No expiry should fire immediately — the timer is far in the future
    let no_expiry = tokio::time::timeout(Duration::from_millis(50), outcome_rx.recv()).await;
    assert!(
        no_expiry.is_err(),
        "no expiry should fire immediately after replacing the timer with a far-future wake"
    );
}

#[tokio::test]
async fn cancel_nonexistent_snooze_returns_accepted() {
    let (snooze_tx, mut outcome_rx, port) = make_port();
    tokio::spawn(run_snooze_scheduler(port, test_metrics()));

    let key = make_key("NEVER:SET#Analog");
    snooze_tx
        .send(Snooze::Cancel { key: key.clone() })
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for snooze outcome")
        .expect("outcome channel closed");

    assert!(
        matches!(outcome, SnoozeOutcome::Accepted { key: k } if k == key),
        "cancelling a non-existent snooze must return Accepted without panicking"
    );
}

#[tokio::test]
async fn expired_snooze_sends_expired_outcome() {
    let (snooze_tx, mut outcome_rx, port) = make_port();
    tokio::spawn(run_snooze_scheduler(port, test_metrics()));

    let key = make_key("M:BEAM#Analog");

    // Use a timestamp 1 ms in the future so the timer fires almost immediately
    let soon = {
        let target = chrono::Utc::now() + chrono::Duration::seconds(1);
        Timestamp {
            seconds: target.timestamp(),
            nanos: 0,
        }
    };

    snooze_tx
        .send(Snooze::Set {
            key: key.clone(),
            wake: soon,
        })
        .await
        .unwrap();

    // Consume the Accepted outcome
    let accepted = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for Accepted")
        .expect("outcome channel closed");
    assert!(matches!(accepted, SnoozeOutcome::Accepted { .. }));

    // Wait for the expiry
    let expired = timeout(Duration::from_secs(TIMEOUT_SECS), outcome_rx.recv())
        .await
        .expect("timed out waiting for Expired outcome")
        .expect("outcome channel closed");

    assert!(
        matches!(expired, SnoozeOutcome::Expired { key: k } if k == key),
        "expected Expired outcome after the timer fires"
    );
}
