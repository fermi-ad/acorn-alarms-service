//! Tests for the workflow handler.

use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};

use crate::{
    engine::workflow::{
        CoordinatorWorkflowPort, Job, JobOutcome, PublishWorkflowPort, SnoozeWorkflowPort,
        WorkflowHandler,
    },
    metrics::Metrics,
    model::{
        errors::SymmetricalResult,
        key::Key,
        publish::{Publish, PublishOutcome},
        snooze::{Snooze, SnoozeOutcome},
    },
    proto::{
        common::alarm::{Status, status::State},
        google::protobuf::Timestamp,
    },
    test_utils::{DEFAULT_TEST_QUEUE_CONFIG, make_status},
};

const CHAN_CAP: usize = 16;
const TIMEOUT_SECS: u64 = 2;

type ChannelHandles = (
    mpsc::Sender<Job>,
    mpsc::Receiver<JobOutcome>,
    mpsc::Receiver<Snooze>,
    mpsc::Sender<SnoozeOutcome>,
    mpsc::Receiver<Publish>,
    mpsc::Sender<PublishOutcome>,
);

fn make_key(s: &str) -> Key {
    Key::try_from(s).unwrap()
}

fn make_job(key: Key, status: Status, user_initiated: bool) -> Job {
    Job {
        id: 1,
        key,
        status,
        user_initiated,
    }
}

/// Builds a `WorkflowHandler` and returns all the channel ends needed to drive it from tests.
///
/// Returns:
/// - `job_tx`: send `Job` values into the handler
/// - `job_outcome_rx`: receive `JobOutcome` values from the handler
/// - `snooze_rx`: receive `Snooze` commands dispatched by the handler
/// - `snooze_outcome_tx`: send `SnoozeOutcome` replies back to the handler
/// - `publish_rx`: receive `Publish` commands dispatched by the handler
/// - `publish_outcome_tx`: send `PublishOutcome` replies back to the handler
fn make_handler() -> ChannelHandles {
    let (job_tx, job_rx) = mpsc::channel(CHAN_CAP);
    let (job_outcome_tx, job_outcome_rx) = mpsc::channel(CHAN_CAP);
    let (snooze_tx, snooze_rx) = mpsc::channel(CHAN_CAP);
    let (snooze_outcome_tx, snooze_outcome_rx) = mpsc::channel(CHAN_CAP);
    let (publish_tx, publish_rx) = mpsc::channel(CHAN_CAP);
    let (publish_outcome_tx, publish_outcome_rx) = mpsc::channel(CHAN_CAP);

    let handler = WorkflowHandler::new(
        CoordinatorWorkflowPort {
            job_rx,
            job_outcome_tx,
        },
        PublishWorkflowPort {
            publish_outcome_rx,
            publish_tx,
        },
        SnoozeWorkflowPort {
            snooze_outcome_rx,
            snooze_tx,
        },
        Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG),
    );
    tokio::spawn(handler.start());

    (
        job_tx,
        job_outcome_rx,
        snooze_rx,
        snooze_outcome_tx,
        publish_rx,
        publish_outcome_tx,
    )
}

#[tokio::test]
async fn new_job_dispatches_snooze_first() {
    let (
        job_tx,
        _job_outcome_rx,
        mut snooze_rx,
        _snooze_outcome_tx,
        mut publish_rx,
        _publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let mut status = make_status(
        "M:BEAM",
        State::Bypassed,
        crate::proto::common::alarm::status::Source::Analog,
    );
    status.wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });

    job_tx.send(make_job(key, status, false)).await.unwrap();

    // Snooze must arrive before publish
    let snooze = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out waiting for snooze command")
        .expect("snooze channel closed");

    assert!(
        matches!(snooze, Snooze::Set { .. }),
        "expected Snooze::Set for a Bypassed job with a wake timestamp"
    );

    // Publish channel must still be empty at this point
    assert!(
        publish_rx.try_recv().is_err(),
        "publish must not be dispatched before snooze is acknowledged"
    );
}

#[tokio::test]
async fn snooze_accepted_then_dispatches_publish() {
    let (
        job_tx,
        _job_outcome_rx,
        mut snooze_rx,
        snooze_outcome_tx,
        mut publish_rx,
        _publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let mut status = make_status(
        "M:BEAM",
        State::Bypassed,
        crate::proto::common::alarm::status::Source::Analog,
    );
    status.wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });

    job_tx
        .send(make_job(key.clone(), status, false))
        .await
        .unwrap();

    // Consume the snooze command
    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out waiting for snooze command")
        .expect("snooze channel closed");

    // Reply with Accepted
    snooze_outcome_tx
        .send(SnoozeOutcome::Accepted { key })
        .await
        .unwrap();

    // Now publish must arrive
    let publish = timeout(Duration::from_secs(TIMEOUT_SECS), publish_rx.recv())
        .await
        .expect("timed out waiting for publish command")
        .expect("publish channel closed");

    assert!(
        matches!(publish, Publish::Automated(_)),
        "expected an automated publish after snooze accepted"
    );
}

#[tokio::test]
async fn publish_success_sends_committed_outcome() {
    let (
        job_tx,
        mut job_outcome_rx,
        mut snooze_rx,
        snooze_outcome_tx,
        mut publish_rx,
        publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let status = make_status(
        "M:BEAM",
        State::Alarmed,
        crate::proto::common::alarm::status::Source::Analog,
    );

    job_tx
        .send(make_job(key.clone(), status, false))
        .await
        .unwrap();

    // Consume snooze (Cancel for non-Bypassed)
    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out waiting for snooze command")
        .expect("snooze channel closed");
    snooze_outcome_tx
        .send(SnoozeOutcome::Accepted { key: key.clone() })
        .await
        .unwrap();

    // Consume publish
    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), publish_rx.recv())
        .await
        .expect("timed out waiting for publish command")
        .expect("publish channel closed");

    // Reply with success
    publish_outcome_tx
        .send(PublishOutcome::Single(SymmetricalResult::Ok(key)))
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), job_outcome_rx.recv())
        .await
        .expect("timed out waiting for job outcome")
        .expect("job outcome channel closed");

    assert!(
        matches!(outcome, JobOutcome::Committed(_)),
        "expected JobOutcome::Committed after publish success"
    );
}

#[tokio::test]
async fn publish_failure_sends_failed_outcome() {
    let (
        job_tx,
        mut job_outcome_rx,
        mut snooze_rx,
        snooze_outcome_tx,
        mut publish_rx,
        publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let status = make_status(
        "M:BEAM",
        State::Alarmed,
        crate::proto::common::alarm::status::Source::Analog,
    );

    job_tx
        .send(make_job(key.clone(), status, false))
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out")
        .expect("closed");
    snooze_outcome_tx
        .send(SnoozeOutcome::Accepted { key: key.clone() })
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), publish_rx.recv())
        .await
        .expect("timed out")
        .expect("closed");

    publish_outcome_tx
        .send(PublishOutcome::Single(SymmetricalResult::Err(key)))
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), job_outcome_rx.recv())
        .await
        .expect("timed out waiting for job outcome")
        .expect("job outcome channel closed");

    assert!(
        matches!(outcome, JobOutcome::Failed(_)),
        "expected JobOutcome::Failed after publish failure"
    );
}

#[tokio::test]
async fn snooze_invalid_wake_sends_failed_outcome() {
    let (
        job_tx,
        mut job_outcome_rx,
        mut snooze_rx,
        snooze_outcome_tx,
        _publish_rx,
        _publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let mut status = make_status(
        "M:BEAM",
        State::Bypassed,
        crate::proto::common::alarm::status::Source::Analog,
    );
    status.wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });

    job_tx
        .send(make_job(key.clone(), status, true))
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out")
        .expect("closed");

    snooze_outcome_tx
        .send(SnoozeOutcome::InvalidWake { key })
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), job_outcome_rx.recv())
        .await
        .expect("timed out waiting for job outcome")
        .expect("job outcome channel closed");

    assert!(
        matches!(outcome, JobOutcome::Failed(_)),
        "expected JobOutcome::Failed after InvalidWake"
    );
}

#[tokio::test]
async fn snooze_expiry_sends_wake_outcome() {
    let (
        job_tx,
        mut job_outcome_rx,
        mut snooze_rx,
        snooze_outcome_tx,
        _publish_rx,
        _publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let mut status = make_status(
        "M:BEAM",
        State::Bypassed,
        crate::proto::common::alarm::status::Source::Analog,
    );
    status.wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });

    job_tx
        .send(make_job(key.clone(), status, false))
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out")
        .expect("closed");

    snooze_outcome_tx
        .send(SnoozeOutcome::Expired { key: key.clone() })
        .await
        .unwrap();

    let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), job_outcome_rx.recv())
        .await
        .expect("timed out waiting for job outcome")
        .expect("job outcome channel closed");

    assert!(
        matches!(outcome, JobOutcome::Wake(k) if k == key),
        "expected JobOutcome::Wake with the correct key"
    );
}

#[tokio::test]
async fn non_bypass_job_sends_cancel_snooze() {
    let (
        job_tx,
        _job_outcome_rx,
        mut snooze_rx,
        _snooze_outcome_tx,
        _publish_rx,
        _publish_outcome_tx,
    ) = make_handler();

    let key = make_key("M:BEAM#Analog");
    let status = make_status(
        "M:BEAM",
        State::Alarmed,
        crate::proto::common::alarm::status::Source::Analog,
    );

    job_tx.send(make_job(key, status, false)).await.unwrap();

    let snooze = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
        .await
        .expect("timed out waiting for snooze command")
        .expect("snooze channel closed");

    assert!(
        matches!(snooze, Snooze::Cancel { .. }),
        "expected Snooze::Cancel for a non-Bypassed job"
    );
}

#[tokio::test]
async fn automated_batch_outcome_dispatches_all_results() {
    let (
        job_tx,
        mut job_outcome_rx,
        mut snooze_rx,
        snooze_outcome_tx,
        mut publish_rx,
        publish_outcome_tx,
    ) = make_handler();

    let key1 = make_key("M:BEAM#Analog");
    let key2 = make_key("Z:ACLTST#Digital");

    // Send two jobs
    for key in [key1.clone(), key2.clone()] {
        let status = make_status(
            key.device.as_str(),
            State::Alarmed,
            crate::proto::common::alarm::status::Source::Analog,
        );
        job_tx
            .send(make_job(key.clone(), status, false))
            .await
            .unwrap();

        // Drain snooze for each job
        let snooze = timeout(Duration::from_secs(TIMEOUT_SECS), snooze_rx.recv())
            .await
            .expect("timed out waiting for snooze")
            .expect("snooze channel closed");
        let snooze_key = match snooze {
            Snooze::Cancel { key } => key,
            Snooze::Set { key, .. } => key,
        };
        snooze_outcome_tx
            .send(SnoozeOutcome::Accepted { key: snooze_key })
            .await
            .unwrap();

        // Drain publish for each job
        let _ = timeout(Duration::from_secs(TIMEOUT_SECS), publish_rx.recv())
            .await
            .expect("timed out waiting for publish")
            .expect("publish channel closed");
    }

    // Send a batch outcome for both keys
    publish_outcome_tx
        .send(PublishOutcome::Batch(vec![
            SymmetricalResult::Ok(key1),
            SymmetricalResult::Ok(key2),
        ]))
        .await
        .unwrap();

    // Both JobOutcome messages must arrive
    for _ in 0..2 {
        let outcome = timeout(Duration::from_secs(TIMEOUT_SECS), job_outcome_rx.recv())
            .await
            .expect("timed out waiting for job outcome")
            .expect("job outcome channel closed");
        assert!(
            matches!(outcome, JobOutcome::Committed(_)),
            "expected JobOutcome::Committed for each batch entry"
        );
    }
}
