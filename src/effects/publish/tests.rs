//! Tests for the publish effect module.
//!
//! # Structure
//!
//! - **Unit tests** for publish-engine routing logic, driven entirely through the channel pair.
//!   No real broker is needed. A controllable test publisher makes publishes complete deterministically.
//! - **Integration test** using [`KafkaTestHarness`] to verify that the serialized payload reaches
//!   the broker with the correct key.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use rust_pubsub_lib::{
    KafkaPublisher, KafkaSubscriber, KafkaTestHarness, Message, PubSubError, Publisher,
    StringMessage, Subscriber,
};
use tokio::{sync::mpsc, time::timeout};
use tokio_stream::StreamExt;

use super::*;
use crate::{
    metrics::Metrics,
    model::{
        key::Key,
        publish::{Publish, PublishDetails},
    },
    proto::common::alarm::{
        Status,
        status::{Source, State},
    },
    test_utils::{DEFAULT_TEST_QUEUE_CONFIG, TestPub, make_status},
};

const TEST_PIPELINE_CAPACITY: usize = 8;

#[derive(Clone, Copy, Debug)]
enum PublishBehavior {
    Succeed,
    Fail,
}

#[derive(Clone, Debug)]
struct ControlledPublisher {
    plans: Arc<Mutex<HashMap<String, VecDeque<PublishBehavior>>>>,
    sent: Arc<Mutex<Vec<String>>>,
}

impl ControlledPublisher {
    fn new(plans: impl IntoIterator<Item = (String, Vec<PublishBehavior>)>) -> Self {
        let plans = plans
            .into_iter()
            .map(|(key, behaviors)| (key, VecDeque::from(behaviors)))
            .collect();
        Self {
            plans: Arc::new(Mutex::new(plans)),
            sent: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sent_keys(&self) -> Vec<String> {
        self.sent.lock().expect("sent lock poisoned").clone()
    }
}

#[tonic::async_trait]
impl Publisher for ControlledPublisher {
    fn new(_host: String, _topic: String) -> Self {
        Self::new([])
    }

    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        let bytes = message.into_bytes();
        let decoded = StringMessage::from_bytes(bytes.key_ref(), bytes.value_ref());
        let key = decoded
            .key_ref()
            .expect("publish engine should always set a key")
            .to_string();

        self.sent
            .lock()
            .expect("sent lock poisoned")
            .push(key.clone());

        let behavior = self
            .plans
            .lock()
            .expect("plans lock poisoned")
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .unwrap_or(PublishBehavior::Fail);

        match behavior {
            PublishBehavior::Succeed => Ok(()),
            PublishBehavior::Fail => Err(PubSubError::default()),
        }
    }
}

fn make_pipeline<P: Publisher + Send + Sync + 'static>(
    publisher: P,
) -> (mpsc::Sender<Publish>, mpsc::Receiver<PublishOutcome>) {
    let (publish_tx, publish_rx) = mpsc::channel::<Publish>(TEST_PIPELINE_CAPACITY);
    let (publish_outcome_tx, publish_outcome_rx) =
        mpsc::channel::<PublishOutcome>(TEST_PIPELINE_CAPACITY);
    let port = PublishEffectPort {
        publish_rx,
        publish_outcome_tx,
    };
    tokio::spawn(run_publish_engine(
        publisher,
        port,
        Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG),
    ));
    (publish_tx, publish_outcome_rx)
}

async fn recv_delivery_outcome(rx: &mut mpsc::Receiver<PublishOutcome>) -> PublishOutcome {
    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for delivery outcome")
        .expect("outcome channel closed unexpectedly")
}

async fn recv_automated_batch(
    rx: &mut mpsc::Receiver<PublishOutcome>,
) -> Vec<crate::model::errors::SymmetricalResult<Key>> {
    match recv_delivery_outcome(rx).await {
        PublishOutcome::Batch(batch) => batch,
        PublishOutcome::Single(_) => panic!("expected automated delivery results to be batched"),
    }
}

fn automated_publish(device: &str, source: Source, state: State) -> Publish {
    let key = Key::try_from(format!("{device}#{source:?}").as_str()).unwrap();
    let status = make_status(device, state, source);
    Publish::Automated(PublishDetails { key, status })
}

fn user_publish(device: &str, source: Source, state: State, user: &str) -> Publish {
    let key = Key::try_from(format!("{device}#{source:?}").as_str()).unwrap();
    let mut status = make_status(device, state, source);
    status.user = user.to_string();
    Publish::User(PublishDetails { key, status })
}

#[tokio::test]
async fn user_update_success_routes_single_delivery_success() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init());

    publish_tx
        .send(user_publish(
            "M:BEAM",
            Source::Analog,
            State::Acknowledged,
            "operator",
        ))
        .await
        .unwrap();

    let outcome = recv_delivery_outcome(&mut outcome_rx).await;
    assert!(matches!(outcome, PublishOutcome::Single(ref r) if r.is_ok()));
}

#[tokio::test]
async fn user_update_publish_failure_routes_single_retryable_failure() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init_throwing());

    publish_tx
        .send(user_publish(
            "M:BEAM",
            Source::Analog,
            State::Acknowledged,
            "operator",
        ))
        .await
        .unwrap();

    match recv_delivery_outcome(&mut outcome_rx).await {
        PublishOutcome::Single(result) => {
            assert!(!result.is_ok(), "expected a failure result");
        }
        _ => panic!("expected single failed delivery result"),
    }
}

#[tokio::test]
async fn automated_update_success_routes_batched_delivery_success() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init());

    publish_tx
        .send(automated_publish("M:BEAM", Source::Analog, State::Alarmed))
        .await
        .unwrap();

    let results = recv_automated_batch(&mut outcome_rx).await;
    assert_eq!(results.len(), 1, "expected exactly one result in the batch");
    assert!(results[0].is_ok());
}

#[tokio::test]
async fn automated_update_publish_failure_routes_batched_retryable_failure() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init_throwing());

    publish_tx
        .send(automated_publish(
            "Z:ACLTST",
            Source::Digital,
            State::Alarmed,
        ))
        .await
        .unwrap();

    let results = recv_automated_batch(&mut outcome_rx).await;
    assert_eq!(results.len(), 1, "expected exactly one result in the batch");
    assert!(!results[0].is_ok(), "expected failed delivery result");
}

/// The publish engine no longer deduplicates per-key — supersession is the coordinator's
/// responsibility. Two publishes for the same key must both go in-flight independently.
#[tokio::test]
async fn two_publishes_for_same_key_both_go_in_flight_independently() {
    let publisher = ControlledPublisher::new([(
        "M:BEAM#Analog".to_string(),
        vec![PublishBehavior::Succeed, PublishBehavior::Succeed],
    )]);
    let sent = publisher.clone();
    let (publish_tx, mut outcome_rx) = make_pipeline(publisher);

    publish_tx
        .send(automated_publish("M:BEAM", Source::Analog, State::Alarmed))
        .await
        .unwrap();
    publish_tx
        .send(automated_publish("M:BEAM", Source::Analog, State::Latched))
        .await
        .unwrap();

    // Both should complete and appear in the batch
    let batch = recv_automated_batch(&mut outcome_rx).await;
    assert!(
        !batch.is_empty(),
        "at least one result must arrive in the batch"
    );
    assert!(
        batch.iter().all(|r| r.is_ok()),
        "all results should succeed"
    );

    // Both publishes should have been sent to the broker
    assert_eq!(
        sent.sent_keys().len(),
        2,
        "both publishes must be dispatched independently"
    );
}

#[tokio::test]
async fn user_completion_is_not_batched_behind_automated_results() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init());

    publish_tx
        .send(automated_publish(
            "AUTO:ONE",
            Source::Analog,
            State::Alarmed,
        ))
        .await
        .unwrap();
    publish_tx
        .send(user_publish(
            "USER:ONE",
            Source::Digital,
            State::Bypassed,
            "operator",
        ))
        .await
        .unwrap();

    let first = recv_delivery_outcome(&mut outcome_rx).await;
    assert!(
        matches!(first, PublishOutcome::Single(ref r) if r.is_ok()),
        "user completion should be forwarded immediately as a single result"
    );

    let second = recv_delivery_outcome(&mut outcome_rx).await;
    assert!(
        matches!(second, PublishOutcome::Batch(ref batch) if batch.len() == 1),
        "automated completion should still arrive as a batch"
    );
}

#[tokio::test]
async fn automated_batch_flushed_when_buffer_reaches_capacity() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init());

    for id in 0..TEST_PIPELINE_CAPACITY as u64 {
        let device = format!("DEV{id}");
        publish_tx
            .send(automated_publish(&device, Source::Analog, State::Alarmed))
            .await
            .unwrap();
    }

    let results = recv_automated_batch(&mut outcome_rx).await;
    assert_eq!(
        results.len(),
        TEST_PIPELINE_CAPACITY,
        "bounded action queue should flush once the completed batch reaches channel capacity"
    );
    assert!(results.iter().all(|r| r.is_ok()), "all results must be Ok");
}

#[tokio::test]
async fn partial_batch_flushed_on_timer() {
    let (publish_tx, mut outcome_rx) = make_pipeline(TestPub::init());

    for id in 0..3u64 {
        let device = format!("DEV{id}");
        publish_tx
            .send(automated_publish(&device, Source::Analog, State::Alarmed))
            .await
            .unwrap();
    }

    let results = recv_automated_batch(&mut outcome_rx).await;
    assert_eq!(
        results.len(),
        3,
        "partial batch must be flushed by the timer"
    );
}

#[tokio::test]
async fn pipeline_exits_when_state_queue_receiver_is_dropped() {
    let (publish_tx, outcome_rx) = make_pipeline(TestPub::init());
    drop(outcome_rx);
    drop(publish_tx);
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn message_body_is_json_status() {
    let dropbox = Arc::default();
    let test_pub = TestPub::init_inspectable(Arc::clone(&dropbox));
    let (publish_tx, mut outcome_rx) = make_pipeline(test_pub);

    let key = Key::try_from("Z:ACLTST#Digital").unwrap();
    let mut status = make_status("Z:ACLTST", State::Bypassed, Source::Digital);
    status.user = "operator".to_string();

    publish_tx
        .send(Publish::User(PublishDetails {
            key: key.clone(),
            status: status.clone(),
        }))
        .await
        .unwrap();

    let outcome = recv_delivery_outcome(&mut outcome_rx).await;
    assert!(matches!(outcome, PublishOutcome::Single(ref r) if r.is_ok()));

    let json = serde_json::to_string(&status).expect("Status must be JSON-serializable");
    let message = StringMessage::new(Some(key.to_string()), json);
    let encoded = message.into_bytes();
    let actual = dropbox
        .lock()
        .expect("lock should not be poisoned")
        .take()
        .expect("Dropbox must have gotten message");
    assert_eq!(encoded, actual, "sent message should match received");
}

#[tokio::test]
async fn kafka_publish_reaches_broker_with_correct_key() {
    let (harness, topic) = KafkaTestHarness::with_new_topic("kafka-worker-integration").await;
    let host = harness.host().await;

    let mut subscriber = KafkaSubscriber::new(host.clone(), topic.clone());
    let mut stream = subscriber.get_stream::<StringMessage>().await.unwrap();

    let publisher = KafkaPublisher::new(host.clone(), topic.clone());
    let (publish_tx, mut outcome_rx) = make_pipeline(publisher);

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let status = make_status("M:BEAM", State::Alarmed, Source::Analog);

    publish_tx
        .send(Publish::Automated(PublishDetails {
            key: key.clone(),
            status: status.clone(),
        }))
        .await
        .unwrap();

    let results = recv_automated_batch(&mut outcome_rx).await;
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());

    let msg = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for Kafka message")
        .expect("stream ended unexpectedly")
        .expect("Kafka subscriber returned an error");

    assert_eq!(msg.key_ref(), Some("M:BEAM#Analog"));
    let decoded: Status =
        serde_json::from_str(msg.value_ref()).expect("payload must be valid JSON");
    assert_eq!(decoded.device, status.device);
    assert_eq!(decoded.state(), status.state());
    assert_eq!(decoded.source(), status.source());
}
