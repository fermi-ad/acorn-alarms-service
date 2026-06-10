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
    test_utils::{TestPub, make_status},
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
            .unwrap_or(PublishBehavior::Succeed);

        match behavior {
            PublishBehavior::Succeed => Ok(()),
            PublishBehavior::Fail => Err(PubSubError::default()),
        }
    }
}

fn make_pipeline<P: Publisher + Send + Sync + 'static>(
    publisher: P,
) -> (
    mpsc::Sender<DomainEffect>,
    mpsc::Receiver<CoordinatorMessage>,
) {
    let (effect_tx, effect_rx) = mpsc::channel::<DomainEffect>(TEST_PIPELINE_CAPACITY);
    let (action_tx, action_rx) = mpsc::channel::<CoordinatorMessage>(TEST_PIPELINE_CAPACITY);
    tokio::spawn(run_publish_engine(
        publisher,
        effect_rx,
        action_tx,
        Metrics::new(),
    ));
    (effect_tx, action_rx)
}

async fn recv_delivery_outcome(rx: &mut mpsc::Receiver<CoordinatorMessage>) -> PublishOutcome {
    match timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timed out waiting for delivery outcome")
        .expect("state channel closed unexpectedly")
    {
        CoordinatorMessage::EffectResult(EffectResult::Publish(outcome)) => outcome,
        _ => panic!("expected delivery outcome from the delivery engine"),
    }
}

async fn recv_automated_batch(
    rx: &mut mpsc::Receiver<CoordinatorMessage>,
) -> Vec<Result<PublishAttempt, PublishAttempt>> {
    match recv_delivery_outcome(rx).await {
        PublishOutcome::Batch(batch) => batch,
        PublishOutcome::Single(_) => panic!("expected automated delivery results to be batched"),
    }
}

fn automated_publish(id: u64, device: &str, source: Source, state: State) -> DomainEffect {
    let key = Key::try_from(format!("{device}#{source:?}").as_str()).unwrap();
    let status = make_status(device, state, source);
    DomainEffect::Publish(Publish::Automated(PublishDetails { id, key, status }))
}

fn user_publish(id: u64, device: &str, source: Source, state: State, user: &str) -> DomainEffect {
    let key = Key::try_from(format!("{device}#{source:?}").as_str()).unwrap();
    let mut status = make_status(device, state, source);
    status.user = user.to_string();
    DomainEffect::Publish(Publish::User(PublishDetails { id, key, status }))
}

#[tokio::test]
async fn user_update_success_routes_single_delivery_success() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init());

    effect_tx
        .send(user_publish(
            1,
            "M:BEAM",
            Source::Analog,
            State::Acknowledged,
            "operator",
        ))
        .await
        .unwrap();

    let outcome = recv_delivery_outcome(&mut action_rx).await;
    assert!(matches!(outcome, PublishOutcome::Single(Ok(_))));
}

#[tokio::test]
async fn user_update_publish_failure_routes_single_retryable_failure() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init_throwing());

    effect_tx
        .send(user_publish(
            42,
            "M:BEAM",
            Source::Analog,
            State::Acknowledged,
            "operator",
        ))
        .await
        .unwrap();

    match recv_delivery_outcome(&mut action_rx).await {
        PublishOutcome::Single(Err(failure)) => {
            assert_eq!(
                failure.attempt_count, MAX_DELIVERY_ATTEMPTS,
                "should try twice before giving up"
            );
        }
        _ => panic!("expected single failed delivery result"),
    }
}

#[tokio::test]
async fn automated_update_success_routes_batched_delivery_success() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init());

    effect_tx
        .send(automated_publish(
            7,
            "M:BEAM",
            Source::Analog,
            State::Alarmed,
        ))
        .await
        .unwrap();

    let results = recv_automated_batch(&mut action_rx).await;
    assert_eq!(results.len(), 1, "expected exactly one result in the batch");
    assert!(results[0].is_ok());
}

#[tokio::test]
async fn automated_update_publish_failure_routes_batched_retryable_failure() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init_throwing());

    effect_tx
        .send(automated_publish(
            99,
            "Z:ACLTST",
            Source::Digital,
            State::Alarmed,
        ))
        .await
        .unwrap();

    let results = recv_automated_batch(&mut action_rx).await;
    assert_eq!(results.len(), 1, "expected exactly one result in the batch");
    match &results[0] {
        Err(failure) => assert_eq!(
            failure.attempt_count, MAX_DELIVERY_ATTEMPTS,
            "should try twice before returning failure"
        ),
        Ok(_) => panic!("expected failed delivery result"),
    }
}

#[tokio::test]
async fn stale_failure_of_replaced_attempt_is_not_retried() {
    let publisher = ControlledPublisher::new([(
        "M:BEAM#Analog".to_string(),
        vec![PublishBehavior::Fail, PublishBehavior::Succeed],
    )]);
    let sent = publisher.clone();
    let (effect_tx, mut action_rx) = make_pipeline(publisher);

    effect_tx
        .send(automated_publish(
            1,
            "M:BEAM",
            Source::Analog,
            State::Alarmed,
        ))
        .await
        .unwrap();
    effect_tx
        .send(automated_publish(
            2,
            "M:BEAM",
            Source::Analog,
            State::Latched,
        ))
        .await
        .unwrap();

    let batch = recv_automated_batch(&mut action_rx).await;
    assert_eq!(
        batch.len(),
        2,
        "timer-based batching may flush replaced and current outcomes together"
    );
    assert!(
        batch
            .iter()
            .any(|result| matches!(result, Ok(success) if success.details_ref().id == 1)),
        "the replaced failed attempt should be reported as superseded transport history"
    );
    assert!(
        batch
            .iter()
            .any(|result| matches!(result, Ok(success) if success.details_ref().id == 2)),
        "the newer tracked attempt should still be delivered"
    );

    assert_eq!(
        sent.sent_keys(),
        vec![
            "M:BEAM#Analog".to_string(),
            "M:BEAM#Analog".to_string(),
            "M:BEAM#Analog".to_string(),
        ],
        "publish engine should send the replaced attempt once, then retry the newer tracked attempt"
    );
}

#[tokio::test]
async fn stale_success_reports_old_attempt_then_delivers_newer_attempt() {
    let publisher = ControlledPublisher::new([(
        "M:BEAM#Analog".to_string(),
        vec![PublishBehavior::Succeed, PublishBehavior::Succeed],
    )]);
    let sent = publisher.clone();
    let (effect_tx, mut action_rx) = make_pipeline(publisher);

    effect_tx
        .send(automated_publish(
            10,
            "M:BEAM",
            Source::Analog,
            State::Alarmed,
        ))
        .await
        .unwrap();
    effect_tx
        .send(automated_publish(
            11,
            "M:BEAM",
            Source::Analog,
            State::Latched,
        ))
        .await
        .unwrap();

    let batch = recv_automated_batch(&mut action_rx).await;
    assert_eq!(batch.len(), 2);
    assert!(
        matches!(&batch[0], Ok(success) if success.details_ref().id == 10),
        "first result should report the stale successful attempt"
    );
    assert!(
        matches!(&batch[1], Ok(success) if success.details_ref().id == 11),
        "second result should report the newer successful attempt"
    );

    assert_eq!(
        sent.sent_keys(),
        vec!["M:BEAM#Analog".to_string(), "M:BEAM#Analog".to_string(),],
        "publish engine should deliver the stale completion and then the newer tracked attempt"
    );
}

#[tokio::test]
async fn user_completion_is_not_batched_behind_automated_results() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init());

    effect_tx
        .send(automated_publish(
            1,
            "AUTO:ONE",
            Source::Analog,
            State::Alarmed,
        ))
        .await
        .unwrap();
    effect_tx
        .send(user_publish(
            2,
            "USER:ONE",
            Source::Digital,
            State::Bypassed,
            "operator",
        ))
        .await
        .unwrap();

    let first = recv_delivery_outcome(&mut action_rx).await;
    assert!(
        matches!(first, PublishOutcome::Single(Ok(_))),
        "user completion should be forwarded immediately as a single result"
    );

    let second = recv_delivery_outcome(&mut action_rx).await;
    assert!(
        matches!(second, PublishOutcome::Batch(batch) if batch.len() == 1),
        "automated completion should still arrive as a batch"
    );
}

#[tokio::test]
async fn automated_batch_flushed_when_buffer_reaches_capacity() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init());

    for id in 0..TEST_PIPELINE_CAPACITY as u64 {
        let device = format!("DEV{id}");
        effect_tx
            .send(automated_publish(
                id,
                &device,
                Source::Analog,
                State::Alarmed,
            ))
            .await
            .unwrap();
    }

    let results = recv_automated_batch(&mut action_rx).await;
    assert_eq!(
        results.len(),
        TEST_PIPELINE_CAPACITY,
        "bounded action queue should flush once the completed batch reaches channel capacity"
    );
    assert!(results.iter().all(Result::is_ok), "all results must be Ok");
}

#[tokio::test]
async fn partial_batch_flushed_on_timer() {
    let (effect_tx, mut action_rx) = make_pipeline(TestPub::init());

    for id in 0..3u64 {
        let device = format!("DEV{id}");
        effect_tx
            .send(automated_publish(
                id,
                &device,
                Source::Analog,
                State::Alarmed,
            ))
            .await
            .unwrap();
    }

    let results = recv_automated_batch(&mut action_rx).await;
    assert_eq!(
        results.len(),
        3,
        "partial batch must be flushed by the timer"
    );
}

#[tokio::test]
async fn pipeline_exits_when_state_queue_receiver_is_dropped() {
    let (effect_tx, action_rx) = make_pipeline(TestPub::init());
    drop(action_rx);
    drop(effect_tx);
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn message_body_is_json_status() {
    let dropbox = Arc::default();
    let test_pub = TestPub::init_inspectable(Arc::clone(&dropbox));
    let (effect_tx, mut action_rx) = make_pipeline(test_pub);

    let key = Key::try_from("Z:ACLTST#Digital").unwrap();
    let mut status = make_status("Z:ACLTST", State::Bypassed, Source::Digital);
    status.user = "operator".to_string();

    effect_tx
        .send(DomainEffect::Publish(Publish::User(PublishDetails {
            id: 5,
            key: key.clone(),
            status: status.clone(),
        })))
        .await
        .unwrap();

    let outcome = recv_delivery_outcome(&mut action_rx).await;
    assert!(matches!(outcome, PublishOutcome::Single(Ok(_))));

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
    let (effect_tx, mut action_rx) = make_pipeline(publisher);

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let status = make_status("M:BEAM", State::Alarmed, Source::Analog);

    effect_tx
        .send(DomainEffect::Publish(Publish::Automated(PublishDetails {
            id: 123,
            key: key.clone(),
            status: status.clone(),
        })))
        .await
        .unwrap();

    let results = recv_automated_batch(&mut action_rx).await;
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
