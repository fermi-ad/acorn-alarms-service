//! Publish effect execution.
//!
//! This module owns transport-facing publish work. It accepts [`Publish`] values from the
//! workflow handler, performs publish attempts through a [`Publisher`], and reports completed
//! [`PublishOutcome`] values back to the workflow handler.

use std::{
    mem::take,
    time::{Duration, Instant},
};

use futures::stream::{FuturesUnordered, StreamExt};
use rust_pubsub_lib::{Message, Publisher, StringMessage};
use tokio::{
    sync::mpsc::{self, error::SendError},
    time::{MissedTickBehavior, interval},
};
use tracing::{error, warn};

use crate::{
    metrics::{Metrics, PublishOutcomeKind},
    model::{
        errors::SymmetricalResult,
        key::Key,
        publish::{Publish, PublishAttempt, PublishOutcome, PublishResult},
    },
    proto::common::alarm::Status,
};

#[cfg(test)]
mod tests;

const BATCH_SIZE: usize = 50;
const BUFFER_CAPACITY: usize = 100;
const FLUSH_INTERVAL_MS: u64 = 10;
const MAX_DELIVERY_ATTEMPTS: u8 = 2;

type TimedPublishResult = (SymmetricalResult<PublishAttempt>, Duration);

/// Channel pair used by the workflow handler to communicate with the publish engine.
///
/// `publish_rx` receives incoming [`Publish`] requests; `publish_outcome_tx` sends completed
/// [`PublishOutcome`] values back to the workflow handler.
pub struct PublishEffectPort {
    pub publish_rx: mpsc::Receiver<Publish>,
    pub publish_outcome_tx: mpsc::Sender<PublishOutcome>,
}
impl PublishEffectPort {
    async fn recv(&mut self) -> Option<Publish> {
        self.publish_rx.recv().await
    }

    async fn send(&self, outgoing: PublishOutcome) -> Result<(), SendError<PublishOutcome>> {
        self.publish_outcome_tx.send(outgoing).await
    }
}

/// Runs the publish engine.
///
/// Every incoming [`Publish`] is dispatched immediately as a transport attempt. A failed attempt
/// is retried up to `MAX_DELIVERY_ATTEMPTS` times before a failure outcome is reported.
///
/// User-initiated completions are forwarded immediately as [`PublishOutcome::Single`].
/// Automated completions are buffered and flushed as [`PublishOutcome::Batch`] to reduce
/// message traffic on the outcome channel.
pub async fn run_publish_engine<P: Publisher + Send + Sync + 'static>(
    publisher: P,
    mut port: PublishEffectPort,
    metrics: Metrics,
) {
    let mut in_flight = FuturesUnordered::new();
    let mut completed_batch = Vec::with_capacity(BUFFER_CAPACITY);
    let mut flush_interval = interval(Duration::from_millis(FLUSH_INTERVAL_MS));
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;

            Some(result) = in_flight.next(), if !in_flight.is_empty() => {
                let (result, latency): TimedPublishResult = result;
                match result {
                    SymmetricalResult::Ok(attempt) => {
                        metrics.record_publish_completion(PublishOutcomeKind::Success, latency);
                        handle_completed_work(SymmetricalResult::Ok(attempt), &port, &mut completed_batch).await;
                    }
                    SymmetricalResult::Err(mut attempt) => {
                        if attempt.attempt_count < MAX_DELIVERY_ATTEMPTS {
                                metrics.record_publish_completion(PublishOutcomeKind::Failure, latency);
                                metrics.record_publish_retry();
                                attempt.increment_attempt();
                                in_flight.push(deliver_attempt(&publisher, attempt.clone(), metrics.clone()));
                            } else {
                                metrics.record_publish_completion(PublishOutcomeKind::Failure, latency);
                                handle_completed_work(SymmetricalResult::Err(attempt), &port, &mut completed_batch).await;
                            }
                    }
                }
            },

            _ = flush_interval.tick() => {
                if !completed_batch.is_empty() {
                    flush_completed_batch(&port, &mut completed_batch).await;
                }
            },

            Some(publish) = port.recv() => {
                let attempt = PublishAttempt::new(publish);
                in_flight.push(deliver_attempt(&publisher, attempt, metrics.clone()));
            }

            else => break,
        }
    }
}

async fn handle_completed_work(
    completed_work: SymmetricalResult<PublishAttempt>,
    port: &PublishEffectPort,
    completed_batch: &mut Vec<PublishResult>,
) {
    let is_user_initiated = completed_work.inner_ref().request.is_user_initiated();
    let response = completed_work.map(|inner| inner.request.into_details().key);

    if is_user_initiated {
        if port.send(PublishOutcome::Single(response)).await.is_err() {
            warn!("Dropping user delivery outcome because the coordinator has stopped");
        }
    } else {
        completed_batch.push(response);
        if completed_batch.len() >= BATCH_SIZE {
            flush_completed_batch(port, completed_batch).await;
        }
    }
}

async fn flush_completed_batch(port: &PublishEffectPort, completed_batch: &mut Vec<PublishResult>) {
    let batch = take(completed_batch);
    if port.send(PublishOutcome::Batch(batch)).await.is_err() {
        warn!("Dropping automated delivery batch because the coordinator has stopped");
    }
}

/// Performs a single publish attempt.
///
/// Retry and supersession decisions are made by [`run_publish_engine`]
/// after the result is returned. This function only performs one transport attempt and preserves
/// the original [`PublishAttempt`] so the caller can reconcile by id.
async fn deliver_attempt<P: Publisher + Send + Sync + 'static>(
    publisher: &P,
    attempt: PublishAttempt,
    metrics: Metrics,
) -> TimedPublishResult {
    metrics.record_publish_attempt_started();
    let started_at = Instant::now();
    let details = attempt.details_ref();
    let record = alarm_to_message(&details.key, &details.status);
    let result = match publisher.publish(record).await {
        Ok(()) => SymmetricalResult::Ok(attempt),
        Err(e) => {
            error!(
                "Failed to send Kafka message: {e}\n{:?}\n{:?}",
                details.key, details.status
            );
            SymmetricalResult::Err(attempt)
        }
    };
    (result, started_at.elapsed())
}

/// Converts a [`Key`] and [`Status`] into a [`StringMessage`] ready for publishing.
///
/// The message key is the [`Key`] string representation of the alarm
/// (`"DEVICE#SourceVariant"`), which lets downstream consumers identify which
/// alarm a Kafka message belongs to without deserializing the body.
fn alarm_to_message(key: &Key, payload: &Status) -> StringMessage {
    let message_body = serde_json::to_string(payload).expect("Status should be JSON serializable");
    StringMessage::new(Some(key.to_string()), message_body)
}
