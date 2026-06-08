//! Publish effect execution.
//!
//! This module owns transport-facing publish work. It accepts [`DomainEffect`] values from the
//! coordinator, performs publish attempts through a [`Publisher`], and reports completed
//! [`PublishOutcome`] values back to the coordinator.

use std::{
    collections::{HashMap, hash_map::Entry},
    mem::take,
    time::{Duration, Instant},
};

use futures::stream::{FuturesUnordered, StreamExt};
use rust_pubsub_lib::{Message, Publisher, StringMessage};
use tokio::{
    sync::mpsc,
    time::{MissedTickBehavior, interval},
};
use tracing::{error, warn};

use crate::{
    engine::messages::{CoordinatorMessage, DomainEffect, EffectResult},
    metrics::{Metrics, PublishOutcomeKind},
    model::{
        key::Key,
        publish::{PublishAttempt, PublishOutcome, PublishResult},
    },
    proto::common::alarm::Status,
};

#[cfg(test)]
mod tests;

const BATCH_SIZE: usize = 50;
const BUFFER_CAPACITY: usize = 100;
const FLUSH_INTERVAL_MS: u64 = 10;
const MAX_DELIVERY_ATTEMPTS: u8 = 2;

type TimedPublishResult = (PublishResult, Duration);

/// Runs the publish engine.
///
/// The publish engine owns transport concerns for publish effects:
///
/// - it keeps at most one tracked publish attempt per [`Key`]
/// - a newer attempt for the same key replaces the older tracked attempt before any further retry
/// - retries are only performed for the attempt that is still current for that key
/// - completed outcomes are reported back to the coordinator with their original ids intact
///
/// This keeps transport freshness in the publish engine while leaving the coordinator responsible
/// for deciding what the latest desired state is and for interpreting returned outcomes.
///
/// ## Invariants
///
/// - `tracked_alarms` contains at most one current tracked attempt per [`Key`]
/// - replacing an entry in `tracked_alarms` does not cancel an already in-flight transport attempt;
///   stale completions are therefore expected and must be reported with their original ids
/// - only the attempt currently stored in `tracked_alarms` is eligible for retry
/// - a stale completion may trigger delivery of the newer tracked attempt, but it must not restore
///   the stale attempt as current
/// - user-initiated completions are forwarded immediately as [`PublishOutcome::Single`] so they are
///   not structurally delayed behind automated batch flushing
/// - automated completions may be buffered and flushed as [`PublishOutcome::Batch`] to reduce
///   coordinator message traffic
/// - the publish engine owns transport freshness and retry policy, but it does not decide domain
///   truth; the coordinator remains the only authority that interprets returned outcomes
pub async fn run_publish_engine<P: Publisher + Send + Sync + 'static>(
    publisher: P,
    mut effect_rx: mpsc::Receiver<DomainEffect>,
    priority_tx: mpsc::Sender<CoordinatorMessage>,
    metrics: Metrics,
) {
    let mut in_flight = FuturesUnordered::new();
    let mut completed_batch = Vec::with_capacity(BUFFER_CAPACITY);
    let mut flush_interval = interval(Duration::from_millis(FLUSH_INTERVAL_MS));
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut tracked_alarms: HashMap<Key, PublishAttempt> = HashMap::new();

    loop {
        tokio::select! {
            biased;

            Some(result) = in_flight.next(), if !in_flight.is_empty() => {
                let (result, latency): TimedPublishResult = result;
                match result {
                    Ok(attempt) => {
                        let attempt_details = attempt.details_ref();
                        let outcome_kind = if let Some(latest) = tracked_alarms.get(&attempt_details.key) {
                            if latest.details_ref().id > attempt_details.id {
                                in_flight.push(deliver_attempt(&publisher, latest.clone(), metrics.clone()));
                                PublishOutcomeKind::Superseded
                            } else {
                                tracked_alarms.remove(&attempt_details.key);
                                PublishOutcomeKind::Success
                            }
                        } else {
                            error!("Record was dropped for {}! Bad state!", attempt_details.key);
                            PublishOutcomeKind::Success
                        };
                        metrics.record_publish_completion(outcome_kind, latency);
                        handle_completed_work(Ok(attempt), &priority_tx, &mut completed_batch).await;
                    }
                    Err(mut attempt) => {
                        let attempt_details = attempt.details_ref();
                        if let Some(latest) = tracked_alarms.get(&attempt_details.key) {
                            if latest.details_ref().id > attempt_details.id {
                                metrics.record_publish_completion(PublishOutcomeKind::Superseded, latency);
                                in_flight.push(deliver_attempt(&publisher, latest.clone(), metrics.clone()));
                                handle_completed_work(Ok(attempt), &priority_tx, &mut completed_batch).await;
                            } else if attempt.attempt_count < MAX_DELIVERY_ATTEMPTS {
                                metrics.record_publish_completion(PublishOutcomeKind::Failure, latency);
                                metrics.record_publish_retry();
                                attempt.increment_attempt();
                                tracked_alarms.insert(attempt.details_ref().key.clone(), attempt.clone());
                                in_flight.push(deliver_attempt(&publisher, attempt.clone(), metrics.clone()));
                            } else {
                                metrics.record_publish_completion(PublishOutcomeKind::Failure, latency);
                                tracked_alarms.remove(&attempt_details.key);
                                handle_completed_work(Err(attempt), &priority_tx, &mut completed_batch).await;
                            }
                        } else {
                            error!("Record was dropped for {}! Bad state!", attempt_details.key);
                            metrics.record_publish_completion(PublishOutcomeKind::Failure, latency);
                            handle_completed_work(Err(attempt), &priority_tx, &mut completed_batch).await;
                        }
                    }
                }
            },

            _ = flush_interval.tick() => {
                if !completed_batch.is_empty() {
                    flush_completed_batch(&priority_tx, &mut completed_batch).await;
                }
            },

            Some(effect) = effect_rx.recv() => {
                match effect {
                    DomainEffect::Publish(work) => {
                        let attempt = PublishAttempt::new(work);
                        match tracked_alarms.entry(attempt.details_ref().key.clone()) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(attempt.clone());
                                in_flight.push(deliver_attempt(&publisher, attempt, metrics.clone()));
                            }
                            Entry::Occupied(mut occupied) => {
                                let _ = occupied.insert(attempt);
                            }
                        }
                    }
                }
            }

            else => break,
        }
    }
}

async fn handle_completed_work(
    completed_work: PublishResult,
    priority_tx: &mpsc::Sender<CoordinatorMessage>,
    completed_batch: &mut Vec<PublishResult>,
) {
    let is_user_initiated = match &completed_work {
        Ok(delivered) => delivered.request.is_user_initiated(),
        Err(failure) => failure.request.is_user_initiated(),
    };

    if is_user_initiated {
        if priority_tx
            .send(CoordinatorMessage::EffectResult(EffectResult::Publish(
                PublishOutcome::Single(completed_work),
            )))
            .await
            .is_err()
        {
            warn!("Dropping user delivery outcome because the coordinator has stopped");
        }
    } else {
        completed_batch.push(completed_work);
        if completed_batch.len() >= BATCH_SIZE {
            flush_completed_batch(priority_tx, completed_batch).await;
        }
    }
}

async fn flush_completed_batch(
    priority_tx: &mpsc::Sender<CoordinatorMessage>,
    completed_batch: &mut Vec<PublishResult>,
) {
    let batch = take(completed_batch);
    if priority_tx
        .send(CoordinatorMessage::EffectResult(EffectResult::Publish(
            PublishOutcome::Batch(batch),
        )))
        .await
        .is_err()
    {
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
        Ok(()) => Ok(attempt),
        Err(e) => {
            error!(
                "Failed to send Kafka message: {e}\n{:?}\n{:?}",
                details.key, details.status
            );
            Err(attempt)
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
