//! Snooze scheduler — `DelayQueue`-backed per-key timer management.
//!
//! Accepts [`Snooze`] commands and maintains a [`DelayQueue`] of per-key timers. Emits
//! [`SnoozeOutcome`] values in response to commands and when timers fire.
//!
//! ## Protocol
//!
//! - `Snooze::Set { key, wake }` — register (or replace) a timer for `key` that fires at `wake`.
//!   Responds with `SnoozeOutcome::Accepted` on success or `SnoozeOutcome::InvalidWake` if the
//!   timestamp is in the past or out of range.
//! - `Snooze::Cancel { key }` — remove any existing timer for `key`.  Always responds with
//!   `SnoozeOutcome::Accepted` (cancelling a non-existent timer is a no-op).

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use tokio::{
    sync::mpsc::{self, error::SendError},
    time::Instant,
};
use tokio_stream::StreamExt;
use tokio_util::time::{DelayQueue, delay_queue::Key as DelayTaskKey};

use crate::{
    metrics::Metrics,
    model::{
        key::Key as AlarmKey,
        snooze::{Snooze, SnoozeOutcome},
    },
    proto::google::protobuf::Timestamp,
};

#[cfg(test)]
mod tests;

/// Channel pair used by the workflow handler to communicate with the snooze scheduler.
pub struct SnoozeEffectPort {
    pub snooze_rx: mpsc::Receiver<Snooze>,
    pub snooze_outcome_tx: mpsc::Sender<SnoozeOutcome>,
}
impl SnoozeEffectPort {
    async fn recv(&mut self) -> Option<Snooze> {
        self.snooze_rx.recv().await
    }

    async fn send(&self, outgoing: SnoozeOutcome) -> Result<(), SendError<SnoozeOutcome>> {
        self.snooze_outcome_tx.send(outgoing).await
    }
}

/// Run the snooze scheduler event loop.
///
/// Accepts [`Snooze`] commands from `port` and maintains a [`DelayQueue`] of per-key timers.
/// When a timer fires, emits [`SnoozeOutcome::Expired`] so the workflow handler can synthesise a
/// wake event for the coordinator.
///
/// The loop exits when the workflow handler drops its sender half (i.e. the service is shutting
/// down) or when the outcome channel is closed.
pub async fn run_snooze_scheduler(mut port: SnoozeEffectPort, metrics: Metrics) {
    let mut registry: HashMap<AlarmKey, DelayTaskKey> = HashMap::new();
    let mut delay_queue = DelayQueue::new();

    loop {
        tokio::select! {
            Some(snooze) = port.recv() => {
                if handle_snooze_input(snooze, &mut delay_queue, &mut registry, &port, &metrics).await.is_err() {
                    // Coordinator has shut down
                    break;
                }
            },
            Some(expired) = delay_queue.next(), if !delay_queue.is_empty() => {
                let key = expired.into_inner();
                registry.remove(&key);
                metrics.record_snooze_expiration();
                if port.send(SnoozeOutcome::Expired { key }).await.is_err() {
                    // Coordinator has shut down
                    break;
                }
            },

            // Coordinator has shut down
            else => break,
        }
    }
}

async fn handle_snooze_input(
    snooze: Snooze,
    delay_queue: &mut DelayQueue<AlarmKey>,
    registry: &mut HashMap<AlarmKey, DelayTaskKey>,
    port: &SnoozeEffectPort,
    metrics: &Metrics,
) -> Result<(), SendError<SnoozeOutcome>> {
    let outcome = match snooze {
        Snooze::Set { key, wake } => match timestamp_secs_to_instant(wake) {
            Some(instant) => {
                if let Some(queue_key) = registry.get(&key) {
                    // Replacing an existing timer: the in-flight count stays the same
                    // (one timer out, one timer in), so we do not adjust the gauge here.
                    delay_queue.reset_at(queue_key, instant);
                } else {
                    let queue_key = delay_queue.insert_at(key.clone(), instant);
                    registry.insert(key.clone(), queue_key);
                    // New timer inserted — record_snooze_set increments in_flight_snooze_timers.
                    metrics.record_snooze_set();
                }
                SnoozeOutcome::Accepted { key }
            }
            None => {
                metrics.record_snooze_invalid_wake();
                SnoozeOutcome::InvalidWake { key }
            }
        },
        Snooze::Cancel { key } => {
            if let Some(queue_key) = registry.remove(&key) {
                delay_queue.remove(&queue_key);
                metrics.record_snooze_cancel();
            }
            SnoozeOutcome::Accepted { key }
        }
    };
    port.send(outcome).await
}

/// Maximum delay that `tokio`'s `DelayQueue` can safely handle.
///
/// `tokio::time` internally uses a wheel with a finite horizon. Capping at 1 year
/// (31_536_000 seconds) keeps the deadline well within the safe range while still
/// supporting any practically meaningful snooze wake time.
const MAX_DELAY: Duration = Duration::from_hours(365 * 24);

/// Convert a protobuf [`Timestamp`] (seconds since Unix epoch) to a `tokio::time::Instant`.
///
/// Returns `None` if the timestamp is in the past, out of the representable range for
/// [`DateTime`], or would produce a zero/negative duration — any of which would cause the
/// `DelayQueue` to fire immediately rather than at the intended future time.  The caller should
/// treat `None` as an invalid wake time and respond with [`SnoozeOutcome::InvalidWake`].
///
/// The resulting instant is capped at [`MAX_DELAY`] from now to keep the deadline within
/// `tokio`'s safe scheduling horizon.
fn timestamp_secs_to_instant(wake: Timestamp) -> Option<Instant> {
    DateTime::<Utc>::from_timestamp_secs(wake.seconds).and_then(|target_time| {
        let current_time = Utc::now();
        // A past or present timestamp is not a valid future wake time — return None so
        // the caller sends SnoozeOutcome::InvalidWake instead of firing immediately.
        let duration = target_time
            .signed_duration_since(current_time)
            .to_std()
            .ok()?;
        // Cap the delay so tokio's DelayQueue never receives a deadline too far in the future.
        Some(Instant::now() + duration.min(MAX_DELAY))
    })
}
