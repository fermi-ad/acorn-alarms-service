//! Ingress handles for automated and user-driven coordinator traffic.

use std::{sync::Arc, time::Instant};

use ringmap::RingMap;
use tokio::sync::{
    Mutex, Notify,
    mpsc::{
        self,
        error::{SendError, TrySendError},
    },
};

use crate::{
    engine::messages::DomainInput, metrics::Metrics, model::key::Key, proto::common::alarm::Status,
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum ChannelSendError {
    Closed,
    Full,
}

struct OverloadState {
    overload_mode: bool,
    entered_at: Option<Instant>,
    pending: RingMap<Key, Status>,
}

/// Handle for automated alarm updates entering the coordinator.
#[derive(Clone)]
pub struct AutomatedIngressHandle {
    coordinator_tx: mpsc::Sender<DomainInput>,
    overload_notify: Arc<Notify>,
    overload_state: Arc<Mutex<OverloadState>>,
    metrics: Metrics,
}

impl AutomatedIngressHandle {
    /// Creates a new automated ingress handle and starts the overload drain task.
    pub fn new(tx: mpsc::Sender<DomainInput>, metrics: Metrics) -> Self {
        let overload_tx = tx.clone();

        let overload_notify = Arc::new(Notify::new());
        let notify_handle = Arc::clone(&overload_notify);

        let overload_state = Arc::new(Mutex::new(OverloadState {
            overload_mode: false,
            entered_at: None,
            pending: RingMap::new(),
        }));
        let updates_handle = Arc::clone(&overload_state);

        tokio::spawn(run_overload_drain_loop(
            notify_handle,
            overload_tx,
            updates_handle,
            metrics.clone(),
        ));
        Self {
            coordinator_tx: tx,
            overload_notify,
            overload_state,
            metrics,
        }
    }

    /// Sends an automated update into the coordinator ingress.
    ///
    /// Automated updates use a two-mode overload policy:
    ///
    /// - in normal mode, when no retained backlog exists, the update is offered directly to the
    ///   coordinator queue with [`mpsc::Sender::try_send`]
    /// - if that direct send finds the queue full, ingress enters overload mode, retains the latest
    ///   [`Status`] per [`Key`], and wakes a single background drain task
    /// - while overload mode is active, all subsequent automated updates are merged into the retained
    ///   latest-by-key map instead of competing for queue slots directly
    /// - overload mode ends only after the drain task has emptied the retained backlog
    ///
    /// This keeps the common case cheap while ensuring that, under pressure, automated dispatch is
    /// owned by one drain loop and intermediate automated transitions may be coalesced away.
    /// User commands are not coalesced.
    ///
    /// Overload policy: bounded-and-await after coalescing. Callers should await this send so Redis
    /// ingestion slows down instead of buffering unbounded work in memory.
    pub async fn send_automated_update(&self, status: Status) -> Result<(), SendError<Status>> {
        let key = Key::from(&status);

        let mut overload_state = self.overload_state.lock().await;
        if overload_state.overload_mode {
            overload_state.pending.insert(key, status);
            self.metrics
                .record_retained_automated_keys(overload_state.pending.len());
            return Ok(());
        }

        drop(overload_state);
        let message = DomainInput::AutomatedUpdate(status.clone());
        match self.coordinator_tx.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Closed(_)) => Err(SendError(status)),
            Err(TrySendError::Full(_)) => {
                self.metrics.record_automated_queue_full();
                let mut overload_state = self.overload_state.lock().await;
                overload_state.pending.insert(key, status);
                overload_state.overload_mode = true;
                overload_state.entered_at = Some(Instant::now());
                self.metrics
                    .record_overload_entry(overload_state.pending.len());
                self.overload_notify.notify_one();
                Ok(())
            }
        }
    }
}

/// Drains retained automated updates into the coordinator while overload mode is active.
///
/// This task is the sole automated dispatcher during overload mode. It removes the oldest retained
/// entry from the pending map, awaits queue capacity, and sends that retained [`Status`] into the
/// coordinator. Newer updates for the same [`Key`] that arrive while a send is in flight are
/// reinserted into the pending map and will be sent on a later pass.
async fn run_overload_drain_loop(
    notify: Arc<Notify>,
    coordinator_tx: mpsc::Sender<DomainInput>,
    overload_state: Arc<Mutex<OverloadState>>,
    metrics: Metrics,
) {
    loop {
        notify.notified().await;

        loop {
            let mut overload_state = overload_state.lock().await;
            let next = overload_state.pending.pop_front();
            metrics.record_retained_automated_keys(overload_state.pending.len());
            if let Some((_, status)) = next {
                drop(overload_state);
                let message = DomainInput::AutomatedUpdate(status);
                let result = coordinator_tx.send(message).await;
                if result.is_err() {
                    // The coordinator has shut down!
                    return;
                }
            } else {
                // No more pending updates, go back to waiting for the next overload.
                overload_state.overload_mode = false;
                if let Some(entered_at) = overload_state.entered_at.take() {
                    metrics.record_overload_exit(entered_at.elapsed());
                }
                break;
            }
        }
    }
}

/// Handle for user-driven commands entering the coordinator.
#[derive(Clone)]
pub struct UserIngressHandle {
    tx: mpsc::Sender<DomainInput>,
    metrics: Metrics,
}

impl UserIngressHandle {
    /// Creates a new user ingress handle.
    pub fn new(tx: mpsc::Sender<DomainInput>, metrics: Metrics) -> Self {
        Self { tx, metrics }
    }

    /// Attempts to enqueue a user command without waiting.
    ///
    /// Overload policy: bounded-and-reject. gRPC handlers use [`UserIngressHandle::try_send`] so
    /// callers receive an explicit overload error instead of waiting behind storm traffic.
    pub fn try_send(&self, message: DomainInput) -> Result<(), ChannelSendError> {
        self.tx.try_send(message).map_err(|e| match e {
            TrySendError::Closed(_) => ChannelSendError::Closed,
            TrySendError::Full(_) => {
                self.metrics.record_user_queue_full_rejection();
                ChannelSendError::Full
            }
        })
    }
}
