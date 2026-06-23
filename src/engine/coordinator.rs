//! Alarm-state coordination for snapshots, user actions, and publish reconciliation.

use std::collections::HashMap;

use chrono::Utc;
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot,
};
use tracing::error;

use crate::{
    engine::{
        messages::{AlarmsCache, AlarmsSnapshot, Confirmation, DomainInput},
        policy::{should_publish, user_action_allowed},
        workflow::{Job, JobOutcome},
    },
    model::{
        errors::{StateTransition, UpdateError},
        key::Key,
        user_action::UserAction,
    },
    proto::{
        common::alarm::{Status, status::State},
        google::protobuf::Timestamp,
    },
};

#[cfg(test)]
mod tests;

const DEFAULT_HYDRATED_ID: u64 = 0;
const INITIAL_PUBLISH_ID: u64 = 1;

const WORKFLOW_HANDLER_STOPPED: &str = "Workflow handler stopped running!";

/// Channel pair owned by the coordinator for communicating with the workflow handler.
///
/// `job_tx` sends new [`Job`] values to the workflow handler; `job_outcome_rx` receives
/// [`JobOutcome`] values back after each job completes its effect pipeline.
pub struct JobPort {
    pub job_tx: mpsc::Sender<Job>,
    pub job_outcome_rx: mpsc::Receiver<JobOutcome>,
}
impl JobPort {
    async fn send(&self, job: Job) -> Result<(), SendError<Job>> {
        self.job_tx.send(job).await
    }

    async fn recv(&mut self) -> Option<JobOutcome> {
        self.job_outcome_rx.recv().await
    }
}

/// Owns alarm-state decisions and reconciles job outcomes back into domain state.
///
/// The coordinator is the semantic authority for the alarm domain:
///
/// - it decides whether an input changes the latest desired state for a [`Key`]
/// - it assigns a monotonically increasing id to each emitted [`Job`]
/// - it tracks speculative state until a [`JobOutcome`] is received
/// - it interprets outcomes by id so stale results do not replace newer intent
///
/// ## Invariants
///
/// - only the coordinator mutates `confirmed_state`, `speculative_state`, and
///   `pending_confirmations`
/// - `id_counter` is monotonic within a coordinator instance, and every emitted [`Job`] carries the
///   id allocated for that decision
/// - `speculative_state` holds at most one latest desired status per [`Key`], paired with the id of
///   the [`Job`] expected to confirm or fail that intent
/// - `confirmed_state` advances when a [`JobOutcome::Committed`] is received
/// - a stale success must not clear newer speculative intent for the same [`Key`]
/// - a stale failure must not remove newer speculative intent for the same [`Key`]
/// - `pending_confirmations` is keyed by job id, so a user confirmation resolves only when the
///   matching outcome is reconciled
/// - snapshots are built from `confirmed_state` only; speculative intent is not exposed as confirmed
///   operator-visible state
pub struct AlarmStateCoordinator {
    confirmed_state: AlarmsCache,
    speculative_state: AlarmsCache,
    pending_confirmations: HashMap<u64, Confirmation>,
    automated_rx: mpsc::Receiver<DomainInput>,
    user_rx: mpsc::Receiver<DomainInput>,
    job_port: JobPort,
    id_counter: u64,
}

impl AlarmStateCoordinator {
    /// Creates a coordinator with separate automated and priority ingress channels.
    ///
    /// Hydrated statuses seed `confirmed_state` with the reserved baseline id so
    /// the first live publish id remains newer than any startup-restored entry.
    pub fn new(
        automated_rx: mpsc::Receiver<DomainInput>,
        user_rx: mpsc::Receiver<DomainInput>,
        job_port: JobPort,
        hydrated_statuses: HashMap<Key, Status>,
    ) -> Self {
        Self {
            confirmed_state: hydrated_statuses
                .into_iter()
                .map(|(key, status)| (key, (status, DEFAULT_HYDRATED_ID)))
                .collect(),
            speculative_state: HashMap::new(),
            pending_confirmations: HashMap::new(),
            automated_rx,
            user_rx,
            job_port,
            id_counter: INITIAL_PUBLISH_ID,
        }
    }

    /// Runs the coordinator event loop.
    ///
    /// The biased `tokio::select!` gives the highest priority to [`JobOutcome`] messages from the
    /// workflow handler, then to user commands, and finally to automated ingress. This ensures that
    /// publish outcomes and snooze wake events are reconciled promptly, user latency is minimized,
    /// and automated backlog does not structurally delay operator requests.
    ///
    /// The loop exits when all input channels are closed, at which point any pending user
    /// confirmations are failed with [`UpdateError::Internal`].
    pub async fn start(mut self) {
        loop {
            let outcome = tokio::select! {
                biased;

                Some(job_outcome) = self.job_port.recv() => {
                    self.reconcile_job_outcome(job_outcome).await
                }

                Some(message) = self.user_rx.recv() => self.handle_message(message).await,

                Some(message) = self.automated_rx.recv() => self.handle_message(message).await,

                else => Err("All channels dropped")
            };
            if let Err(cause) = outcome {
                error!("Unrecoverable: {cause}");
                self.fail_all_confirmations();
                break;
            }
        }
    }

    async fn handle_message(&mut self, message: DomainInput) -> Result<(), &'static str> {
        match message {
            DomainInput::SnapshotRequest(sender) => self.handle_snapshot(sender),
            DomainInput::AutomatedUpdate(status) => self.handle_automated_update(status).await,
            DomainInput::UserUpdate {
                key,
                action,
                user,
                confirmation,
            } => {
                self.handle_user_update(key, action, user, confirmation)
                    .await
            }
        }
    }

    /// Returns the current confirmed snapshot to a waiting requester.
    fn handle_snapshot(&self, sender: oneshot::Sender<AlarmsSnapshot>) -> Result<(), &'static str> {
        let snapshot = self
            .confirmed_state
            .values()
            .filter_map(|(status, _)| {
                if matches!(status.state(), State::Ok | State::Unbypassed) {
                    None
                } else {
                    Some(status)
                }
            })
            .cloned()
            .collect();
        if sender.send(snapshot).is_err() {
            error!("Failed sending alarms snapshot over the oneshot channel.")
        }
        Ok(())
    }

    /// Applies an automated status update.
    ///
    /// If the update changes the latest desired state for the key, the coordinator records that
    /// state speculatively and emits a [`Job`] carrying a new id. That id becomes the
    /// coordinator's reference point for later success or failure reconciliation.
    ///
    /// If a job is already in-flight for the key (speculative state is already set), the
    /// speculative state is updated but no new job is dispatched. A new job will be dispatched
    /// once the current one completes.
    async fn handle_automated_update(&mut self, status: Status) -> Result<(), &'static str> {
        let key = Key::from(&status);
        let prev = self.get_latest_status(&key);
        if should_publish(prev, &key, &status) {
            let id = self.get_next_id();
            let in_flight = self.speculative_state.insert(key.clone(), (status, id));
            if in_flight.is_none() {
                self.send_next_for(&key).await?;
            }
        }
        Ok(())
    }

    /// Applies a user-requested state transition.
    ///
    /// Allowed user actions become the latest desired state for the key immediately in
    /// `speculative_state`, and the coordinator emits a publish effect with a new id. The
    /// confirmation sender is retained until a publish outcome for that id is reconciled.
    async fn handle_user_update(
        &mut self,
        key: Key,
        action: UserAction,
        user: String,
        confirmation: Confirmation,
    ) -> Result<(), &'static str> {
        let latest_status = self.get_latest_status(&key);
        let latest_state = latest_status
            .map(|status| status.state())
            .unwrap_or(State::Unknown);
        let latest_wake = latest_status.and_then(|status| status.wake);

        if user_action_allowed(latest_state, latest_wake, &action) {
            let updated_status = build_user_status(&key, action, user);

            let id = self.get_next_id();
            let _ = self.pending_confirmations.insert(id, confirmation);
            let in_flight = self
                .speculative_state
                .insert(key.clone(), (updated_status.clone(), id));
            if in_flight.is_none() {
                self.send_next_for(&key).await?
            }
        } else {
            let _ = confirmation.send(Err(UpdateError::StateNotAllowed(StateTransition {
                key,
                current: latest_state,
                requested: action.as_state(),
            })));
        }
        Ok(())
    }

    /// Dispatches the next job for `key` if speculative state is still pending for it.
    ///
    /// Called after a job completes (committed or failed) to pick up any newer speculative intent
    /// that arrived while the previous job was in-flight. If speculative state has been cleared
    /// (i.e. the last committed job was the latest), this is a no-op.
    async fn send_next_for(&self, key: &Key) -> Result<(), &'static str> {
        if let Some((status, id)) = self.speculative_state.get(key) {
            self.job_port
                .send(Job {
                    id: *id,
                    key: key.clone(),
                    status: status.clone(),
                    user_initiated: self.pending_confirmations.contains_key(id),
                })
                .await
                .map_err(|_| WORKFLOW_HANDLER_STOPPED)?;
        }
        Ok(())
    }

    /// Fails every pending user confirmation with [`UpdateError::Internal`].
    ///
    /// Called when the coordinator is shutting down (all channels closed) so that waiting gRPC
    /// callers receive an error rather than hanging indefinitely.
    fn fail_all_confirmations(&mut self) {
        for (id, confirmation) in self.pending_confirmations.drain() {
            if let Some(key) = self
                .speculative_state
                .iter()
                .find_map(|(key, (_, mapped_id))| (*mapped_id == id).then_some(key))
            {
                let _ = confirmation.send(Err(UpdateError::Internal(key.clone())));
            }
        }
    }

    async fn reconcile_job_outcome(&mut self, outcome: JobOutcome) -> Result<(), &'static str> {
        match outcome {
            JobOutcome::Committed(job) => self.reconcile_success(job).await,
            JobOutcome::Failed(job) => self.reconcile_failure(job).await,
            JobOutcome::Wake(key) => self.dispatch_automated_wake(key).await,
        }
    }

    /// Synthesizes an automated `Unbypassed` update for `key` and re-enters the coordination loop.
    ///
    /// The synthesized status uses the current wall-clock time and carries no user field.
    async fn dispatch_automated_wake(&mut self, key: Key) -> Result<(), &'static str> {
        self.handle_automated_update(Status {
            device: key.device,
            source: key.source as i32,
            state: State::Unbypassed as i32,
            time: Some(Timestamp {
                seconds: Utc::now().timestamp(),
                nanos: 0,
            }),
            ..Status::default()
        })
        .await
    }

    /// Reconciles a failed job outcome.
    ///
    /// Resolves any pending user confirmation with [`UpdateError::Internal`]. For user-initiated
    /// jobs whose id still matches the current speculative id, rolls speculative state back to the
    /// last confirmed state before dispatching the next job. For automated failures, speculative
    /// state is left in place so the next dispatch picks up the latest intent.
    async fn reconcile_failure(&mut self, job: Job) -> Result<(), &'static str> {
        let key = &job.key;
        if let Some(confirmation) = self.pending_confirmations.remove(&job.id) {
            let _ = confirmation.send(Err(UpdateError::Internal(key.clone())));
        }

        let (_, spec_id) = self
            .speculative_state
            .get(key)
            .ok_or("Lost state for job")?;
        if *spec_id == job.id && job.user_initiated {
            self.converge_to_last_confirmed(key);
        }
        self.send_next_for(key).await
    }

    /// Rolls speculative state back to the last confirmed state for `key`.
    ///
    /// Used after a user-initiated job fails: the speculative intent that was never persisted is
    /// replaced with the last known-good confirmed state (or a default status if no confirmed
    /// state exists). This prevents a failed user action from leaving stale speculative state
    /// that would block future updates for the key.
    fn converge_to_last_confirmed(&mut self, key: &Key) {
        let confirmed_entry = self.confirmed_state.get(key).cloned().unwrap_or_else(|| {
            let status = Status {
                device: key.device.clone(),
                source: key.source as i32,
                ..Default::default()
            };
            (status, DEFAULT_HYDRATED_ID)
        });
        self.speculative_state.insert(key.clone(), confirmed_entry);
    }

    /// Reconciles a successful job outcome.
    ///
    /// Always advances `confirmed_state` for the key. Clears `speculative_state` only when the
    /// committed id still matches the current speculative id — if newer intent has already been
    /// recorded, the speculative entry is left in place and `send_next_for` is called to dispatch
    /// the next job immediately. Resolves any pending user confirmation with `Ok(())`.
    async fn reconcile_success(&mut self, success: Job) -> Result<(), &'static str> {
        if let Some(confirmation) = self.pending_confirmations.remove(&success.id) {
            let _ = confirmation.send(Ok(()));
        }

        if let Some((_, spec_id)) = self.speculative_state.get(&success.key) {
            if *spec_id == success.id {
                self.speculative_state.remove(&success.key);
            } else {
                self.send_next_for(&success.key).await?;
            }
        }

        self.confirmed_state
            .insert(success.key, (success.status, success.id));
        Ok(())
    }

    /// Returns the latest known status, preferring speculative state over confirmed state.
    fn get_latest_status(&self, key: &Key) -> Option<&Status> {
        self.speculative_state
            .get(key)
            .or_else(|| self.confirmed_state.get(key))
            .map(|(status, _)| status)
    }

    /// Allocates the next monotonically increasing publish id.
    fn get_next_id(&mut self) -> u64 {
        let id = self.id_counter;
        self.id_counter += 1;
        id
    }
}

/// Builds a user-authored status update from a requested action.
fn build_user_status(key: &Key, action: UserAction, user: String) -> Status {
    Status {
        device: key.device.clone(),
        source: key.source as i32,
        state: action.as_state() as i32,
        wake: action.get_wake(),
        user,
        time: Some(Timestamp {
            seconds: Utc::now().timestamp(),
            nanos: 0,
        }),
        ..Status::default()
    }
}
