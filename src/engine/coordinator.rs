//! Alarm-state coordination for snapshots, user actions, and publish reconciliation.

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

use crate::{
    engine::{
        messages::{
            AlarmsCache, AlarmsSnapshot, Confirmation, CoordinatorMessage, DomainEffect,
            DomainInput, EffectResult,
        },
        policy::{should_publish, user_action_allowed},
    },
    model::{
        errors::{StateTransition, UpdateError},
        key::Key,
        publish::{Publish, PublishAttempt, PublishDetails, PublishOutcome},
        user_action::UserAction,
    },
    proto::common::alarm::{Status, status::State},
};

#[cfg(test)]
mod tests;

const DEFAULT_HYDRATED_ID: u64 = 0;
const INITIAL_PUBLISH_ID: u64 = 1;

/// Owns alarm-state decisions and reconciles publish outcomes back into domain state.
///
/// The coordinator is the semantic authority for the alarm domain:
///
/// - it decides whether an input changes the latest desired state for a [`Key`]
/// - it assigns a monotonically increasing publish id to each emitted effect
/// - it tracks speculative state until the publish engine reports an outcome
/// - it interprets publish outcomes by id so stale transport results do not replace newer intent
///
/// The publish engine is responsible for transport freshness and retry behavior. The coordinator
/// remains responsible for deciding what the service should say now and for reconciling returned
/// outcomes against the current speculative and confirmed state caches.
///
/// ## Invariants
///
/// - only the coordinator mutates `confirmed_state`, `speculative_state`, and
///   `pending_confirmations`
/// - `id_counter` is monotonic within a coordinator instance, and every emitted effect carries the
///   id allocated for that decision
/// - `speculative_state` holds at most one latest desired status per [`Key`], paired with the id of
///   the effect expected to confirm or fail that intent
/// - `confirmed_state` only advances when a returned success has an id newer than the last confirmed
///   id for that [`Key`]
/// - a stale success may still advance `confirmed_state`, but it must not clear newer speculative
///   intent for the same [`Key`]
/// - a stale failure must not remove newer speculative intent for the same [`Key`]
/// - `pending_confirmations` is keyed by publish id, so a user confirmation resolves only when the
///   matching effect result is reconciled
/// - snapshots are built from `confirmed_state` only; speculative intent is not exposed as confirmed
///   operator-visible state
pub struct AlarmStateCoordinator {
    confirmed_state: AlarmsCache,
    speculative_state: AlarmsCache,
    pending_confirmations: HashMap<u64, Confirmation>,
    automated_rx: mpsc::Receiver<CoordinatorMessage>,
    priority_rx: mpsc::Receiver<CoordinatorMessage>,
    effect_tx: mpsc::Sender<DomainEffect>,
    id_counter: u64,
}

impl AlarmStateCoordinator {
    /// Creates a coordinator with separate automated and priority ingress channels.
    ///
    /// Hydrated statuses seed `confirmed_state` with the reserved baseline id so
    /// the first live publish id remains newer than any startup-restored entry.
    pub fn new(
        automated_rx: mpsc::Receiver<CoordinatorMessage>,
        priority_rx: mpsc::Receiver<CoordinatorMessage>,
        effect_tx: mpsc::Sender<DomainEffect>,
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
            priority_rx,
            effect_tx,
            id_counter: INITIAL_PUBLISH_ID,
        }
    }

    /// Runs the coordinator event loop.
    ///
    /// Messages received on the priority channel are handled before automated ingress so operator
    /// requests and publish outcomes are not structurally delayed behind automated storm traffic.
    ///
    /// The coordinator blindly accepts priority traffic until the channel is empty. This is deliberate
    /// so that user latency and speculative state are minimized.
    ///
    /// Priority traffic includes both user commands and effect results. This means the coordinator
    /// prefers finishing the control loop for already-started work before consuming more automated
    /// ingress. The intended guarantee is structural priority, not strict fairness.
    pub async fn start(mut self) {
        loop {
            tokio::select! {
                biased;

                message = self.priority_rx.recv() => {
                    match message {
                        Some(message) => self.handle_message(message).await,
                        None if self.automated_rx.is_closed() => break,
                        None => {}
                    }
                }
                message = self.automated_rx.recv() => {
                    match message {
                        Some(message) => self.handle_message(message).await,
                        None if self.priority_rx.is_closed() => break,
                        None => {}
                    }
                }
            }
        }
    }

    async fn handle_message(&mut self, message: CoordinatorMessage) {
        match message {
            CoordinatorMessage::DomainInput(input) => match input {
                DomainInput::SnapshotRequest(sender) => self.handle_snapshot(sender),
                DomainInput::AutomatedUpdate(status) => self.handle_update(status).await,
                DomainInput::UserUpdate {
                    key,
                    action,
                    user,
                    confirmation,
                } => {
                    self.handle_user_update(key, action, user, confirmation)
                        .await
                }
            },
            CoordinatorMessage::EffectResult(EffectResult::Publish(outcome)) => {
                self.handle_publish_outcome(outcome).await
            }
        }
    }

    /// Returns the current confirmed snapshot to a waiting requester.
    fn handle_snapshot(&self, sender: oneshot::Sender<AlarmsSnapshot>) {
        let snapshot = build_snapshot(&self.confirmed_state);
        if sender.send(snapshot).is_err() {
            error!("Failed sending alarms snapshot over the oneshot channel.")
        }
    }

    /// Applies an automated status update.
    ///
    /// If the update changes the latest desired state for the key, the coordinator records that
    /// state speculatively and emits a publish effect carrying a new id. That id becomes the
    /// coordinator's reference point for later success or failure reconciliation.
    async fn handle_update(&mut self, status: Status) {
        let key = Key::from(&status);
        let prev = self.get_latest_status(&key);
        if should_publish(prev, &key, &status) {
            let id = self.get_next_id();
            self.speculative_state
                .insert(key.clone(), (status.clone(), id));
            self.emit_effect(DomainEffect::Publish(Publish::Automated(PublishDetails {
                id,
                key,
                status,
            })))
            .await;
        }
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
    ) {
        let latest_status = self.get_latest_status(&key);
        let latest_state = latest_status
            .map(|status| status.state())
            .unwrap_or(State::Unknown);
        let latest_wake = latest_status.and_then(|status| status.wake);

        if user_action_allowed(latest_state, latest_wake, &action) {
            let updated_status = build_user_status(&key, action, user);

            let id = self.get_next_id();
            self.emit_effect(DomainEffect::Publish(Publish::User(PublishDetails {
                id,
                key: key.clone(),
                status: updated_status.clone(),
            })))
            .await;

            self.speculative_state
                .insert(key.clone(), (updated_status, id));
            self.pending_confirmations.insert(id, confirmation);
        } else {
            let _ = confirmation.send(Err(UpdateError::StateNotAllowed(StateTransition {
                key,
                current: latest_state,
                requested: action.as_state(),
            })));
        }
    }

    /// Reconciles publish outcomes returned by the publish engine.
    ///
    /// Outcomes are interpreted against the coordinator's current speculative state. The publish
    /// engine may report results for attempts that have already been superseded by a newer publish
    /// id for the same key; those outcomes are still valid transport history, but the coordinator is
    /// responsible for deciding whether they still affect current domain state.
    async fn handle_publish_outcome(&mut self, outcome: PublishOutcome) {
        match outcome {
            PublishOutcome::Batch(batch) => {
                for result in batch {
                    self.handle_publish_result(result).await;
                }
            }
            PublishOutcome::Single(result) => self.handle_publish_result(result).await,
        }
    }

    async fn handle_publish_result(&mut self, result: Result<PublishAttempt, PublishAttempt>) {
        match result {
            Ok(success) => self.reconcile_publish_success(success),
            Err(failure) => self.reconcile_publish_failure(failure),
        }
    }

    /// Reconciles a successful publish attempt.
    ///
    /// A success only clears speculative state when its id still matches the current speculative id
    /// for the key. Older successes may still be observed after newer intent has been recorded; in
    /// that case they are ignored for speculative-state removal while still being eligible to update
    /// confirmed state if they are newer than the last confirmed publish.
    fn reconcile_publish_success(&mut self, success: PublishAttempt) {
        let details = success.into_request().into_details();
        if let Some((_, spec_id)) = self.speculative_state.get(&details.key)
            && *spec_id == details.id
        {
            self.speculative_state.remove(&details.key);
        }
        if let Some(confirmation) = self.pending_confirmations.remove(&details.id) {
            let _ = confirmation.send(Ok(()));
        }

        if should_commit(&self.confirmed_state, &details) {
            self.confirmed_state
                .insert(details.key, (details.status, details.id));
        }
    }

    /// Reconciles a failed publish attempt.
    ///
    /// Failures are interpreted by comparing the returned id with the current speculative id for the
    /// key. A superseded failure is logged and does not remove newer speculative state. A current
    /// failure removes the speculative entry for that key and resolves any waiting confirmation with
    /// [`UpdateError::KafkaWriteFailed`].
    fn reconcile_publish_failure(&mut self, failure: PublishAttempt) {
        let details = failure.into_request().into_details();
        if is_superseded(&self.speculative_state, &details) {
            warn!(
                "Skipping failed delivery result for {}. It has been superseded by another pending update.\nDropped update: {:?}",
                details.key, details.status
            );
        } else {
            error!(
                "Delivery engine failed to send. Dropping message: {:?}",
                details.status
            );
            self.speculative_state.remove(&details.key);
        }
        if let Some(confirmation) = self.pending_confirmations.remove(&details.id) {
            let _ = confirmation.send(Err(UpdateError::KafkaWriteFailed(details.key)));
        }
    }

    /// Emits a domain effect to the downstream effect pipeline.
    async fn emit_effect(&self, effect: DomainEffect) {
        match effect {
            DomainEffect::Publish(_) => self
                .effect_tx
                .send(effect)
                .await
                .expect("The Kafka pipeline should be running."),
        }
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

/// Returns whether the coordinator has already recorded a newer speculative publish id for the key.
fn is_superseded(speculative_state: &AlarmsCache, details: &PublishDetails) -> bool {
    speculative_state
        .get(&details.key)
        .is_some_and(|(_, spec_id)| *spec_id > details.id)
}

/// Returns whether a publish outcome should advance confirmed state for the key.
fn should_commit(confirmed_state: &AlarmsCache, details: &PublishDetails) -> bool {
    confirmed_state
        .get(&details.key)
        .is_none_or(|(_, last_id)| *last_id < details.id)
}

/// Builds a user-authored status update from a requested action.
fn build_user_status(key: &Key, action: UserAction, user: String) -> Status {
    Status {
        device: key.device.clone(),
        source: key.source as i32,
        state: action.as_state() as i32,
        wake: action.get_wake(),
        user,
        ..Status::default()
    }
}

/// Builds the externally visible snapshot from confirmed state.
fn build_snapshot(cache: &AlarmsCache) -> Vec<Status> {
    cache
        .values()
        .filter_map(|(status, _)| {
            let state = status.state();
            (state != State::Ok && state != State::Unbypassed).then_some(status)
        })
        .cloned()
        .collect()
}
