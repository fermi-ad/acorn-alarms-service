//! Shared message types exchanged between engine ingress, coordination, and effects.

use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::{
    model::{
        errors::UpdateError,
        key::Key,
        publish::{Publish, PublishOutcome},
        user_action::UserAction,
    },
    proto::common::alarm::Status,
};

/// Cached alarm state keyed by device and source, paired with the publish id that produced it.
pub type AlarmsCache = HashMap<Key, (Status, u64)>;

/// Snapshot payload returned to snapshot requesters.
pub type AlarmsSnapshot = Vec<Status>;

/// One-shot confirmation channel for user-requested updates.
pub type Confirmation = oneshot::Sender<Result<(), UpdateError>>;

/// Domain inputs accepted by the coordinator.
pub enum DomainInput {
    SnapshotRequest(oneshot::Sender<AlarmsSnapshot>),
    AutomatedUpdate(Status),
    UserUpdate {
        key: Key,
        action: UserAction,
        user: String,
        confirmation: Confirmation,
    },
}

/// Effects emitted by the coordinator for downstream execution.
pub enum DomainEffect {
    Publish(Publish),
}

/// Results returned from effect execution back into the coordinator.
pub enum EffectResult {
    Publish(PublishOutcome),
}

/// Top-level messages routed into the coordinator.
pub enum CoordinatorMessage {
    DomainInput(DomainInput),
    EffectResult(EffectResult),
}
