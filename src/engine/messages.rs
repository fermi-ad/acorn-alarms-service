//! Domain input types accepted by the coordinator from ingress adapters.
//!
//! This module contains [`DomainInput`] and its supporting types. These are the only messages
//! that flow from the ingress layer into the coordinator; effect results and job outcomes travel
//! on separate channels and are not part of this module.

use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::{
    model::{errors::UpdateError, key::Key, user_action::UserAction},
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
