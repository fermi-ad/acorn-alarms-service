//! Startup hydration helpers for seeding confirmed coordinator state.
//!
//! This module owns the startup-only assembly step that gathers confirmed alarm
//! state before the runtime begins processing live traffic.
/// ACNET initial state is recovered from the AEOLUS gRPC proxy.
/// EPICS bypass state is restored from the configured snapshot backend.
/// The final hydrated state is the merger of these two sources.
use std::{collections::HashMap, error::Error, fmt::Display};

use rust_pubsub_lib::{PubSubError, Snapshot};
use tonic::{Status as TonicStatus, transport::Error as TonicError};

use crate::{
    adapters::{acnet_hydration::load_acnet_hydration, epics_hydration::load_epics_hydration},
    model::key::Key,
    proto::common::alarm::Status,
};

#[cfg(test)]
mod tests;

/// Initial confirmed statuses assembled during startup hydration.
pub type HydratedStatuses = HashMap<Key, Status>;

#[derive(Debug)]
pub enum HydrationError {
    AcnetSnapshotReadFailed(TonicStatus),
    AeolusProxyConnectionFailed(TonicError),
    EpicsSnapshotReadFailed(PubSubError),
    KeyInMultipleSources(Key),
}
impl Display for HydrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HydrationError::AcnetSnapshotReadFailed(status) => {
                write!(f, "Failed reading ACNET snapshot. Cause: {status}")
            }
            HydrationError::AeolusProxyConnectionFailed(source) => {
                write!(f, "Failed contacting AEOLUS proxy. Cause: {source}")
            }
            HydrationError::KeyInMultipleSources(key) => write!(
                f,
                "Encountered identical key from more than one hydration source: {key}"
            ),
            HydrationError::EpicsSnapshotReadFailed(source) => {
                write!(f, "Failed reading EPICS snapshot. Cause: {source}")
            }
        }
    }
}
impl Error for HydrationError {}

/// Loads the startup hydration state required to seed the coordinator.
///
/// Startup hydration runs before live adapters begin sending traffic.
/// ACNET initial state is recovered from the AEOLUS gRPC proxy.
/// EPICS bypass state is restored from the configured snapshot backend.
/// The final hydrated state is the merger of these two sources.
pub async fn load_startup_hydration<S: Snapshot>(
    acnet_host: String,
    epics_host: String,
    epics_topic: String,
) -> Result<HydratedStatuses, HydrationError> {
    let (acnet_statuses, epics_statuses) = tokio::try_join!(
        load_acnet_hydration(acnet_host),
        load_epics_hydration::<S>(epics_host, epics_topic)
    )?;
    merge_hydrated_statuses(acnet_statuses, epics_statuses)
}

/// Merges one loader result into the accumulated startup hydration map.
///
/// Hydration sources are expected to own disjoint keyspaces. If two loaders
/// attempt to seed the same [`Key`], startup fails loudly so source ownership
/// remains explicit and no precedence rule is applied implicitly.
fn merge_hydrated_statuses(
    mut accumulated: HydratedStatuses,
    incoming: HydratedStatuses,
) -> Result<HydratedStatuses, HydrationError> {
    for (key, status) in incoming {
        if accumulated.contains_key(&key) {
            return Err(HydrationError::KeyInMultipleSources(key));
        }
        accumulated.insert(key, status);
    }

    Ok(accumulated)
}
