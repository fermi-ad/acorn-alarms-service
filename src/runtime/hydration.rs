//! Startup hydration helpers for seeding confirmed coordinator state.
//!
//! This module owns the startup-only assembly step that gathers confirmed alarm
//! state before the runtime begins processing live traffic. The current
//! implementation restores EPICS bypass state from a snapshot backend and merges
//! loader outputs through a shared contract so future sources can contribute
//! disjoint keyspaces without changing coordinator startup semantics.

use std::{collections::HashMap, error::Error, fmt::Display};

use rust_pubsub_lib::{PubSubError, Snapshot};

use crate::{
    adapters::epics_hydration::load_epics_hydration, model::key::Key, proto::common::alarm::Status,
};

#[cfg(test)]
mod tests;

/// Initial confirmed statuses assembled during startup hydration.
pub type HydratedStatuses = HashMap<Key, Status>;

#[derive(Debug)]
pub enum HydrationError {
    SnapshotReadFailed(PubSubError),
    KeyInMultipleSources(Key),
}
impl Display for HydrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HydrationError::KeyInMultipleSources(key) => write!(
                f,
                "Encountered identical key from more than one hydration source: {key}"
            ),
            HydrationError::SnapshotReadFailed(source) => {
                write!(f, "Failed reading snapshot. Cause: {source}")
            }
        }
    }
}
impl Error for HydrationError {}

/// Loads the startup hydration state required to seed the coordinator.
///
/// Startup hydration runs before live adapters begin sending traffic. The
/// current implementation restores EPICS bypass state from the configured
/// snapshot backend, then merges additional loader outputs through the same
/// contract when they become available.
pub async fn load_startup_hydration<S: Snapshot>(
    epics_host: String,
    epics_topic: String,
) -> Result<HydratedStatuses, HydrationError> {
    let mut epics_statuses = load_epics_hydration::<S>(epics_host, epics_topic).await?;
    let placeholder_acnet_statuses = HashMap::new();
    merge_hydrated_statuses(&mut epics_statuses, placeholder_acnet_statuses)?;
    Ok(epics_statuses)
}

/// Merges one loader result into the accumulated startup hydration map.
///
/// Hydration sources are expected to own disjoint keyspaces. If two loaders
/// attempt to seed the same [`Key`], startup fails loudly so source ownership
/// remains explicit and no precedence rule is applied implicitly.
fn merge_hydrated_statuses(
    accumulated: &mut HydratedStatuses,
    incoming: HydratedStatuses,
) -> Result<(), HydrationError> {
    for (key, status) in incoming {
        if accumulated.contains_key(&key) {
            return Err(HydrationError::KeyInMultipleSources(key));
        }
        accumulated.insert(key, status);
    }

    Ok(())
}
