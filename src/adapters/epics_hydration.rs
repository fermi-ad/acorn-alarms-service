//! Startup hydration for service-managed EPICS bypass and snooze state.

use std::collections::HashMap;

use rust_pubsub_lib::{Message, Snapshot, StringMessage};
use tracing::{error, warn};

use crate::{
    model::key::Key,
    proto::common::alarm::{
        Status,
        status::{Source, State},
    },
    runtime::hydration::{HydratedStatuses, HydrationError},
};

#[cfg(test)]
mod tests;

/// Loads startup hydration state for EPICS records from a generic snapshot backend.
///
/// The snapshot backend is generic so tests can supply a fake implementation of
/// rust-pubsub-lib's `Snapshot` trait while production chooses the concrete
/// backend type at the call site.
pub async fn load_epics_hydration<S: Snapshot>(
    host: String,
    topic: String,
) -> Result<HydratedStatuses, HydrationError> {
    let records = S::get::<StringMessage>(host, topic)
        .await
        .map_err(HydrationError::EpicsSnapshotReadFailed)?;
    Ok(reduce_snapshot(records))
}

fn reduce_snapshot(records: Vec<StringMessage>) -> HydratedStatuses {
    let mut hydrated = HashMap::new();

    for record in records {
        let (key, value) = record.extract_key_value();
        let Some(key_text) = key else {
            warn!(
                target = "startup_hydration",
                "Skipping snapshot record without key"
            );
            continue;
        };

        let key = match Key::try_from(key_text.as_str()) {
            Ok(key) => key,
            Err(err) => {
                warn!(target = "startup_hydration", key = %key_text, error = %err, "Skipping snapshot record with invalid key");
                continue;
            }
        };

        if key.source != Source::Epics {
            continue;
        }

        if value.trim().is_empty() || value.trim().eq_ignore_ascii_case("null") {
            hydrated.remove(&key);
            continue;
        }

        let status: Status = match serde_json::from_str(&value) {
            Ok(status) => status,
            Err(err) => {
                error!(target = "startup_hydration", key = %key, error = %err, "Skipping snapshot record with invalid status payload");
                continue;
            }
        };

        if status.source() != Source::Epics {
            warn!(target = "startup_hydration", key = %key, source = ?status.source(), "Discarding non-EPICS status from EPICS hydration snapshot");
            continue;
        }

        if status.state() == State::Bypassed {
            hydrated.insert(key, status);
        } else {
            hydrated.remove(&key);
        }
    }

    hydrated
}
