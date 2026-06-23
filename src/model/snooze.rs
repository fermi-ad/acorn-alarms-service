//! `SnoozeInput`, `Snooze`, and `SnoozeOutcome` types for the snooze scheduler protocol.
//!
//! [`SnoozeInput`] is the external command type carried over the channel from the workflow handler
//! to the snooze scheduler. The scheduler converts it to a [`Snooze`] decision internally before
//! acting on it. [`SnoozeOutcome`] is returned for every command and emitted spontaneously when a
//! timer fires.

use crate::{model::key::Key, proto::google::protobuf::Timestamp};

/// A command sent to the snooze scheduler.
pub struct SnoozeInput {
    pub key: Key,
    pub wake: Option<Timestamp>,
}

/// The action taken by the scheduler. Derived from [`SnoozeInput`].
pub enum Snooze {
    /// Register (or replace) a timer for `key` that fires at the `wake` timestamp.
    Set { key: Key, wake: Timestamp },
    /// Remove any existing timer for `key`. No-op if no timer exists for the key.
    Cancel { key: Key },
}
impl From<SnoozeInput> for Snooze {
    fn from(value: SnoozeInput) -> Self {
        match value.wake {
            Some(timestamp) => Self::Set {
                key: value.key,
                wake: timestamp,
            },
            None => Self::Cancel { key: value.key },
        }
    }
}

/// The outcome of a [`Snooze`] command, or a spontaneous timer expiry notification.
pub enum SnoozeOutcome {
    /// A previously registered timer has elapsed.
    Expired { key: Key },
    /// A `SnoozeInput` with a `wake` timestamp that is not a valid future time was rejected.
    InvalidWake { key: Key },
    /// The command was accepted.
    Accepted { key: Key },
}
