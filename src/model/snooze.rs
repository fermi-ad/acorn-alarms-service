//! Snooze command and outcome types for the snooze scheduler protocol.

use crate::{model::key::Key, proto::google::protobuf::Timestamp};

/// A command sent to the snooze scheduler.
pub enum Snooze {
    /// Register (or replace) a timer for `key` that fires at the `wake` timestamp.
    Set { key: Key, wake: Timestamp },
    /// Remove any existing timer for `key`. No-op if no timer exists for the key.
    Cancel { key: Key },
}

/// The outcome of a [`Snooze`] command, or a spontaneous timer expiry notification.
pub enum SnoozeOutcome {
    /// A previously registered timer has elapsed.
    Expired { key: Key },
    /// A `Snooze::Set` was rejected because the wake timestamp is not a valid future time.
    InvalidWake { key: Key },
    /// The command was accepted.
    Accepted { key: Key },
}
