//! Error types for model-level alarm update operations.

use std::{error::Error, fmt};

use crate::{model::key::Key, proto::common::alarm::status::State};

#[cfg(test)]
mod tests;

/// Describes an attempted transition from one alarm state to another.
#[derive(Debug)]
pub struct StateTransition {
    /// The alarm being updated.
    pub key: Key,
    /// The alarm state before the requested change.
    pub current: State,
    /// The alarm state requested by the caller.
    pub requested: State,
}

impl fmt::Display for StateTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "for {} from {:?} into {:?}",
            self.key, self.current, self.requested
        )
    }
}

/// Errors that can occur while applying or publishing an alarm update.
#[derive(Debug)]
pub enum UpdateError {
    /// Writing the update to Kafka failed for the given alarm key.
    KafkaWriteFailed(Key),
    /// The requested state transition is not allowed.
    StateNotAllowed(StateTransition),
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdateError::KafkaWriteFailed(key) => {
                write!(f, "Failed writing update for {key} to Kafka.")
            }
            UpdateError::StateNotAllowed(state) => {
                write!(f, "Invalid state transition {state} requested.")
            }
        }
    }
}

impl Error for UpdateError {}
