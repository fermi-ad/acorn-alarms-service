//! Error types for model-level alarm update operations, and [`SymmetricalResult`].

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
    /// An internal subsystem failure prevented the update from being persisted.
    ///
    /// This covers any failure inside the publish engine, snooze scheduler, or workflow handler
    /// that means the update was not durably written — not just Kafka write failures.
    Internal(Key),
    /// The requested state transition is not allowed.
    StateNotAllowed(StateTransition),
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdateError::Internal(key) => {
                write!(
                    f,
                    "Internal error. The update for {key} has not been persisted."
                )
            }
            UpdateError::StateNotAllowed(state) => {
                write!(f, "Invalid state transition {state} requested.")
            }
        }
    }
}

impl Error for UpdateError {}

/// A result type where both the success and failure paths carry the same inner value.
///
/// Used where `Result<T, T>` would be misleading because there is no distinct error payload —
/// both outcomes carry the same type (e.g. a [`Key`] identifying which alarm was affected).
/// The symmetry is made explicit so callers cannot accidentally treat the `Err` variant as
/// carrying a different type than `Ok`.
pub enum SymmetricalResult<T> {
    /// The operation succeeded; the inner value identifies the subject.
    Ok(T),
    /// The operation failed; the inner value identifies the subject.
    Err(T),
}
impl<T> SymmetricalResult<T> {
    pub fn inner_ref(&self) -> &T {
        match self {
            Self::Err(val) => val,
            Self::Ok(val) => val,
        }
    }

    pub fn into_inner(self) -> T {
        match self {
            Self::Err(val) => val,
            Self::Ok(val) => val,
        }
    }

    pub fn map<U, F: Fn(T) -> U>(self, func: F) -> SymmetricalResult<U> {
        match self {
            Self::Err(val) => SymmetricalResult::Err(func(val)),
            Self::Ok(val) => SymmetricalResult::Ok(func(val)),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok(_))
    }
}
