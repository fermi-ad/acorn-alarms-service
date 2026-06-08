//! User-initiated alarm actions and their state transitions.

use crate::proto::{common::alarm::status::State, google::protobuf::Timestamp};

#[cfg(test)]
mod tests;

/// A user action that changes alarm state.
#[derive(Clone, Copy)]
pub enum UserAction {
    /// Acknowledge the current alarm.
    Acknowledge,
    /// Reactivate a previously bypassed alarm.
    Activate,
    /// Bypass an alarm until an optional wake time.
    Bypass(Option<Timestamp>),
}

impl UserAction {
    /// Returns the alarm state represented by this action.
    pub fn as_state(&self) -> State {
        match self {
            UserAction::Acknowledge => State::Acknowledged,
            UserAction::Activate => State::Unbypassed,
            UserAction::Bypass(_) => State::Bypassed,
        }
    }

    /// Returns the wake timestamp associated with a bypass action, if any.
    pub fn get_wake(&self) -> Option<Timestamp> {
        match self {
            UserAction::Bypass(wake) => *wake,
            _ => None,
        }
    }
}
