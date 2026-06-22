//! Policy helpers for automated alarm transitions and user-requested actions.

use tracing::{debug, info};

use crate::{
    model::{key::Key, user_action::UserAction},
    proto::{
        common::alarm::{
            Status,
            status::{Source, State},
        },
        google::protobuf::Timestamp,
    },
};

#[cfg(test)]
mod tests;

/// Returns whether an automated status update should be published.
pub fn should_publish(prev: Option<&Status>, key: &Key, alarm: &Status) -> bool {
    let next_state = alarm.state();
    if is_bypassed(prev) {
        if alarm.source() == Source::Epics && next_state != State::Unbypassed {
            debug!(
                target = "alarm_transition",
                device = %key.device,
                source = ?key.source,
                "Skipping alarm due to source-specific bypass"
            );
            return false;
        }
        info!(
            target = "alarm_transition",
            device = %key.device,
            source = ?key.source,
            "Non-Bypass state received from automated source for Bypassed alarm."
        );
    }

    let next_severity = alarm.severity();

    let changed = prev.is_none_or(|prev_status| {
        transition_allowed(prev_status.state(), next_state)
            || prev_status.severity() != next_severity
    });

    let debug_text = if changed {
        "Alarm state transition detected"
    } else {
        "Duplicate or non-actionable transition skipped"
    };
    debug!(
        target = "alarm_transition",
        device = %key.device,
        source = ?key.source,
        previous = ?prev.map(|s| (s.state(), s.severity())),
        current = ?(next_state, next_severity),
        "{debug_text}"
    );

    changed
}

/// Returns whether a user action is allowed from the latest known state.
pub fn user_action_allowed(
    latest_state: State,
    latest_wake: Option<Timestamp>,
    action: &UserAction,
) -> bool {
    match action {
        UserAction::Acknowledge => matches!(latest_state, State::Alarmed | State::Latched),
        UserAction::Activate => latest_state == State::Bypassed,
        UserAction::Bypass(user_wake) => {
            latest_state != State::Bypassed || latest_wake != *user_wake
        }
    }
}

/// Returns whether the latest known state is bypassed for the source-specific key.
fn is_bypassed(prev: Option<&Status>) -> bool {
    prev.is_some_and(|prev_status| prev_status.state() == State::Bypassed)
}

/// Returns whether an automated transition is meaningful enough to publish.
fn transition_allowed(prev: State, next: State) -> bool {
    matches!(
        (prev, next),
        (State::Ok, State::Alarmed)
            | (State::Alarmed, State::Latched)
            | (State::Alarmed, State::Ok)
            | (State::Acknowledged, State::Ok)
            | (State::Acknowledged, State::Alarmed)
            | (State::Bypassed, _)
            | (State::Unknown, _)
    )
}
