use super::*;

// ---------------------------------------------------------------------------
// transition_allowed
// ---------------------------------------------------------------------------

#[test]
fn ok_to_alarmed_is_allowed() {
    assert!(transition_allowed(State::Ok, State::Alarmed));
}

#[test]
fn alarmed_to_latched_is_allowed() {
    assert!(transition_allowed(State::Alarmed, State::Latched));
}

#[test]
fn alarmed_to_ok_is_allowed() {
    assert!(transition_allowed(State::Alarmed, State::Ok));
}

#[test]
fn acknowledged_to_ok_is_allowed() {
    assert!(transition_allowed(State::Acknowledged, State::Ok));
}

#[test]
fn acknowledged_to_alarmed_is_allowed() {
    assert!(transition_allowed(State::Acknowledged, State::Alarmed));
}

#[test]
fn ok_to_ok_is_not_allowed() {
    assert!(!transition_allowed(State::Ok, State::Ok));
}

/// `transition_allowed` does not include `Alarmed→Alarmed` — that path is
/// handled separately in `should_publish` via the severity-change branch.
/// A severity escalation (`Low→High`) is still forwarded end-to-end; see
/// `alarmed_to_alarmed_severity_escalation_is_published`.
#[test]
fn alarmed_to_alarmed_not_in_transition_allowed() {
    assert!(!transition_allowed(State::Alarmed, State::Alarmed));
}

#[test]
fn alarmed_to_acknowledged_is_not_allowed_via_automated() {
    // Acknowledged is a user action; automated updates must not produce it.
    assert!(!transition_allowed(State::Alarmed, State::Acknowledged));
}

#[test]
fn bypassed_to_alarmed_is_not_allowed_via_automated() {
    assert!(!transition_allowed(State::Bypassed, State::Alarmed));
}

// ---------------------------------------------------------------------------
// user_action_allowed
// ---------------------------------------------------------------------------

#[test]
fn acknowledge_alarmed_is_allowed() {
    assert!(user_action_allowed(
        State::Alarmed,
        None,
        &UserAction::Acknowledge
    ));
}

#[test]
fn acknowledge_latched_is_allowed() {
    assert!(user_action_allowed(
        State::Latched,
        None,
        &UserAction::Acknowledge
    ));
}

#[test]
fn acknowledge_ok_is_not_allowed() {
    assert!(!user_action_allowed(
        State::Ok,
        None,
        &UserAction::Acknowledge
    ));
}

#[test]
fn acknowledge_bypassed_is_not_allowed() {
    assert!(!user_action_allowed(
        State::Bypassed,
        None,
        &UserAction::Acknowledge
    ));
}

#[test]
fn acknowledge_unknown_is_not_allowed() {
    assert!(!user_action_allowed(
        State::Unknown,
        None,
        &UserAction::Acknowledge
    ));
}

#[test]
fn bypass_non_bypassed_is_allowed() {
    assert!(user_action_allowed(
        State::Ok,
        None,
        &UserAction::Bypass(None)
    ));
    assert!(user_action_allowed(
        State::Alarmed,
        None,
        &UserAction::Bypass(None)
    ));
    assert!(user_action_allowed(
        State::Unknown,
        None,
        &UserAction::Bypass(None)
    ));
}

#[test]
fn bypass_already_bypassed_with_same_wake_is_not_allowed() {
    let wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });
    assert!(!user_action_allowed(
        State::Bypassed,
        wake,
        &UserAction::Bypass(wake)
    ));
}

#[test]
fn bypass_already_bypassed_with_different_wake_is_allowed() {
    let old_wake = Some(Timestamp {
        seconds: 1_000_000,
        nanos: 0,
    });
    let new_wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });
    assert!(user_action_allowed(
        State::Bypassed,
        old_wake,
        &UserAction::Bypass(new_wake)
    ));
}

#[test]
fn bypass_already_bypassed_with_no_wake_both_sides_is_not_allowed() {
    assert!(!user_action_allowed(
        State::Bypassed,
        None,
        &UserAction::Bypass(None)
    ));
}

#[test]
fn unbypass_bypassed_is_allowed() {
    assert!(user_action_allowed(
        State::Bypassed,
        None,
        &UserAction::Activate
    ));
}

#[test]
fn unbypass_non_bypassed_is_not_allowed() {
    assert!(!user_action_allowed(
        State::Alarmed,
        None,
        &UserAction::Activate
    ));
    assert!(!user_action_allowed(State::Ok, None, &UserAction::Activate));
    assert!(!user_action_allowed(
        State::Unknown,
        None,
        &UserAction::Activate
    ));
}
