//! Tests for alarm transition and user-action policy helpers.

use super::*;

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
fn bypassed_to_alarmed_is_allowed() {
    assert!(transition_allowed(State::Bypassed, State::Alarmed));
}

#[test]
fn bypassed_to_ok_is_allowed() {
    assert!(transition_allowed(State::Bypassed, State::Ok));
}

#[test]
fn unknown_to_alarmed_is_allowed() {
    assert!(transition_allowed(State::Unknown, State::Alarmed));
}

#[test]
fn unknown_to_ok_is_allowed() {
    assert!(transition_allowed(State::Unknown, State::Ok));
}

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

fn make_status(source: Source, state: State) -> Status {
    Status {
        device: "M:BEAM".to_string(),
        state: state as i32,
        source: source as i32,
        ..Status::default()
    }
}

fn beam_analog_key() -> Key {
    Key::try_from("M:BEAM#Analog").unwrap()
}

#[test]
fn should_publish_epics_source_bypassed_alarm_is_suppressed() {
    let prev = make_status(Source::Epics, State::Bypassed);
    let next = make_status(Source::Epics, State::Alarmed);
    let key = Key::try_from("M:BEAM#Epics").unwrap();
    assert!(
        !should_publish(Some(&prev), &key, &next),
        "Epics-sourced alarm while bypassed must be suppressed"
    );
}

#[test]
fn should_publish_non_epics_source_bypassed_alarm_is_allowed() {
    let prev = make_status(Source::Analog, State::Bypassed);
    let next = make_status(Source::Analog, State::Alarmed);
    let key = beam_analog_key();
    assert!(
        should_publish(Some(&prev), &key, &next),
        "non-Epics alarm while bypassed must be allowed through"
    );
}

#[test]
fn should_publish_unbypassed_from_epics_while_bypassed_is_allowed() {
    let prev = make_status(Source::Epics, State::Bypassed);
    let next = make_status(Source::Epics, State::Unbypassed);
    let key = Key::try_from("M:BEAM#Epics").unwrap();
    assert!(
        should_publish(Some(&prev), &key, &next),
        "Unbypassed from Epics while bypassed must be allowed through"
    );
}
