use super::*;
use crate::proto::common::alarm::status::State;

#[test]
fn acknowledge_maps_to_acknowledged_state() {
    assert_eq!(UserAction::Acknowledge.as_state(), State::Acknowledged);
}

#[test]
fn activate_maps_to_unbypassed_state() {
    assert_eq!(UserAction::Activate.as_state(), State::Unbypassed);
}

#[test]
fn bypass_maps_to_bypassed_state() {
    assert_eq!(UserAction::Bypass(None).as_state(), State::Bypassed);
}

#[test]
fn bypass_returns_wake_timestamp() {
    let wake = Some(crate::proto::google::protobuf::Timestamp {
        seconds: 123,
        nanos: 456,
    });

    assert_eq!(UserAction::Bypass(wake).get_wake(), wake);
}

#[test]
fn non_bypass_actions_have_no_wake() {
    assert_eq!(UserAction::Acknowledge.get_wake(), None);
    assert_eq!(UserAction::Activate.get_wake(), None);
}
