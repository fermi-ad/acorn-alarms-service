use super::*;
use crate::proto::common::alarm::status::State;

#[test]
fn state_transition_display_includes_key_and_states() {
    let transition = StateTransition {
        key: Key::try_from("M:BEAM#Analog").unwrap(),
        current: State::Alarmed,
        requested: State::Acknowledged,
    };

    assert_eq!(
        transition.to_string(),
        "for M:BEAM#Analog from Alarmed into Acknowledged"
    );
}

#[test]
fn internal_error_display_mentions_key() {
    let error = UpdateError::Internal(Key::try_from("M:BEAM#Analog").unwrap());

    assert_eq!(
        error.to_string(),
        "Internal error. The update for M:BEAM#Analog has not been persisted."
    );
}

#[test]
fn state_not_allowed_display_wraps_transition_text() {
    let error = UpdateError::StateNotAllowed(StateTransition {
        key: Key::try_from("M:BEAM#Analog").unwrap(),
        current: State::Ok,
        requested: State::Acknowledged,
    });

    assert_eq!(
        error.to_string(),
        "Invalid state transition for M:BEAM#Analog from Ok into Acknowledged requested."
    );
}

#[test]
fn symmetrical_result_ok_inner_ref_returns_value() {
    let result: SymmetricalResult<u32> = SymmetricalResult::Ok(42);
    assert_eq!(result.inner_ref(), &42);
}

#[test]
fn symmetrical_result_err_inner_ref_returns_value() {
    let result: SymmetricalResult<u32> = SymmetricalResult::Err(42);
    assert_eq!(result.inner_ref(), &42);
}

#[test]
fn symmetrical_result_ok_into_inner_returns_value() {
    let result: SymmetricalResult<u32> = SymmetricalResult::Ok(42);
    assert_eq!(result.into_inner(), 42);
}

#[test]
fn symmetrical_result_err_into_inner_returns_value() {
    let result: SymmetricalResult<u32> = SymmetricalResult::Err(42);
    assert_eq!(result.into_inner(), 42);
}

#[test]
fn symmetrical_result_map_ok_transforms_value() {
    let result: SymmetricalResult<u32> = SymmetricalResult::Ok(1);
    let mapped = result.map(|v| v + 1);
    assert!(mapped.is_ok());
    assert_eq!(mapped.into_inner(), 2);
}

#[test]
fn symmetrical_result_map_err_transforms_value() {
    let result: SymmetricalResult<u32> = SymmetricalResult::Err(1);
    let mapped = result.map(|v| v + 1);
    assert!(!mapped.is_ok());
    assert_eq!(mapped.into_inner(), 2);
}

#[test]
fn symmetrical_result_is_ok_true_for_ok() {
    assert!(SymmetricalResult::Ok(()).is_ok());
}

#[test]
fn symmetrical_result_is_ok_false_for_err() {
    assert!(!SymmetricalResult::Err(()).is_ok());
}
