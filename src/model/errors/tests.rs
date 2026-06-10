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
fn kafka_write_failed_display_mentions_key() {
    let error = UpdateError::KafkaWriteFailed(Key::try_from("M:BEAM#Analog").unwrap());

    assert_eq!(
        error.to_string(),
        "Failed writing update for M:BEAM#Analog to Kafka."
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
