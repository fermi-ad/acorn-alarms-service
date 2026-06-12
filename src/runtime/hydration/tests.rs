use std::collections::HashMap;

use super::*;
use crate::{
    model::key::Key,
    proto::common::alarm::status::{Source, State},
    test_utils::make_status,
};

#[test]
fn merge_hydrated_statuses_adds_disjoint_keys() {
    let mut accumulated = HashMap::from([(
        Key::try_from("M:BEAM#Epics").unwrap(),
        make_status("M:BEAM", State::Bypassed, Source::Epics),
    )]);
    let incoming = HashMap::from([(
        Key::try_from("Z:VAC#Analog").unwrap(),
        make_status("Z:VAC", State::Alarmed, Source::Analog),
    )]);

    merge_hydrated_statuses(&mut accumulated, incoming).expect("disjoint keys should merge");

    assert_eq!(accumulated.len(), 2);
    assert!(accumulated.contains_key(&Key::try_from("M:BEAM#Epics").unwrap()));
    assert!(accumulated.contains_key(&Key::try_from("Z:VAC#Analog").unwrap()));
}

#[test]
fn merge_hydrated_statuses_rejects_duplicate_keys() {
    let mut accumulated = HashMap::from([(
        Key::try_from("M:BEAM#Epics").unwrap(),
        make_status("M:BEAM", State::Bypassed, Source::Epics),
    )]);
    let incoming = HashMap::from([(
        Key::try_from("M:BEAM#Epics").unwrap(),
        make_status("M:BEAM", State::Alarmed, Source::Epics),
    )]);

    let err = merge_hydrated_statuses(&mut accumulated, incoming)
        .expect_err("duplicate keys should fail loudly");

    assert!(matches!(err, HydrationError::KeyInMultipleSources(_)));
    assert_eq!(accumulated.len(), 1, "existing state must remain intact");
    assert_eq!(
        accumulated
            .get(&Key::try_from("M:BEAM#Epics").unwrap())
            .expect("original key must remain")
            .state(),
        State::Bypassed
    );
}
