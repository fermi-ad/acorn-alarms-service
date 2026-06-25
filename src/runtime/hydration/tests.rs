//! Tests for startup hydration merging logic.

use std::collections::HashMap;

use super::*;
use crate::{
    model::key::Key,
    proto::common::alarm::status::{Source, State},
    test_utils::make_status,
};

#[test]
fn merge_hydrated_statuses_adds_disjoint_keys() {
    let accumulated = HashMap::from([(
        Key::try_from("M:BEAM#Epics").unwrap(),
        make_status("M:BEAM", State::Bypassed, Source::Epics),
    )]);
    let incoming = HashMap::from([(
        Key::try_from("Z:VAC#Analog").unwrap(),
        make_status("Z:VAC", State::Alarmed, Source::Analog),
    )]);

    let merged =
        merge_hydrated_statuses(accumulated, incoming).expect("disjoint keys should merge");

    assert_eq!(merged.len(), 2);
    assert!(merged.contains_key(&Key::try_from("M:BEAM#Epics").unwrap()));
    assert!(merged.contains_key(&Key::try_from("Z:VAC#Analog").unwrap()));
}

#[test]
fn merge_hydrated_statuses_rejects_duplicate_keys() {
    let accumulated = HashMap::from([(
        Key::try_from("M:BEAM#Epics").unwrap(),
        make_status("M:BEAM", State::Bypassed, Source::Epics),
    )]);
    let incoming = HashMap::from([(
        Key::try_from("M:BEAM#Epics").unwrap(),
        make_status("M:BEAM", State::Alarmed, Source::Epics),
    )]);

    let err = merge_hydrated_statuses(accumulated, incoming)
        .expect_err("duplicate keys should fail loudly");

    assert!(matches!(err, HydrationError::KeyInMultipleSources(_)));
}
