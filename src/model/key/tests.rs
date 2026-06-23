//! Tests for `Key` construction, normalization, display, and equality.

use crate::{proto::common::alarm::status::State, test_utils::make_status};

use super::*;

#[test]
fn key_from_status_normalizes_device_name() {
    let status = make_status("  m:beam  ", State::Alarmed, Source::Analog);
    let key = Key::from(&status);
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Analog);
}

#[test]
fn key_from_status_preserves_source() {
    let status = make_status("Z:ACLTST", State::Alarmed, Source::Digital);
    let key = Key::from(&status);
    assert_eq!(key.source, Source::Digital);
}

#[test]
fn key_from_str_parses_analog() {
    let key = Key::try_from("M:BEAM#Analog").unwrap();
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Analog);
}

#[test]
fn key_from_str_parses_digital() {
    let key = Key::try_from("Z:ACLTST#Digital").unwrap();
    assert_eq!(key.device, "Z:ACLTST");
    assert_eq!(key.source, Source::Digital);
}

#[test]
fn key_from_str_parses_epics() {
    let key = Key::try_from("PIP2IT:pHB650#Epics").unwrap();
    assert_eq!(key.device, "PIP2IT:PHB650");
    assert_eq!(key.source, Source::Epics);
}

#[test]
fn key_from_str_unknown_source_returns_err() {
    let result = Key::try_from("M:BEAM#Bogus");
    assert!(result.is_err());
    assert_eq!(
        "M:BEAM#Bogus does not contain a known alarms source.",
        result.unwrap_err()
    );
}

#[test]
fn key_from_str_no_separator_returns_err() {
    let result = Key::try_from("M:BEAM");
    assert!(result.is_err());
    assert_eq!(
        "M:BEAM does not contain the expected '#' delimiter for separating device name from alarm source.",
        result.unwrap_err()
    );
}

#[test]
fn key_from_str_no_device_returns_err() {
    let result = Key::try_from("#Analog");
    assert!(result.is_err());
    assert_eq!("#Analog is missing a device name.", result.unwrap_err());
}

#[test]
fn key_from_str_normalizes_device_name() {
    let key = Key::try_from("  m:beam  #Analog").unwrap();
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Analog);
}

#[test]
fn key_from_str_source_matching_is_case_insensitive() {
    assert_eq!(
        Key::try_from("M:BEAM#analog").unwrap().source,
        Source::Analog
    );
    assert_eq!(
        Key::try_from("M:BEAM#ANALOG").unwrap().source,
        Source::Analog
    );
    assert_eq!(
        Key::try_from("M:BEAM#digital").unwrap().source,
        Source::Digital
    );
    assert_eq!(Key::try_from("M:BEAM#EPICS").unwrap().source, Source::Epics);
}

#[test]
fn key_display_formats_as_device_hash_source() {
    let key = Key::try_from("M:BEAM#Analog").unwrap();
    assert_eq!(key.to_string(), "M:BEAM#Analog");
}

#[test]
fn key_display_formats_digital_source() {
    let key = Key::try_from("Z:ACLTST#Digital").unwrap();
    assert_eq!(key.to_string(), "Z:ACLTST#Digital");
}

#[test]
fn keys_with_same_device_and_source_are_equal() {
    let a = Key::try_from("M:BEAM#Analog").unwrap();
    let b = Key::try_from("m:beam#analog").unwrap();
    assert_eq!(a, b);
}

#[test]
fn keys_with_different_sources_are_not_equal() {
    let a = Key::try_from("M:BEAM#Analog").unwrap();
    let b = Key::try_from("M:BEAM#Digital").unwrap();
    assert_ne!(a, b);
}
