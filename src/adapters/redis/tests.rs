//! Unit tests for the redis_stream module.

use super::*;

fn make_map(fields: &[(&str, &str)]) -> HashMap<String, String> {
    fields
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn severity_of(status: &Status) -> Severity {
    status.severity()
}

fn state_of(status: &Status) -> State {
    status.state()
}

fn source_of(status: &Status) -> Source {
    status.source()
}

#[test]
fn device_is_uppercased() {
    let map = make_map(&[
        ("device", "m:beam"),
        ("severity", "HIGH"),
        ("source", "ANALOG"),
    ]);
    let status = build_status_from_redis(map);
    assert_eq!(status.device, "M:BEAM");
}

#[test]
fn device_already_uppercase_is_unchanged() {
    let map = make_map(&[
        ("device", "M:BEAM"),
        ("severity", "HIGH"),
        ("source", "ANALOG"),
    ]);
    let status = build_status_from_redis(map);
    assert_eq!(status.device, "M:BEAM");
}

#[test]
fn missing_device_field_yields_empty_string() {
    let map = make_map(&[("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert!(status.device.is_empty());
}

#[test]
fn severity_low_maps_to_low() {
    let map = make_map(&[("device", "D"), ("severity", "LOW"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::Low);
}

#[test]
fn severity_minor_maps_to_low() {
    let map = make_map(&[("device", "D"), ("severity", "MINOR"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::Low);
}

#[test]
fn severity_high_maps_to_high() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::High);
}

#[test]
fn severity_major_maps_to_high() {
    let map = make_map(&[("device", "D"), ("severity", "MAJOR"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::High);
}

#[test]
fn severity_unrecognised_maps_to_unknown() {
    let map = make_map(&[
        ("device", "D"),
        ("severity", "CRITICAL"),
        ("source", "ANALOG"),
    ]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::Unknown);
}

#[test]
fn missing_severity_field_maps_to_unknown() {
    let map = make_map(&[("device", "D"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::Unknown);
}

#[test]
fn severity_lowercase_low_is_accepted() {
    let map = make_map(&[("device", "D"), ("severity", "low"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::Low);
}

#[test]
fn severity_lowercase_high_is_accepted() {
    let map = make_map(&[("device", "D"), ("severity", "high"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(severity_of(&status), Severity::High);
}

#[test]
fn low_severity_yields_alarmed_state() {
    let map = make_map(&[("device", "D"), ("severity", "LOW"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(state_of(&status), State::Alarmed);
}

#[test]
fn high_severity_yields_alarmed_state() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(state_of(&status), State::Alarmed);
}

#[test]
fn no_alarm_severity_yields_ok_state() {
    let map = make_map(&[
        ("device", "D"),
        ("severity", "NO_ALARM"),
        ("source", "ANALOG"),
    ]);
    let status = build_status_from_redis(map);
    assert_eq!(state_of(&status), State::Ok);
}

#[test]
fn unrecognised_severity_yields_unknown_state() {
    let map = make_map(&[
        ("device", "D"),
        ("severity", "CRITICAL"),
        ("source", "ANALOG"),
    ]);
    let status = build_status_from_redis(map);
    assert_eq!(state_of(&status), State::Unknown);
}

#[test]
fn missing_severity_yields_unknown_state() {
    let map = make_map(&[("device", "D"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(state_of(&status), State::Unknown);
}

#[test]
fn source_analog_maps_correctly() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Analog);
}

#[test]
fn source_digital_maps_correctly() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "DIGITAL")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Digital);
}

#[test]
fn source_epics_maps_correctly() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "EPICS")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Epics);
}

#[test]
fn source_unrecognised_maps_to_unknown() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "SCADA")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Unknown);
}

#[test]
fn missing_source_field_maps_to_unknown() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Unknown);
}

#[test]
fn source_lowercase_analog_is_accepted() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "analog")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Analog);
}

#[test]
fn source_lowercase_digital_is_accepted() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "digital")]);
    let status = build_status_from_redis(map);
    assert_eq!(source_of(&status), Source::Digital);
}

#[test]
fn timestamp_is_extracted_when_present() {
    let map = make_map(&[
        ("device", "D"),
        ("severity", "HIGH"),
        ("source", "ANALOG"),
        ("timestamp", "123456789"),
    ]);
    let status = build_status_from_redis(map);
    assert!(
        status
            .time
            .is_some_and(|timestamp| timestamp.seconds == 123_456_789)
    );
}

#[test]
fn timestamp_defaults_to_current_second_when_absent() {
    let current = Utc::now().timestamp();
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert!(
        status
            .time
            .is_some_and(|timestamp| timestamp.seconds >= current)
    );
}

#[test]
fn timestamp_defaults_to_current_second_when_malformed() {
    let current = Utc::now().timestamp();
    let map = make_map(&[
        ("device", "D"),
        ("severity", "HIGH"),
        ("source", "ANALOG"),
        ("timestamp", "123abcdefghijk"),
    ]);
    let status = build_status_from_redis(map);
    assert!(
        status
            .time
            .is_some_and(|timestamp| timestamp.seconds >= current)
    );
}

#[test]
fn acknowledgeable_is_always_false() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert!(!status.acknowledgeable);
}

#[test]
fn epics_type_defaults_to_empty_string() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert!(status.epics_type.is_empty());
}

#[test]
fn user_defaults_to_empty_string() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert!(status.user.is_empty());
}

#[test]
fn wake_defaults_to_none() {
    let map = make_map(&[("device", "D"), ("severity", "HIGH"), ("source", "ANALOG")]);
    let status = build_status_from_redis(map);
    assert!(status.wake.is_none());
}

#[test]
fn empty_map_produces_empty_device_and_unknown_fields() {
    let status = build_status_from_redis(HashMap::new());
    assert!(status.device.is_empty());
    assert_eq!(severity_of(&status), Severity::Unknown);
    assert_eq!(state_of(&status), State::Unknown);
    assert_eq!(source_of(&status), Source::Unknown);
}
