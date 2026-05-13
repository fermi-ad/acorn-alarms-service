//! Tests for the report module.

use super::*;
use crate::proto::{common::alarm::status::Severity, google::protobuf::Timestamp};
use crate::test_utils::TestPub;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;

fn get_test_alarm(device: &str, state: State, source: Source) -> Status {
    Status {
        device: String::from(device),
        state: state as i32,
        severity: Severity::Low as i32,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: 0,
            nanos: 0,
        }),
        epics_type: String::default(),
        user: String::default(),
        wake: None,
        source: source as i32,
    }
}

#[test]
fn call_alarms_reporter_new_with_kafka_publisher() {
    let result = AlarmsReporter::<KafkaPublisher>::new();
    assert_eq!(HashMap::new(), result.known_alarms);
}

#[test]
fn report_alarm_not_active() {
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    let key = Key {
        device: "TEST DEVICE".to_string(),
        source: Source::Analog,
    };
    test_reporter.known_alarms.insert(
        key,
        get_test_alarm("TEST DEVICE", State::Alarmed, Source::Analog),
    );

    test_reporter.report(get_test_alarm("DEVICE 2", State::Ok, Source::Analog));

    let key2 = Key {
        device: "DEVICE 2".to_string(),
        source: Source::Analog,
    };
    assert!(!test_reporter.known_alarms.contains_key(&key2));
    assert!(test_reporter.controls_publisher.latest.is_some());
}

#[test]
fn report_does_not_update_cache_on_publish_failure() {
    // When the Kafka publish fails, the cache must remain unchanged so that
    // the next incoming update can retry the transition.
    let mut test_reporter = AlarmsReporter {
        controls_publisher: TestPub::init_throwing(),
        known_alarms: HashMap::new(),
    };

    let test_alarm = get_test_alarm("TEST DEVICE", State::Alarmed, Source::Digital);
    let test_key = Key::from(&test_alarm);
    test_reporter.report(test_alarm);
    // Publish failed → cache must NOT contain the new alarm.
    assert!(!test_reporter.known_alarms.contains_key(&test_key));
    assert!(test_reporter.controls_publisher.latest.is_none());
}

#[test]
fn set_bypass_does_not_update_cache_on_publish_failure() {
    // When the Kafka publish fails, the cache must remain unchanged:
    // the existing real-source entry must NOT be replaced by the bypass record.
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    // Pre-populate a real-source alarm.
    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };
    test_reporter.known_alarms.insert(
        analog_key.clone(),
        get_test_alarm("DEVICE A", State::Alarmed, Source::Analog),
    );

    // Switch to a throwing publisher so the bypass publish fails.
    test_reporter.controls_publisher = TestPub::init_throwing();
    test_reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    // The entry must still be Alarmed, not replaced by a Bypassed record.
    let entry = test_reporter
        .known_alarms
        .get(&analog_key)
        .expect("cache entry must still exist");
    assert_eq!(
        entry.state(),
        State::Alarmed,
        "cache must not be mutated to Bypassed when publish fails"
    );
}

#[test]
fn set_acknowledged_does_not_update_cache_on_publish_failure() {
    // When the Kafka publish fails, the cache entries must remain in their
    // original state (Alarmed), not be mutated to Acknowledged.
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    // Pre-populate an alarmed device.
    let key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };
    test_reporter.known_alarms.insert(
        key.clone(),
        get_test_alarm("DEVICE A", State::Alarmed, Source::Analog),
    );

    // Switch to a throwing publisher so the acknowledge publish fails.
    test_reporter.controls_publisher = TestPub::init_throwing();
    test_reporter.set_acknowledged("DEVICE A#Analog".to_string(), "operator".to_string());

    // Cache entry must still be Alarmed, not Acknowledged.
    let entry = test_reporter
        .known_alarms
        .get(&key)
        .expect("cache entry must still exist");
    assert_eq!(
        entry.state(),
        State::Alarmed,
        "cache must not be mutated to Acknowledged when publish fails"
    );
}

#[test]
fn set_active_does_not_remove_sentinel_on_publish_failure() {
    // When the Kafka publish fails, the bypass entry must remain in the
    // cache so that the source stays suppressed and the next attempt can retry.
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    // Pre-populate a bypass entry directly (source: Analog, state: Bypassed).
    let bypass_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };
    test_reporter.known_alarms.insert(
        bypass_key.clone(),
        get_test_alarm("DEVICE A", State::Bypassed, Source::Analog),
    );

    // Switch to a throwing publisher so the unbypassed publish fails.
    test_reporter.controls_publisher = TestPub::init_throwing();
    test_reporter.set_active("DEVICE A#Analog".to_string(), "operator".to_string());

    // Bypass entry must still be present — publish failed, cache unchanged.
    assert!(
        test_reporter.known_alarms.contains_key(&bypass_key),
        "bypass entry must not be removed when publish fails"
    );
}

#[test]
fn handles_subset_of_devices_independently() {
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    // Raise alarms for a subset of non contiguous devices
    test_reporter.report(get_test_alarm("DEVICE 2", State::Alarmed, Source::Analog));
    test_reporter.report(get_test_alarm("DEVICE 7", State::Alarmed, Source::Epics));

    let key2 = Key {
        device: "DEVICE 2".to_string(),
        source: Source::Analog,
    };
    let key7 = Key {
        device: "DEVICE 7".to_string(),
        source: Source::Epics,
    };

    assert!(test_reporter.known_alarms.contains_key(&key2));
    assert!(test_reporter.known_alarms.contains_key(&key7));
    assert_eq!(test_reporter.known_alarms.len(), 2);

    // Clear alarm for only one device
    test_reporter.report(get_test_alarm("DEVICE 2", State::Ok, Source::Analog));

    assert!(!test_reporter.known_alarms.contains_key(&key2));

    let status7 = test_reporter.known_alarms.get(&key7).unwrap();
    assert_eq!(status7.state(), State::Alarmed);
    assert_eq!(status7.severity(), Severity::Low);

    assert_eq!(test_reporter.known_alarms.len(), 1);
}

#[test]
fn report_new_alarm() {
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    let source = Source::Analog;
    test_reporter.report(get_test_alarm("TEST DEVICE", State::Ok, source));

    let key = Key {
        device: "TEST DEVICE".to_string(),
        source,
    };
    assert!(!test_reporter.known_alarms.contains_key(&key));

    test_reporter.report(get_test_alarm("TEST DEVICE", State::Alarmed, source));
    let status = test_reporter.known_alarms.get(&key).unwrap();
    assert_eq!(status.state(), State::Alarmed);
    assert_eq!(status.severity(), Severity::Low);
}

#[test]
fn report_same_alarm_does_not_throw_err() {
    let mut test_reporter = AlarmsReporter::<TestPub>::new();

    let source = Source::Analog;
    test_reporter.report(get_test_alarm("TEST DEVICE", State::Ok, source));

    let key = Key {
        device: "TEST DEVICE".to_string(),
        source,
    };
    assert!(!test_reporter.known_alarms.contains_key(&key));
}

#[test]
fn test_should_publish_logic() {
    let reporter = AlarmsReporter::<TestPub>::new();

    // New alarm should publish
    let alarm = get_test_alarm("DEV1", State::Ok, Source::Analog);
    assert!(reporter.should_publish(&alarm));

    // Simulate stored previous state
    let mut reporter = AlarmsReporter::<TestPub>::new();
    let key = Key {
        device: "DEV1".to_string(),
        source: Source::Analog,
    };
    reporter
        .known_alarms
        .insert(key, get_test_alarm("DEV1", State::Ok, Source::Analog));

    let same_alarm = get_test_alarm("DEV1", State::Ok, Source::Analog);
    assert!(!reporter.should_publish(&same_alarm));

    let state_change = get_test_alarm("DEV1", State::Alarmed, Source::Analog);
    assert!(reporter.should_publish(&state_change));
}

#[test]
fn get_snapshot_returns_non_ok_alarms() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));
    reporter.report(get_test_alarm("DEVICE B", State::Alarmed, Source::Digital));
    reporter.report(get_test_alarm("DEVICE A", State::Ok, Source::Analog)); // clears device a

    let snapshot = reporter.get_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].device, "DEVICE B");
    assert_eq!(snapshot[0].state(), State::Alarmed);
}

#[test]
fn get_snapshot_includes_bypassed_alarms() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    let snapshot = reporter.get_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].device, "DEVICE A");
    assert_eq!(snapshot[0].state(), State::Bypassed);
}

#[test]
fn bypass_suppresses_alarm_arriving_after_bypass_is_set() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());
    // Clear the publisher so we can detect whether the next report publishes
    reporter.controls_publisher.latest = None;

    // An alarm on the same source (Analog) must be suppressed.
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));

    assert!(reporter.controls_publisher.latest.is_none());
}

// ── Blocker 2 regression tests ────────────────────────────────────────────────

/// When a bypass is set on a specific source that already has a real alarm,
/// the existing entry must be replaced by the bypass record (state: Bypassed).
#[test]
fn bypass_replaces_real_source_entry_with_bypass_record() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));
    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };

    // Entry must now be Bypassed (replaced in-place, not removed).
    let entry = reporter
        .known_alarms
        .get(&analog_key)
        .expect("bypass entry missing");
    assert_eq!(entry.state(), State::Bypassed);
}

/// After a bypass is set on Analog, a new alarm arriving on the same source
/// must be suppressed.
#[test]
fn bypass_suppresses_alarm_on_same_source() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));
    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    // Clear the publisher so we can detect whether the next report publishes
    reporter.controls_publisher.latest = None;

    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));

    // Must be suppressed — bypass entry is present for Analog
    assert!(
        reporter.controls_publisher.latest.is_none(),
        "alarm on Analog source should have been suppressed by bypass"
    );
}

/// Bypass on Analog must NOT suppress alarms arriving on a different source
/// (Digital).  Source-specific bypass only affects the bypassed source.
#[test]
fn bypass_on_one_source_does_not_suppress_other_sources() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    // Clear the publisher so we can detect whether the next report publishes
    reporter.controls_publisher.latest = None;

    // An alarm on a different source (Digital) must NOT be suppressed.
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Digital));

    assert!(
        reporter.controls_publisher.latest.is_some(),
        "alarm on Digital source must NOT be suppressed by an Analog bypass"
    );
}

/// Simulates the Redis stream path: an alarm arrives on Analog, the Analog
/// source is bypassed, then a subsequent Redis stream update arrives on Epics.
/// The Epics update must NOT be suppressed.
#[test]
fn bypass_does_not_suppress_alarm_on_different_source_via_redis_stream() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    // Simulates a Redis stream update on Analog
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));
    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    // Clear the publisher so we can detect whether the next report publishes
    reporter.controls_publisher.latest = None;

    // Simulates a subsequent Redis stream update on a different source (Epics)
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Epics));

    assert!(
        reporter.controls_publisher.latest.is_some(),
        "alarm on Epics source must NOT be suppressed by an Analog bypass"
    );
}

/// `set_active` must remove only the bypass entry for the specific source and
/// publish a single `Unbypassed` event for that source.
#[test]
fn set_active_removes_bypass_entry_and_publishes_unbypassed() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());
    reporter.set_active("DEVICE A#Analog".to_string(), "operator".to_string());

    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };

    // Bypass entry must be gone
    assert!(
        !reporter.known_alarms.contains_key(&analog_key),
        "bypass entry should have been removed by set_active"
    );

    // Published message must have the actual source and state Unbypassed
    let published = reporter
        .controls_publisher
        .latest
        .as_ref()
        .expect("expected an Unbypassed message to be published");

    let body: Status = serde_json::from_str(&published.value)
        .expect("published message body should deserialize as Status");

    assert_eq!(
        body.source(),
        Source::Analog,
        "published source must be Analog"
    );
    assert_eq!(
        body.state(),
        State::Unbypassed,
        "published state must be Unbypassed"
    );
}

/// `set_active` on one source must not affect a bypass on a different source.
#[test]
fn set_active_on_one_source_does_not_affect_bypass_on_other_source() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    // Bypass both Analog and Digital independently.
    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());
    reporter.set_bypass("DEVICE A#Digital".to_string(), "operator".to_string());

    // Activate (unbypass) only Analog.
    reporter.set_active("DEVICE A#Analog".to_string(), "operator".to_string());

    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };
    let digital_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Digital,
    };

    // Analog bypass must be gone.
    assert!(
        !reporter.known_alarms.contains_key(&analog_key),
        "Analog bypass entry should have been removed"
    );

    // Digital bypass must still be present.
    let digital_entry = reporter
        .known_alarms
        .get(&digital_key)
        .expect("Digital bypass entry must still exist");
    assert_eq!(
        digital_entry.state(),
        State::Bypassed,
        "Digital bypass must be unaffected by activating Analog"
    );
}

/// Acknowledging a specific source must not affect other sources for the same
/// device.
#[test]
fn acknowledge_on_specific_source_does_not_affect_other_sources() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    // Pre-populate two sources as Alarmed.
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Digital));

    // Acknowledge only the Analog source.
    reporter.set_acknowledged("DEVICE A#Analog".to_string(), "operator".to_string());

    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };
    let digital_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Digital,
    };

    // Analog must be Acknowledged.
    let analog_entry = reporter
        .known_alarms
        .get(&analog_key)
        .expect("Analog entry must still exist");
    assert_eq!(
        analog_entry.state(),
        State::Acknowledged,
        "Analog source must be Acknowledged"
    );

    // Digital must still be Alarmed.
    let digital_entry = reporter
        .known_alarms
        .get(&digital_key)
        .expect("Digital entry must still exist");
    assert_eq!(
        digital_entry.state(),
        State::Alarmed,
        "Digital source must remain Alarmed"
    );
}

/// `set_active` called on a device-source that is currently Alarmed (not
/// Bypassed) must be a no-op — it must not clear the alarm from the cache or
/// publish an Unbypassed event.
#[test]
fn set_active_does_not_clear_non_bypassed_alarm() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    // Pre-populate an Alarmed entry (not bypassed).
    reporter.report(get_test_alarm("DEVICE A", State::Alarmed, Source::Analog));

    // Clear the publisher so we can detect whether set_active publishes anything.
    reporter.controls_publisher.latest = None;

    // Attempt to activate a source that is Alarmed, not Bypassed.
    reporter.set_active("DEVICE A#Analog".to_string(), "operator".to_string());

    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };

    // The Alarmed entry must still be present and unchanged.
    let entry = reporter
        .known_alarms
        .get(&analog_key)
        .expect("Alarmed entry must still exist");
    assert_eq!(
        entry.state(),
        State::Alarmed,
        "set_active must not clear a non-bypassed alarm"
    );

    // Nothing should have been published.
    assert!(
        reporter.controls_publisher.latest.is_none(),
        "set_active on a non-bypassed source must not publish anything"
    );
}

/// Acknowledging a bypassed source must be a no-op — it must not pull the
/// source out of bypass.
#[test]
fn acknowledge_does_not_override_bypass() {
    let mut reporter = AlarmsReporter::<TestPub>::new();

    // Bypass the Analog source.
    reporter.set_bypass("DEVICE A#Analog".to_string(), "operator".to_string());

    // Clear the publisher so we can detect whether acknowledge publishes anything.
    reporter.controls_publisher.latest = None;

    // Attempt to acknowledge the bypassed source.
    reporter.set_acknowledged("DEVICE A#Analog".to_string(), "operator".to_string());

    // The bypass entry must still be Bypassed, not Acknowledged.
    let analog_key = Key {
        device: "DEVICE A".to_string(),
        source: Source::Analog,
    };
    let entry = reporter
        .known_alarms
        .get(&analog_key)
        .expect("bypass entry must still exist");
    assert_eq!(
        entry.state(),
        State::Bypassed,
        "acknowledge must not override a bypass"
    );

    // Nothing should have been published.
    assert!(
        reporter.controls_publisher.latest.is_none(),
        "acknowledge on a bypassed source must not publish anything"
    );
}

// ── Key::from(&str) unit tests ─────────────────────────────────────────────────

#[test]
fn key_from_str_parses_analog() {
    let key = Key::from("M:BEAM#Analog");
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Analog);
}

#[test]
fn key_from_str_parses_digital() {
    let key = Key::from("Z:ACLTST#Digital");
    assert_eq!(key.device, "Z:ACLTST");
    assert_eq!(key.source, Source::Digital);
}

#[test]
fn key_from_str_parses_epics() {
    let key = Key::from("PIP2IT:pHB650#Epics");
    assert_eq!(key.device, "PIP2IT:PHB650");
    assert_eq!(key.source, Source::Epics);
}

#[test]
fn key_from_str_unknown_source_falls_back_to_unknown() {
    let key = Key::from("M:BEAM#Bogus");
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Unknown);
}

#[test]
fn key_from_str_no_separator_falls_back_to_unknown_source() {
    let key = Key::from("M:BEAM");
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Unknown);
}

#[test]
fn key_from_str_normalizes_device_name() {
    let key = Key::from("  m:beam  #Analog");
    assert_eq!(key.device, "M:BEAM");
    assert_eq!(key.source, Source::Analog);
}

#[test]
fn key_from_str_source_matching_is_case_insensitive() {
    assert_eq!(Key::from("M:BEAM#analog").source, Source::Analog);
    assert_eq!(Key::from("M:BEAM#ANALOG").source, Source::Analog);
    assert_eq!(Key::from("M:BEAM#digital").source, Source::Digital);
    assert_eq!(Key::from("M:BEAM#EPICS").source, Source::Epics);
}
