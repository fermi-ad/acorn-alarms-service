//! Tests for EPICS startup hydration snapshot loading and reduction.

use rust_pubsub_lib::{Message, PubSubError, Snapshot, StringMessage};

use super::*;
use crate::proto::common::alarm::status::State;

struct FakeSnapshot;

impl Snapshot for FakeSnapshot {
    async fn get<M: Message>(host: String, topic: String) -> Result<Vec<M>, PubSubError> {
        assert_eq!(host, "test-host");
        assert_eq!(topic, "test-topic");

        let records = vec![
            StringMessage::new(
                Some("M:BEAM#Epics".to_string()),
                serde_json::to_string(&epics_status("M:BEAM", State::Bypassed)).unwrap(),
            ),
            StringMessage::new(
                Some("M:BEAM#Analog".to_string()),
                serde_json::to_string(&analog_status("M:BEAM", State::Alarmed)).unwrap(),
            ),
            StringMessage::new(
                Some("M:BEAM#Epics".to_string()),
                serde_json::to_string(&epics_status("M:BEAM", State::Unbypassed)).unwrap(),
            ),
            StringMessage::new(
                Some("Z:VAC#Epics".to_string()),
                serde_json::to_string(&epics_status("Z:VAC", State::Bypassed)).unwrap(),
            ),
            StringMessage::new(Some("BADKEY".to_string()), "{}".to_string()),
            StringMessage::from_value("missing-key".to_string()),
        ];

        Ok(records
            .into_iter()
            .map(|record| M::from(record.into_bytes()))
            .collect())
    }
}

#[tokio::test]
async fn load_epics_hydration_filters_to_latest_epics_state() {
    let hydrated =
        load_epics_hydration::<FakeSnapshot>("test-host".to_string(), "test-topic".to_string())
            .await
            .unwrap();

    assert_eq!(hydrated.len(), 1);
    let status = hydrated
        .get(&Key::try_from("Z:VAC#Epics").unwrap())
        .expect("Z:VAC bypass should remain");
    assert_eq!(status.device, "Z:VAC");
    assert_eq!(status.source(), Source::Epics);
    assert_eq!(status.state(), State::Bypassed);
}

#[test]
fn reduce_snapshot_tombstone_removes_prior_bypass_state() {
    let hydrated = reduce_snapshot(vec![
        StringMessage::new(
            Some("M:BEAM#Epics".to_string()),
            serde_json::to_string(&epics_status("M:BEAM", State::Bypassed)).unwrap(),
        ),
        StringMessage::new(Some("M:BEAM#Epics".to_string()), "null".to_string()),
    ]);

    assert!(
        hydrated.is_empty(),
        "a tombstone should remove an earlier bypass for the same key"
    );
}

#[test]
fn reduce_snapshot_invalid_payload_after_valid_bypass_keeps_prior_state() {
    let hydrated = reduce_snapshot(vec![
        StringMessage::new(
            Some("M:BEAM#Epics".to_string()),
            serde_json::to_string(&epics_status("M:BEAM", State::Bypassed)).unwrap(),
        ),
        StringMessage::new(Some("M:BEAM#Epics".to_string()), "{not-json}".to_string()),
    ]);

    let status = hydrated
        .get(&Key::try_from("M:BEAM#Epics").unwrap())
        .expect("invalid payload should not discard the last valid bypass state");
    assert_eq!(status.device, "M:BEAM");
    assert_eq!(status.source(), Source::Epics);
    assert_eq!(status.state(), State::Bypassed);
}

fn epics_status(device: &str, state: State) -> Status {
    Status {
        device: device.to_string(),
        source: Source::Epics as i32,
        state: state as i32,
        ..Default::default()
    }
}

fn analog_status(device: &str, state: State) -> Status {
    Status {
        device: device.to_string(),
        source: Source::Analog as i32,
        state: state as i32,
        ..Default::default()
    }
}
