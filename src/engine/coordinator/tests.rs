use std::time::Duration;

use tokio::{
    sync::oneshot,
    time::{sleep, timeout},
};

use crate::{
    engine::messages::{CoordinatorMessage, DomainInput},
    model::{errors::UpdateError, key::Key, user_action::UserAction},
    proto::{
        common::alarm::{
            Status,
            status::{Severity, Source, State},
        },
        google::protobuf::Timestamp,
    },
    runtime::AlarmStateIngress,
    test_utils::{get_runtime, get_throwing_runtime, make_status},
};

async fn user_update(
    ingress: &AlarmStateIngress,
    key: Key,
    action: UserAction,
    user: &str,
) -> Result<(), UpdateError> {
    let (sender, receiver) = oneshot::channel();
    ingress
        .user_tx
        .try_send(CoordinatorMessage::DomainInput(DomainInput::UserUpdate {
            key,
            action,
            user: user.to_string(),
            confirmation: sender,
        }))
        .expect("test user queue should have capacity");
    receiver
        .await
        .expect("machine must not drop the confirmation sender")
}

async fn snapshot(ingress: &AlarmStateIngress) -> Vec<Status> {
    let (sender, receiver) = oneshot::channel();
    ingress
        .user_tx
        .try_send(CoordinatorMessage::DomainInput(
            DomainInput::SnapshotRequest(sender),
        ))
        .expect("test user queue should have capacity");
    receiver
        .await
        .expect("machine must not drop the snapshot sender")
}

async fn automated_update(ingress: &AlarmStateIngress, status: Status) {
    ingress
        .automated_tx
        .send_automated_update(status)
        .await
        .unwrap();
}

async fn snapshot_after_automated_update(
    ingress: &AlarmStateIngress,
    status: Status,
) -> Vec<Status> {
    automated_update(ingress, status).await;
    sleep(Duration::from_millis(100)).await;
    timeout(Duration::from_secs(2), snapshot(ingress))
        .await
        .expect("snapshot should arrive after automated update")
}

#[tokio::test]
async fn automated_update_appears_in_snapshot() {
    let ingress = get_runtime().await;

    let snap = snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;

    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].device, "M:BEAM");
    assert_eq!(snap[0].state(), State::Alarmed);
}

#[tokio::test]
async fn ok_alarm_does_not_appear_in_snapshot() {
    let ingress = get_runtime().await;

    snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;
    let snap =
        snapshot_after_automated_update(&ingress, make_status("M:BEAM", State::Ok, Source::Analog))
            .await;

    assert!(
        snap.is_empty(),
        "Ok-state alarm must not appear in snapshot"
    );
}

#[tokio::test]
async fn duplicate_automated_update_is_suppressed() {
    let ingress = get_runtime().await;

    for _ in 0..3 {
        snapshot_after_automated_update(
            &ingress,
            make_status("M:BEAM", State::Alarmed, Source::Analog),
        )
        .await;
    }

    let snap = snapshot(&ingress).await;
    assert_eq!(snap.len(), 1, "duplicate updates must be deduplicated");
}

#[tokio::test]
async fn alarmed_to_alarmed_severity_escalation_is_published() {
    let ingress = get_runtime().await;

    snapshot_after_automated_update(
        &ingress,
        Status {
            device: "M:BEAM".to_string(),
            state: State::Alarmed as i32,
            severity: Severity::Low as i32,
            source: Source::Analog as i32,
            acknowledgeable: false,
            time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            epics_type: String::new(),
            user: String::new(),
            wake: None,
        },
    )
    .await;

    let snap = snapshot_after_automated_update(
        &ingress,
        Status {
            device: "M:BEAM".to_string(),
            state: State::Alarmed as i32,
            severity: Severity::High as i32,
            source: Source::Analog as i32,
            acknowledgeable: false,
            time: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            epics_type: String::new(),
            user: String::new(),
            wake: None,
        },
    )
    .await;

    assert_eq!(snap.len(), 1, "still one entry for the device-source");
    assert_eq!(
        snap[0].severity(),
        Severity::High,
        "snapshot must reflect the escalated severity"
    );
}

#[tokio::test]
async fn alarmed_to_alarmed_same_severity_is_suppressed() {
    let ingress = get_runtime().await;

    snapshot_after_automated_update(
        &ingress,
        Status {
            device: "M:BEAM".to_string(),
            state: State::Alarmed as i32,
            severity: Severity::Low as i32,
            source: Source::Analog as i32,
            acknowledgeable: false,
            time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            epics_type: String::new(),
            user: String::new(),
            wake: None,
        },
    )
    .await;

    let snap = snapshot_after_automated_update(
        &ingress,
        Status {
            device: "M:BEAM".to_string(),
            state: State::Alarmed as i32,
            severity: Severity::Low as i32,
            source: Source::Analog as i32,
            acknowledgeable: false,
            time: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            epics_type: String::new(),
            user: String::new(),
            wake: None,
        },
    )
    .await;

    assert_eq!(snap.len(), 1, "duplicate must not create a second entry");
    assert_eq!(
        snap[0].severity(),
        Severity::Low,
        "severity must remain Low — the duplicate was suppressed"
    );
}

#[tokio::test]
async fn multiple_distinct_alarms_all_appear_in_snapshot() {
    let ingress = get_runtime().await;

    for (device, source) in [
        ("M:BEAM", Source::Analog),
        ("M:BEAM", Source::Digital),
        ("Z:ACLTST", Source::Analog),
    ] {
        snapshot_after_automated_update(&ingress, make_status(device, State::Alarmed, source))
            .await;
    }

    let snap = snapshot(&ingress).await;
    assert_eq!(
        snap.len(),
        3,
        "all three distinct device-sources must appear"
    );
}

#[tokio::test]
async fn empty_machine_snapshot_is_empty() {
    let ingress = get_runtime().await;
    let snap = snapshot(&ingress).await;
    assert!(snap.is_empty());
}

#[tokio::test]
async fn acknowledge_alarmed_device_succeeds() {
    let ingress = get_runtime().await;

    snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let result = user_update(&ingress, key, UserAction::Acknowledge, "operator").await;
    assert!(result.is_ok(), "acknowledge of Alarmed must succeed");
}

#[tokio::test]
async fn acknowledge_unknown_device_returns_state_not_allowed() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let result = user_update(&ingress, key, UserAction::Acknowledge, "operator").await;

    assert!(
        matches!(result, Err(UpdateError::StateNotAllowed(_))),
        "acknowledging an unknown device must return StateNotAllowed"
    );
}

#[tokio::test]
async fn acknowledge_latched_device_succeeds() {
    let ingress = get_runtime().await;

    snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;
    snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Latched, Source::Analog),
    )
    .await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let result = user_update(&ingress, key, UserAction::Acknowledge, "operator").await;
    assert!(result.is_ok(), "acknowledge of Latched must succeed");
}

#[tokio::test]
async fn bypass_non_bypassed_device_succeeds() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let result = user_update(&ingress, key.clone(), UserAction::Bypass(None), "operator").await;

    assert!(
        result.is_ok(),
        "bypass of a non-bypassed device must succeed"
    );

    let snap = snapshot(&ingress).await;
    assert_eq!(snap.len(), 1, "bypass should become confirmed state");
    assert_eq!(snap[0].state(), State::Bypassed);
    assert_eq!(snap[0].user, "operator");
}

#[tokio::test]
async fn bypass_already_bypassed_same_wake_returns_state_not_allowed() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    user_update(&ingress, key.clone(), UserAction::Bypass(None), "operator")
        .await
        .unwrap();

    let result = user_update(&ingress, key, UserAction::Bypass(None), "operator").await;
    assert!(
        matches!(result, Err(UpdateError::StateNotAllowed(_))),
        "re-bypassing with the same wake must return StateNotAllowed"
    );
}

#[tokio::test]
async fn bypass_already_bypassed_different_wake_succeeds() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let old_wake = Some(Timestamp {
        seconds: 1_000_000,
        nanos: 0,
    });
    let new_wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });

    user_update(
        &ingress,
        key.clone(),
        UserAction::Bypass(old_wake),
        "operator",
    )
    .await
    .unwrap();

    let result = user_update(&ingress, key, UserAction::Bypass(new_wake), "operator").await;
    assert!(
        result.is_ok(),
        "re-bypassing with a different wake must succeed"
    );

    let snap = snapshot(&ingress).await;
    assert_eq!(snap.len(), 1, "updated bypass should replace prior bypass");
    assert_eq!(snap[0].wake, new_wake);
}

#[tokio::test]
async fn automated_alarm_suppressed_while_bypassed() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    user_update(&ingress, key, UserAction::Bypass(None), "operator")
        .await
        .unwrap();

    let snap = snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;

    assert_eq!(
        snap.len(),
        1,
        "only the bypass entry must be in the snapshot"
    );
    assert_eq!(snap[0].state(), State::Bypassed);
}

#[tokio::test]
async fn bypass_on_one_source_does_not_suppress_other_source() {
    let ingress = get_runtime().await;

    let analog_key = Key::try_from("M:BEAM#Analog").unwrap();
    user_update(&ingress, analog_key, UserAction::Bypass(None), "operator")
        .await
        .unwrap();

    let snap = snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Digital),
    )
    .await;

    assert_eq!(
        snap.len(),
        2,
        "bypass on Analog must not suppress Digital alarm"
    );
    let has_digital_alarm = snap
        .iter()
        .any(|s| s.source() == Source::Digital && s.state() == State::Alarmed);
    assert!(has_digital_alarm, "Digital alarm must be present");
}

#[tokio::test]
async fn unbypass_bypassed_device_succeeds() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();

    user_update(&ingress, key.clone(), UserAction::Bypass(None), "operator")
        .await
        .unwrap();

    let result = user_update(&ingress, key, UserAction::Activate, "operator").await;
    assert!(result.is_ok(), "unbypass of a Bypassed device must succeed");
}

#[tokio::test]
async fn unbypass_non_bypassed_device_returns_state_not_allowed() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let result = user_update(&ingress, key, UserAction::Activate, "operator").await;
    assert!(
        matches!(result, Err(UpdateError::StateNotAllowed(_))),
        "unbypassing a non-bypassed device must return StateNotAllowed"
    );
}

#[tokio::test]
async fn after_unbypass_entry_absent_from_snapshot() {
    let ingress = get_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();

    user_update(&ingress, key.clone(), UserAction::Bypass(None), "operator")
        .await
        .unwrap();

    let result = user_update(&ingress, key, UserAction::Activate, "operator").await;

    assert!(result.is_ok(), "activate should confirm successfully");

    let snap = snapshot(&ingress).await;
    assert!(snap.is_empty(), "snapshot must not contain unbypass");
}

#[tokio::test]
async fn user_snapshot_is_not_structurally_blocked_by_queued_automated_updates() {
    let ingress = get_runtime().await;

    automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    for index in 0..200 {
        automated_update(
            &ingress,
            make_status(&format!("M:LOAD:{index}"), State::Alarmed, Source::Digital),
        )
        .await;
    }

    let snap = timeout(Duration::from_secs(2), snapshot(&ingress))
        .await
        .expect("snapshot should not be blocked behind automated ingress");

    assert!(
        snap.iter().any(|status| status.device == "M:BEAM"),
        "snapshot should still observe coordinator state while automated work is queued"
    );
}

#[tokio::test]
async fn burst_of_same_key_automated_updates_converges_to_latest_snapshot_state() {
    let ingress = get_runtime().await;

    let mut first = make_status("M:BEAM", State::Alarmed, Source::Analog);
    first.severity = Severity::Low as i32;
    automated_update(&ingress, first).await;

    for severity in [Severity::High, Severity::Low, Severity::High] {
        let mut status = make_status("M:BEAM", State::Alarmed, Source::Analog);
        status.severity = severity as i32;
        automated_update(&ingress, status).await;
    }

    sleep(Duration::from_millis(100)).await;
    let snap = snapshot(&ingress).await;

    assert_eq!(
        snap.len(),
        1,
        "same key should still converge to one snapshot entry"
    );
    assert_eq!(
        snap[0].severity(),
        Severity::High,
        "latest meaningful automated state should win after a burst"
    );
}

#[tokio::test]
async fn user_command_is_not_structurally_blocked_by_queued_automated_updates() {
    let ingress = get_runtime().await;

    snapshot_after_automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;

    for index in 0..200 {
        automated_update(
            &ingress,
            make_status(&format!("Z:LOAD:{index}"), State::Alarmed, Source::Digital),
        )
        .await;
    }

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    timeout(
        Duration::from_secs(2),
        user_update(&ingress, key, UserAction::Acknowledge, "operator"),
    )
    .await
    .expect("user command should not be blocked behind automated ingress")
    .expect("acknowledge should still succeed");
}

#[tokio::test]
async fn stale_automated_failure_does_not_block_later_user_command() {
    let ingress = get_throwing_runtime().await;

    automated_update(
        &ingress,
        make_status("M:BEAM", State::Alarmed, Source::Analog),
    )
    .await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let result = timeout(
        Duration::from_secs(2),
        user_update(&ingress, key, UserAction::Acknowledge, "operator"),
    )
    .await;

    assert!(
        result.is_ok(),
        "the earlier automated failure must not prevent the later user command from resolving"
    );
}

#[tokio::test]
async fn kafka_failure_delivers_kafka_write_failed_error() {
    let ingress = get_throwing_runtime().await;

    let key = Key::try_from("M:BEAM#Analog").unwrap();
    let (sender, receiver) = oneshot::channel();
    ingress
        .user_tx
        .try_send(CoordinatorMessage::DomainInput(DomainInput::UserUpdate {
            key,
            action: UserAction::Bypass(None),
            user: "operator".to_string(),
            confirmation: sender,
        }))
        .expect("test user queue should have capacity");

    let result = timeout(Duration::from_secs(2), receiver)
        .await
        .expect("confirmation must arrive within 2 s")
        .expect("machine must not drop the confirmation sender");

    assert!(
        matches!(result, Err(UpdateError::KafkaWriteFailed(_))),
        "Kafka failure must produce KafkaWriteFailed, got: {result:?}"
    );
}
