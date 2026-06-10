use std::time::Duration;

use tokio::{sync::mpsc, time::sleep};
use tonic::Request;

use super::*;
use crate::{
    proto::{
        common::alarm::{
            Status,
            status::{Severity, Source, State},
        },
        google::protobuf::{Empty, Timestamp},
        services::alarm_commands::{
            AcknowledgeRequest, ActivateRequest, BypassRequest, SnoozeRequest,
        },
    },
    test_utils::get_runtime,
};

async fn make_service() -> AlarmCommandsService {
    let ingress = get_runtime().await;
    AlarmCommandsService {
        user_channel: ingress.user_tx,
        metrics: ingress.metrics,
    }
}

fn alarmed_status(device: &str, source: Source) -> Status {
    Status {
        device: device.to_string(),
        severity: Severity::Low as i32,
        state: State::Alarmed as i32,
        source: source as i32,
        acknowledgeable: true,
        time: Some(Timestamp {
            seconds: 0,
            nanos: 0,
        }),
        epics_type: String::new(),
        user: String::new(),
        wake: None,
    }
}

#[tokio::test]
async fn acknowledge_alarmed_device_returns_ok() {
    let ingress = get_runtime().await;
    let svc = AlarmCommandsService {
        user_channel: ingress.user_tx.clone(),
        metrics: ingress.metrics.clone(),
    };

    ingress
        .automated_tx
        .send_automated_update(alarmed_status("M:BEAM", Source::Analog))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    let req = Request::new(AcknowledgeRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.acknowledge(req).await;
    assert!(resp.is_ok(), "acknowledge of Alarmed device must succeed");
}

#[tokio::test]
async fn acknowledge_unknown_device_returns_invalid_argument() {
    let svc = make_service().await;

    let req = Request::new(AcknowledgeRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    });

    let result = svc.acknowledge(req).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "acknowledging an unknown device must return InvalidArgument"
    );
}

#[tokio::test]
async fn acknowledge_multiple_alarmed_devices_returns_ok() {
    let ingress = get_runtime().await;
    let svc = AlarmCommandsService {
        user_channel: ingress.user_tx.clone(),
        metrics: ingress.metrics.clone(),
    };

    for device in ["M:BEAM", "M:OUTTUNE"] {
        ingress
            .automated_tx
            .send_automated_update(alarmed_status(device, Source::Analog))
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(100)).await;

    let req = Request::new(AcknowledgeRequest {
        devices: vec!["M:BEAM#Analog".to_string(), "M:OUTTUNE#Analog".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.acknowledge(req).await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn bypass_non_bypassed_device_returns_ok() {
    let svc = make_service().await;

    let req = Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.bypass(req).await;
    assert!(resp.is_ok(), "bypass of a non-bypassed device must succeed");
}

#[tokio::test]
async fn bypass_already_bypassed_device_returns_invalid_argument() {
    let svc = make_service().await;

    svc.bypass(Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    }))
    .await
    .unwrap();

    sleep(Duration::from_millis(100)).await;

    let result = svc
        .bypass(Request::new(BypassRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "operator".to_string(),
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "bypassing an already-bypassed device must return InvalidArgument"
    );
}

#[tokio::test]
async fn bypass_multiple_devices_returns_ok() {
    let svc = make_service().await;

    let req = Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string(), "Z:ACLTST#Digital".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.bypass(req).await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn activate_bypassed_device_returns_ok() {
    let svc = make_service().await;

    svc.bypass(Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    }))
    .await
    .unwrap();

    sleep(Duration::from_millis(100)).await;

    let resp = svc
        .activate(Request::new(ActivateRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "operator".to_string(),
        }))
        .await;

    assert!(resp.is_ok(), "activate of a Bypassed device must succeed");
}

#[tokio::test]
async fn activate_non_bypassed_device_returns_invalid_argument() {
    let svc = make_service().await;

    let result = svc
        .activate(Request::new(ActivateRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "operator".to_string(),
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "activating a non-bypassed device must return InvalidArgument"
    );
}

#[tokio::test]
async fn snooze_with_wake_returns_ok() {
    let svc = make_service().await;

    let resp = svc
        .snooze(Request::new(SnoozeRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "operator".to_string(),
            wake: Some(Timestamp {
                seconds: 9_999_999_999,
                nanos: 0,
            }),
        }))
        .await;

    assert!(resp.is_ok(), "snooze with a wake timestamp must succeed");
}

#[tokio::test]
async fn snooze_without_wake_returns_invalid_argument() {
    let svc = make_service().await;

    let result = svc
        .snooze(Request::new(SnoozeRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "operator".to_string(),
            wake: None,
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "snooze without a wake timestamp must return InvalidArgument"
    );
}

#[tokio::test]
async fn snooze_duplicate_wake_returns_invalid_argument() {
    let svc = make_service().await;

    let wake = Some(Timestamp {
        seconds: 9_999_999_999,
        nanos: 0,
    });

    svc.snooze(Request::new(SnoozeRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
        wake,
    }))
    .await
    .unwrap();

    sleep(Duration::from_millis(100)).await;

    let result = svc
        .snooze(Request::new(SnoozeRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "operator".to_string(),
            wake,
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "snoozing with the same wake time must return InvalidArgument"
    );
}

#[tokio::test]
async fn get_snapshot_empty_returns_empty_list() {
    let svc = make_service().await;

    let resp = svc.get_snapshot(Request::new(Empty {})).await.unwrap();
    assert!(
        resp.into_inner().snapshot.is_empty(),
        "snapshot of empty state machine must be empty"
    );
}

#[tokio::test]
async fn get_snapshot_after_bypass_contains_bypassed_entry() {
    let svc = make_service().await;

    svc.bypass(Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    }))
    .await
    .unwrap();

    sleep(Duration::from_millis(100)).await;

    let resp = svc.get_snapshot(Request::new(Empty {})).await.unwrap();
    let snapshot = resp.into_inner().snapshot;

    assert_eq!(
        snapshot.len(),
        1,
        "snapshot must contain the bypassed entry"
    );
    assert_eq!(snapshot[0].device, "M:BEAM");
    assert_eq!(snapshot[0].state(), State::Bypassed);
}

#[tokio::test]
async fn get_snapshot_after_automated_alarm_contains_alarm() {
    let ingress = get_runtime().await;
    let svc = AlarmCommandsService {
        user_channel: ingress.user_tx.clone(),
        metrics: ingress.metrics.clone(),
    };

    ingress
        .automated_tx
        .send_automated_update(alarmed_status("Z:ACLTST", Source::Digital))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    let resp = svc.get_snapshot(Request::new(Empty {})).await.unwrap();
    let snapshot = resp.into_inner().snapshot;

    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].device, "Z:ACLTST");
    assert_eq!(snapshot[0].state(), State::Alarmed);
}

#[tokio::test]
async fn dropped_state_channel_returns_internal_error() {
    let (tx, rx) = mpsc::channel::<CoordinatorMessage>(1);
    drop(rx);

    let metrics = Metrics::new();
    let svc = AlarmCommandsService {
        user_channel: UserIngressHandle::new(tx, metrics.clone()),
        metrics,
    };

    let result = svc
        .acknowledge(Request::new(AcknowledgeRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "op".to_string(),
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::Internal);
}

#[tokio::test]
async fn full_user_queue_returns_resource_exhausted() {
    let queue_capacity = 1;
    let (tx, _rx) = mpsc::channel::<CoordinatorMessage>(queue_capacity);
    for index in 0..queue_capacity {
        tx.try_send(CoordinatorMessage::DomainInput(
            DomainInput::AutomatedUpdate(alarmed_status(&format!("DEV{index}"), Source::Analog)),
        ))
        .expect("queue should accept messages until capacity is reached");
    }
    let metrics = Metrics::new();
    let svc = AlarmCommandsService {
        user_channel: UserIngressHandle::new(tx, metrics.clone()),
        metrics,
    };

    let result = svc
        .acknowledge(Request::new(AcknowledgeRequest {
            devices: vec!["M:BEAM#Analog".to_string()],
            user: "op".to_string(),
        }))
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
}
