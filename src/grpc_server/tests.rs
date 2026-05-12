use super::*;
use crate::{
    proto::{
        common::alarm::{
            Status,
            status::{Severity, Source, State},
        },
        google::protobuf::Timestamp,
        services::alarm_commands::{
            AcknowledgeRequest, ActivateRequest, BypassRequest, SnoozeRequest,
        },
    },
    report::AlarmsReporter,
    test_utils::TestPub,
};

fn make_service() -> AlarmCommandsService<TestPub> {
    AlarmCommandsService {
        reporter: Arc::new(Mutex::new(AlarmsReporter::<TestPub>::new())),
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
        epics_type: String::default(),
        user: String::default(),
        wake: None,
    }
}

#[tokio::test]
async fn acknowledge_calls_report_for_each_device_source() {
    let svc = make_service();

    // Pre-populate both device-sources as Alarmed so the Acknowledged
    // transition is allowed.  The devices field uses DEVICE#Source format.
    {
        let mut reporter = svc.reporter.lock().await;
        reporter.report(alarmed_status("M:BEAM", Source::Analog));
        reporter.report(alarmed_status("M:OUTTUNE", Source::Analog));
    }

    let req = Request::new(AcknowledgeRequest {
        devices: vec!["M:BEAM#Analog".to_string(), "M:OUTTUNE#Analog".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.acknowledge(req).await;
    assert!(resp.is_ok());

    // After acknowledgement both device-sources should be in Acknowledged state
    let reporter = svc.reporter.lock().await;
    let snapshot = reporter.get_snapshot();
    assert_eq!(snapshot.len(), 2);
    assert!(snapshot.iter().all(|s| s.state() == State::Acknowledged));
}

#[tokio::test]
async fn snooze_returns_invalid_argument_when_wake_missing() {
    let svc = make_service();

    let req = Request::new(SnoozeRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
        wake: None,
    });

    let result = svc.snooze(req).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn bypass_delegates_to_reporter() {
    let svc = make_service();

    let req = Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.bypass(req).await;
    assert!(resp.is_ok());

    let reporter = svc.reporter.lock().await;
    let snapshot = reporter.get_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].device, "M:BEAM");
    assert_eq!(snapshot[0].source(), Source::Analog);
    assert_eq!(snapshot[0].state(), State::Bypassed);
}

#[tokio::test]
async fn get_snapshot_returns_reporter_snapshot() {
    let svc = make_service();

    // Pre-populate via bypass so there is something in the snapshot
    {
        let mut reporter = svc.reporter.lock().await;
        reporter.set_bypass("M:BEAM#Analog".to_string(), "operator".to_string());
    }

    let req = Request::new(Empty {});
    let resp = svc.get_snapshot(req).await.unwrap();
    let snapshot = resp.into_inner().snapshot;

    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].device, "M:BEAM");
    assert_eq!(snapshot[0].source(), Source::Analog);
}

#[tokio::test]
async fn activate_removes_bypass_and_publishes_unbypassed() {
    let svc = make_service();

    // First bypass the device-source
    {
        let mut reporter = svc.reporter.lock().await;
        reporter.set_bypass("M:BEAM#Analog".to_string(), "operator".to_string());
    }

    // Now activate (remove bypass) for the same device-source
    let req = Request::new(ActivateRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    });

    let resp = svc.activate(req).await;
    assert!(resp.is_ok());

    // Bypass entry should be gone from the snapshot
    let reporter = svc.reporter.lock().await;
    let snapshot = reporter.get_snapshot();
    assert!(snapshot.is_empty());
}

#[tokio::test]
async fn bypass_on_one_source_does_not_affect_other_sources() {
    let svc = make_service();

    // Pre-populate M:BEAM with an Analog alarm
    {
        let mut reporter = svc.reporter.lock().await;
        reporter.report(alarmed_status("M:BEAM", Source::Analog));
        reporter.report(alarmed_status("M:BEAM", Source::Digital));
    }

    // Bypass only the Analog source
    let req = Request::new(BypassRequest {
        devices: vec!["M:BEAM#Analog".to_string()],
        user: "operator".to_string(),
    });
    let resp = svc.bypass(req).await;
    assert!(resp.is_ok());

    let reporter = svc.reporter.lock().await;
    let snapshot = reporter.get_snapshot();

    // Both entries should be present: Analog as Bypassed, Digital as Alarmed
    assert_eq!(snapshot.len(), 2);

    let analog = snapshot
        .iter()
        .find(|s| s.source() == Source::Analog)
        .expect("Analog entry must be present");
    assert_eq!(analog.state(), State::Bypassed);

    let digital = snapshot
        .iter()
        .find(|s| s.source() == Source::Digital)
        .expect("Digital entry must be present");
    assert_eq!(digital.state(), State::Alarmed);
}
