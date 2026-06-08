use super::*;
use crate::{
    metrics::Metrics,
    model::user_action::UserAction,
    proto::common::alarm::status::{Severity, Source, State},
    test_utils::{get_runtime, make_status},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, timeout},
};

#[tokio::test]
async fn automated_ingress_coalesces_latest_status_for_same_key_when_queue_is_full() {
    let (tx, mut rx) = mpsc::channel(1);
    let handle = AutomatedIngressHandle::new(tx, Metrics::new());

    let mut first = make_status("M:BEAM", State::Alarmed, Source::Analog);
    first.severity = Severity::Unknown as i32;
    handle
        .send_automated_update(first)
        .await
        .expect("first message should fill the queue");

    let mut second = make_status("M:BEAM", State::Alarmed, Source::Analog);
    second.severity = Severity::Low as i32;
    handle
        .send_automated_update(second)
        .await
        .expect("second message should be coalesced");

    let queued = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("first queued message should arrive")
        .expect("channel should stay open");
    match queued {
        CoordinatorMessage::DomainInput(DomainInput::AutomatedUpdate(status)) => {
            assert_eq!(status.severity(), Severity::Unknown);
        }
        _ => panic!("expected automated update"),
    }

    let latest = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("coalesced latest message should arrive")
        .expect("channel should stay open");
    match latest {
        CoordinatorMessage::DomainInput(DomainInput::AutomatedUpdate(status)) => {
            assert_eq!(status.severity(), Severity::Low);
        }
        _ => panic!("expected automated update"),
    }
}

#[tokio::test]
async fn user_queue_rejects_when_full() {
    let ingress = get_runtime().await;

    for index in 0..10 {
        let (sender, _receiver) = oneshot::channel();
        ingress
            .user_tx
            .try_send(CoordinatorMessage::DomainInput(DomainInput::UserUpdate {
                key: Key::try_from(format!("DEV{index}#Analog").as_str()).unwrap(),
                action: UserAction::Bypass(None),
                user: "operator".to_string(),
                confirmation: sender,
            }))
            .expect("queue should accept messages until capacity is reached");
    }

    let (sender, _receiver) = oneshot::channel();
    let result =
        ingress
            .user_tx
            .try_send(CoordinatorMessage::DomainInput(DomainInput::UserUpdate {
                key: Key::try_from("OVERFLOW#Analog").unwrap(),
                action: UserAction::Bypass(None),
                user: "operator".to_string(),
                confirmation: sender,
            }));

    assert!(
        result.is_err(),
        "queue should reject once capacity is reached"
    );
}

#[tokio::test]
async fn automated_ingress_does_not_coalesce_distinct_keys() {
    let (tx, mut rx) = mpsc::channel(1);
    let handle = AutomatedIngressHandle::new(tx, Metrics::new());

    handle
        .send_automated_update(make_status("M:BEAM", State::Alarmed, Source::Analog))
        .await
        .expect("first message should fill the queue");
    handle
        .send_automated_update(make_status("Z:ACLTST", State::Alarmed, Source::Digital))
        .await
        .expect("second key should be retained separately");

    let first = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("first queued message should arrive")
        .expect("channel should stay open");
    let second = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("second queued message should arrive")
        .expect("channel should stay open");

    let devices = [first, second]
        .into_iter()
        .map(|message| match message {
            CoordinatorMessage::DomainInput(DomainInput::AutomatedUpdate(status)) => status.device,
            _ => panic!("expected automated update"),
        })
        .collect::<Vec<_>>();

    assert!(devices.contains(&"M:BEAM".to_string()));
    assert!(devices.contains(&"Z:ACLTST".to_string()));
}
