//! gRPC adapter for user-driven alarm commands.

use std::time::Instant;

use futures::future::join_all;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status as TonicStatus};
use tracing::{debug, error, warn};

use crate::{
    engine::{
        ingress::{ChannelSendError, UserIngressHandle},
        messages::{CoordinatorMessage, DomainInput},
    },
    metrics::Metrics,
    model::{errors::UpdateError, key::Key, user_action::UserAction},
    proto::{
        google::protobuf::Empty,
        services::alarm_commands::{
            AcknowledgeRequest, ActivateRequest, BypassRequest, SnapshotResponse, SnoozeRequest,
            alarm_commands_server::AlarmCommands,
        },
    },
};

#[cfg(test)]
mod tests;

type Confirmation = oneshot::Receiver<Result<(), UpdateError>>;

pub struct AlarmCommandsService {
    pub user_channel: UserIngressHandle,
    pub metrics: Metrics,
}

impl AlarmCommandsService {
    async fn submit_and_confirm_user_update(
        &self,
        devices: &[String],
        action: UserAction,
        user: &str,
    ) -> Result<Response<Empty>, TonicStatus> {
        let started_at = Instant::now();
        let keys = build_keys(devices)?;
        let results_fut = self.submit_updates(&keys, action, user)?;

        handle_confirmation(results_fut, &keys, &self.metrics, started_at).await
    }

    fn submit_updates(
        &self,
        keys: &[Key],
        action: UserAction,
        user: &str,
    ) -> Result<Vec<Confirmation>, TonicStatus> {
        let mut results_fut = Vec::new();

        for key in keys {
            let (sender, receiver) = oneshot::channel();
            results_fut.push(receiver);

            if let Err(e) = self.user_channel.try_send(CoordinatorMessage::DomainInput(
                DomainInput::UserUpdate {
                    key: key.clone(),
                    action,
                    user: user.to_string(),
                    confirmation: sender,
                },
            )) {
                return Err(map_user_send_error(e));
            }
        }

        Ok(results_fut)
    }
}

#[tonic::async_trait]
impl AlarmCommands for AlarmCommandsService {
    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        debug!(
            "Acknowledge request for device-sources: {:?}, user: {}",
            req.devices, req.user
        );

        self.submit_and_confirm_user_update(&req.devices, UserAction::Acknowledge, &req.user)
            .await
    }

    async fn activate(
        &self,
        request: Request<ActivateRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        debug!(
            "Activate request for device-sources: {:?}, user: {}",
            req.devices, req.user
        );

        self.submit_and_confirm_user_update(&req.devices, UserAction::Activate, &req.user)
            .await
    }

    async fn bypass(
        &self,
        request: Request<BypassRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        debug!(
            "Bypass request for device-sources: {:?}, user: {}",
            req.devices, req.user
        );

        self.submit_and_confirm_user_update(&req.devices, UserAction::Bypass(None), &req.user)
            .await
    }

    async fn snooze(
        &self,
        request: Request<SnoozeRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        debug!(
            "Snooze request for device-sources: {:?}, wake: {:?}, user: {}",
            req.devices, req.wake, req.user
        );

        if req.wake.is_none() {
            return Err(TonicStatus::invalid_argument("wake timestamp is required"));
        };

        self.submit_and_confirm_user_update(&req.devices, UserAction::Bypass(req.wake), &req.user)
            .await
    }

    async fn get_snapshot(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SnapshotResponse>, TonicStatus> {
        debug!("Snapshot requested");
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.user_channel.try_send(CoordinatorMessage::DomainInput(
            DomainInput::SnapshotRequest(sender),
        )) {
            return Err(map_user_send_error(e));
        }
        match receiver.await {
            Ok(snapshot) => {
                let response = SnapshotResponse { snapshot };
                Ok(Response::new(response))
            }
            Err(e) => {
                error!("{e}");
                Err(TonicStatus::internal(
                    "The alarms state machine has shut down!",
                ))
            }
        }
    }
}

fn build_keys(devices: &[String]) -> Result<Vec<Key>, TonicStatus> {
    let mut keys = Vec::new();
    let mut errs = Vec::new();

    for device_source in devices {
        match Key::try_from(device_source.as_str()) {
            Ok(key) => keys.push(key),
            Err(text) => errs.push(text),
        }
    }

    if errs.is_empty() {
        Ok(keys)
    } else {
        Err(TonicStatus::invalid_argument(errs.join("; ")))
    }
}

async fn handle_confirmation(
    results_fut: Vec<Confirmation>,
    keys: &[Key],
    metrics: &Metrics,
    started_at: Instant,
) -> Result<Response<Empty>, TonicStatus> {
    let mut bad_requests = Vec::new();
    let mut internal_errs = Vec::new();

    for (index, result) in join_all(results_fut).await.into_iter().enumerate() {
        match result {
            Err(e) => {
                error!("{e}");
                internal_errs.push(format!(
                    "State machine is down. Did not process {}.",
                    keys[index]
                ));
            }
            Ok(Err(e)) => match e {
                UpdateError::KafkaWriteFailed(_) => {
                    internal_errs.push(e.to_string());
                }
                UpdateError::StateNotAllowed(_) => {
                    bad_requests.push(e.to_string());
                }
            },
            _ => (),
        }
    }

    metrics.record_confirmation_latency(started_at.elapsed());
    build_grpc_result(bad_requests, internal_errs)
}

fn build_grpc_result(
    bad_requests: Vec<String>,
    internal_errs: Vec<String>,
) -> Result<Response<Empty>, TonicStatus> {
    if bad_requests.is_empty() && internal_errs.is_empty() {
        Ok(Response::new(Empty {}))
    } else if internal_errs.is_empty() {
        Err(TonicStatus::invalid_argument(bad_requests.join("; ")))
    } else {
        let bad_request_msg = if bad_requests.is_empty() {
            String::new()
        } else {
            format!(
                "\n - \nAdditionally, some requests were malformed: {}",
                bad_requests.join("; ")
            )
        };

        Err(TonicStatus::internal(format!(
            "Failed processing requests: {}{}",
            internal_errs.join(";"),
            bad_request_msg
        )))
    }
}

fn map_user_send_error(error: ChannelSendError) -> TonicStatus {
    match error {
        ChannelSendError::Full => {
            warn!("Rejecting user command because the coordinator user queue is full");
            TonicStatus::resource_exhausted(
                "Alarm command queue is full; rejecting request instead of waiting behind backlog",
            )
        }
        ChannelSendError::Closed => {
            error!("The alarms state machine has shut down!");
            TonicStatus::internal("The alarms state machine has shut down!")
        }
    }
}
