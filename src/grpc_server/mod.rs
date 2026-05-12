use crate::{
    proto::{
        google::protobuf::Empty,
        services::alarm_commands::{
            AcknowledgeRequest, ActivateRequest, BypassRequest, SnapshotResponse, SnoozeRequest,
            alarm_commands_server::AlarmCommands,
        },
    },
    report::AlarmsReporter,
};
use rust_pubsub_lib::Publisher;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status as TonicStatus};

#[cfg(test)]
mod tests;

pub struct AlarmCommandsService<P: Publisher> {
    pub reporter: Arc<Mutex<AlarmsReporter<P>>>,
}

#[tonic::async_trait]
impl<P: Publisher + Send + Sync + 'static> AlarmCommands for AlarmCommandsService<P> {
    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        tracing::info!(
            "Acknowledge applied to device-sources: {:?}, user: {}",
            req.devices,
            req.user
        );

        let mut reporter = self.reporter.lock().await;

        for device in req.devices {
            reporter.set_acknowledged(device, req.user.clone());
        }

        Ok(Response::new(Empty {}))
    }

    async fn activate(
        &self,
        request: Request<ActivateRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        tracing::info!(
            "Activate applied to device-sources: {:?}, user: {}",
            req.devices,
            req.user
        );

        let mut reporter = self.reporter.lock().await;

        for device in req.devices {
            reporter.set_active(device, req.user.clone());
        }

        Ok(Response::new(Empty {}))
    }

    async fn bypass(
        &self,
        request: Request<BypassRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        tracing::info!("Bypass applied to device-sources: {:?}", req.devices);

        let mut reporter = self.reporter.lock().await;

        for device in req.devices {
            reporter.set_bypass(device, req.user.clone());
        }

        Ok(Response::new(Empty {}))
    }

    async fn snooze(
        &self,
        request: Request<SnoozeRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();

        tracing::info!(
            "Snooze applied to device-sources: {:?}, wake: {:?}",
            req.devices,
            req.wake
        );

        let wake = req
            .wake
            .ok_or_else(|| TonicStatus::invalid_argument("wake timestamp is required"))?;

        let mut reporter = self.reporter.lock().await;

        for device in req.devices {
            reporter.set_snooze(device, wake, req.user.clone());
        }

        Ok(Response::new(Empty {}))
    }

    async fn get_snapshot(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SnapshotResponse>, TonicStatus> {
        let reporter = self.reporter.lock().await;
        let snapshot = reporter.get_snapshot();
        let response = SnapshotResponse { snapshot };
        Ok(Response::new(response))
    }
}
