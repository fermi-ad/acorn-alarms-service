use crate::proto::common::alarm::{
    Status,
    status::{Severity, State},
};
use crate::proto::google::protobuf::{Empty, Timestamp};

use crate::report::AlarmsReporter;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;

use crate::proto::services::alarm_commands::v1::{
    AcknowledgeAlarmRequest, BypassAlarmRequest, SnoozeAlarmRequest,
    alarm_commands_server::AlarmCommands,
};

use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status as TonicStatus};

pub struct AlarmCommandsService {
    pub reporter: Arc<Mutex<AlarmsReporter<KafkaPublisher>>>,
}

pub fn handle_ack(reporter: &mut AlarmsReporter<KafkaPublisher>, device: String, user: String) {
    let status = Status {
        device,
        severity: Severity::Unknown as i32,
        state: State::Acknowledged as i32,
        source: 0,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        }),
        epics_type: String::default(),
        user,
        wake: None,
    };

    reporter.report(status);
}

#[tonic::async_trait]
impl AlarmCommands for AlarmCommandsService {
    async fn acknowledge_alarm(
        &self,
        request: Request<AcknowledgeAlarmRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();

        let mut reporter = self.reporter.lock().await;

        for device in req.devices {
            handle_ack(&mut reporter, device, req.user.clone());
        }

        Ok(Response::new(Empty {}))
    }

    async fn bypass_alarm(
        &self,
        request: Request<BypassAlarmRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();
        tracing::info!("Bypass applied to devices: {:?}", req.devices);

        let mut reporter = self.reporter.lock().await;

      for device in req.devices {
    reporter.set_bypass(device);  
}

        Ok(Response::new(Empty {}))
    }

    async fn snooze_alarm(
        &self,
        request: Request<SnoozeAlarmRequest>,
    ) -> Result<Response<Empty>, TonicStatus> {
        let req = request.into_inner();

        tracing::info!(
            "Snooze applied to devices: {:?}, duration: {}s",
            req.devices,
            req.duration_seconds
        );

        let mut reporter = self.reporter.lock().await;

        for device in req.devices {
            reporter.set_snooze(device, req.duration_seconds);
        }

        Ok(Response::new(Empty {}))
    }
}
