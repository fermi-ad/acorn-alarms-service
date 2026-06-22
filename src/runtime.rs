//! Runtime wiring for the alarm state ingress and background tasks.

use rust_pubsub_lib::Publisher;
use tokio::sync::mpsc;

use crate::{
    effects::{
        publish::{PublishEffectPort, run_publish_engine},
        snooze::{SnoozeEffectPort, run_snooze_scheduler},
    },
    engine::{
        coordinator::{AlarmStateCoordinator, JobPort},
        ingress::{AutomatedIngressHandle, UserIngressHandle},
        workflow::{
            CoordinatorWorkflowPort, PublishWorkflowPort, SnoozeWorkflowPort, WorkflowHandler,
        },
    },
    metrics::Metrics,
    runtime::hydration::HydratedStatuses,
};

pub mod hydration;

/// Handles for submitting automated and user-driven ingress messages.
pub struct AlarmStateIngress {
    pub automated_tx: AutomatedIngressHandle,
    pub user_tx: UserIngressHandle,
    pub metrics: Metrics,
}

/// Configured queue capacities for all runtime channels.
///
/// Each field controls the bounded capacity of one channel pair in the runtime topology:
///
/// - `automated` — automated ingress queue (Redis adapter → coordinator)
/// - `user` — user ingress queue (gRPC adapter → coordinator)
/// - `job` — job queue (coordinator → workflow handler, and outcome channel back)
/// - `publish` — publish queue (workflow handler → publish engine, and outcome channel back)
/// - `snooze` — snooze queue (workflow handler → snooze scheduler, and outcome channel back)
pub struct QueueCapacityConfig {
    pub automated: usize,
    pub user: usize,
    pub job: usize,
    pub publish: usize,
    pub snooze: usize,
}

/// Starts all runtime tasks and returns ingress handles for external callers.
///
/// Creates the bounded channel pairs that form the runtime topology, then spawns the publish
/// engine, snooze scheduler, workflow handler, and coordinator as background tasks.
///
/// Returns [`AlarmStateIngress`] handles so adapters can submit domain inputs without holding
/// references to the internal channel ends.
pub async fn start<P: Publisher + Send + Sync + 'static>(
    publisher: P,
    queue_config: QueueCapacityConfig,
    hydrated_statuses: HydratedStatuses,
) -> AlarmStateIngress {
    let metrics = Metrics::new(&queue_config);

    let (automated_tx, automated_rx) = mpsc::channel(queue_config.automated);
    let (user_tx, user_rx) = mpsc::channel(queue_config.user);
    let (job_tx, job_rx) = mpsc::channel(queue_config.job);
    let (job_outcome_tx, job_outcome_rx) = mpsc::channel(queue_config.job);
    let (publish_tx, publish_rx) = mpsc::channel(queue_config.publish);
    let (publish_outcome_tx, publish_outcome_rx) = mpsc::channel(queue_config.publish);
    let (snooze_tx, snooze_rx) = mpsc::channel(queue_config.snooze);
    let (snooze_outcome_tx, snooze_outcome_rx) = mpsc::channel(queue_config.snooze);

    let automated_handle = AutomatedIngressHandle::new(automated_tx.clone(), metrics.clone());
    let user_handle = UserIngressHandle::new(user_tx.clone(), metrics.clone());
    tokio::spawn(run_publish_engine(
        publisher,
        PublishEffectPort {
            publish_rx,
            publish_outcome_tx,
        },
        metrics.clone(),
    ));

    tokio::spawn(run_snooze_scheduler(
        SnoozeEffectPort {
            snooze_rx,
            snooze_outcome_tx,
        },
        metrics.clone(),
    ));

    let workflow_handler = WorkflowHandler::new(
        CoordinatorWorkflowPort {
            job_rx,
            job_outcome_tx,
        },
        PublishWorkflowPort {
            publish_outcome_rx,
            publish_tx,
        },
        SnoozeWorkflowPort {
            snooze_outcome_rx,
            snooze_tx,
        },
        metrics.clone(),
    );
    tokio::spawn(workflow_handler.start());

    let coordinator = AlarmStateCoordinator::new(
        automated_rx,
        user_rx,
        JobPort {
            job_tx,
            job_outcome_rx,
        },
        hydrated_statuses,
    );
    tokio::spawn(coordinator.start());

    AlarmStateIngress {
        automated_tx: automated_handle,
        user_tx: user_handle,
        metrics,
    }
}
