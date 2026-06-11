//! Runtime wiring for the alarm state ingress and background tasks.

pub mod hydration;

use rust_pubsub_lib::Publisher;
use tokio::sync::mpsc;

use crate::{
    effects::publish::run_publish_engine,
    engine::{
        coordinator::AlarmStateCoordinator,
        ingress::{AutomatedIngressHandle, UserIngressHandle},
    },
    metrics::{Metrics, QueueKind},
    runtime::hydration::HydratedStatuses,
};

/// Handles for submitting automated and user-driven ingress messages.
pub struct AlarmStateIngress {
    pub automated_tx: AutomatedIngressHandle,
    pub user_tx: UserIngressHandle,
    pub metrics: Metrics,
}

/// The configured queue capacities for the runtime.
pub struct QueueCapacityConfig {
    pub automated: usize,
    pub priority: usize,
    pub effect: usize,
}

/// Starts the runtime tasks and returns ingress handles for external callers.
pub async fn start<P: Publisher + Send + Sync + 'static>(
    publisher: P,
    queue_config: QueueCapacityConfig,
    hydrated_statuses: HydratedStatuses,
) -> AlarmStateIngress {
    let metrics = Metrics::new();
    metrics.set_queue_capacity(QueueKind::Automated, queue_config.automated);
    metrics.set_queue_capacity(QueueKind::Priority, queue_config.priority);
    metrics.set_queue_capacity(QueueKind::Effect, queue_config.effect);

    let (automated_tx, automated_rx) = mpsc::channel(queue_config.automated);
    let (priority_tx, priority_rx) = mpsc::channel(queue_config.priority);
    let (effect_tx, effect_rx) = mpsc::channel(queue_config.effect);

    let automated_handle = AutomatedIngressHandle::new(automated_tx.clone(), metrics.clone());
    let user_handle = UserIngressHandle::new(priority_tx.clone(), metrics.clone());
    tokio::spawn(run_publish_engine(
        publisher,
        effect_rx,
        priority_tx,
        metrics.clone(),
    ));

    let coordinator =
        AlarmStateCoordinator::new(automated_rx, priority_rx, effect_tx, hydrated_statuses);
    tokio::spawn(coordinator.start());

    AlarmStateIngress {
        automated_tx: automated_handle,
        user_tx: user_handle,
        metrics,
    }
}
