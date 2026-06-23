//! Workflow handler — per-key effect sequencing between the coordinator and effect workers.
//!
//! The [`WorkflowHandler`] receives [`Job`] values from the coordinator and runs each job through
//! [`WORKFLOW_ORDER`], dispatching one effect at a time and waiting for the outcome before
//! advancing to the next step. When all steps complete it sends a [`JobOutcome`] back to the
//! coordinator.

use std::collections::HashMap;

use tokio::sync::mpsc::{self, error::SendError};
use tracing::error;

use crate::{
    metrics::Metrics,
    model::{
        key::Key,
        publish::{Publish, PublishDetails, PublishOutcome, PublishResult},
        snooze::{SnoozeInput, SnoozeOutcome},
    },
    proto::common::alarm::Status,
};

#[cfg(test)]
mod tests;

/// A unit of work processed by the workflow handler.
///
/// Carries the alarm key, the new status to be persisted, and a flag indicating whether the
/// update was user-initiated or automated.
pub struct Job {
    pub id: u64,
    pub key: Key,
    pub status: Status,
    pub user_initiated: bool,
}

impl From<&Job> for PublishDetails {
    fn from(job: &Job) -> Self {
        PublishDetails {
            key: job.key.clone(),
            status: job.status.clone(),
        }
    }
}

/// The result of a completed [`Job`].
pub enum JobOutcome {
    /// All workflow steps completed successfully.
    Committed(Job),
    /// The job failed at one or more workflow steps.
    Failed(Job),
    /// A snooze timer for the key has elapsed.
    Wake(Key),
}

/// Channel pair connecting the coordinator and the workflow handler.
pub struct CoordinatorWorkflowPort {
    pub job_rx: mpsc::Receiver<Job>,
    pub job_outcome_tx: mpsc::Sender<JobOutcome>,
}
impl CoordinatorWorkflowPort {
    async fn recv(&mut self) -> Option<Job> {
        self.job_rx.recv().await
    }

    async fn send(&self, outgoing: JobOutcome) -> Result<(), SendError<JobOutcome>> {
        self.job_outcome_tx.send(outgoing).await
    }
}

/// Channel pair connecting the workflow handler and the publish engine.
pub struct PublishWorkflowPort {
    pub publish_outcome_rx: mpsc::Receiver<PublishOutcome>,
    pub publish_tx: mpsc::Sender<Publish>,
}
impl PublishWorkflowPort {
    async fn recv(&mut self) -> Option<PublishOutcome> {
        self.publish_outcome_rx.recv().await
    }

    async fn send(&self, outgoing: Publish) -> Result<(), SendError<Publish>> {
        self.publish_tx.send(outgoing).await
    }
}

/// Channel pair connecting the workflow handler and the snooze scheduler.
pub struct SnoozeWorkflowPort {
    pub snooze_outcome_rx: mpsc::Receiver<SnoozeOutcome>,
    pub snooze_tx: mpsc::Sender<SnoozeInput>,
}
impl SnoozeWorkflowPort {
    async fn recv(&mut self) -> Option<SnoozeOutcome> {
        self.snooze_outcome_rx.recv().await
    }

    async fn send(&self, outgoing: SnoozeInput) -> Result<(), SendError<SnoozeInput>> {
        self.snooze_tx.send(outgoing).await
    }
}

#[derive(Clone, Copy)]
enum Effect {
    Publish,
    Snooze,
}

/// The fixed order in which effects are executed for every job.
const WORKFLOW_ORDER: &[Effect] = &[Effect::Snooze, Effect::Publish];

struct TrackedJob {
    job: Job,
    step: usize,
}
impl From<Job> for TrackedJob {
    fn from(value: Job) -> Self {
        TrackedJob {
            job: value,
            step: 0,
        }
    }
}

/// Sequences per-key effect pipelines between the coordinator and the effect workers.
///
/// Maintains one in-flight job per key. When a new job arrives for a key that already has a job
/// in-flight, the new job replaces the tracked job.
pub struct WorkflowHandler {
    coordinator_port: CoordinatorWorkflowPort,
    publish_port: PublishWorkflowPort,
    snooze_port: SnoozeWorkflowPort,
    jobs: HashMap<Key, TrackedJob>,
    metrics: Metrics,
}
impl WorkflowHandler {
    pub fn new(
        coordinator_port: CoordinatorWorkflowPort,
        publish_port: PublishWorkflowPort,
        snooze_port: SnoozeWorkflowPort,
        metrics: Metrics,
    ) -> Self {
        WorkflowHandler {
            coordinator_port,
            publish_port,
            snooze_port,
            jobs: HashMap::new(),
            metrics,
        }
    }

    pub async fn start(mut self) {
        loop {
            tokio::select! {
                biased;

                Some(result) = self.publish_port.recv() => {
                    self.handle_publish_outcome(result).await;
                }

                Some(result) = self.snooze_port.recv() => {
                    self.handle_snooze_result(result).await;
                }

                Some(job) = self.coordinator_port.recv() => {
                    self.handle_new_job(job).await;
                }

                else => break
            }
        }
    }

    async fn handle_new_job(&mut self, job: Job) {
        self.metrics.record_job_dispatched();
        let key = job.key.clone();
        self.add_work(&key, job);
        self.advance_workflow(key).await;
    }

    fn add_work(&mut self, key: &Key, job: Job) {
        self.jobs.insert(key.clone(), job.into());
    }

    fn remove_work(&mut self, key: &Key) -> Job {
        let tracked = self
            .jobs
            .remove(key)
            .expect("Every key must have an associated job");
        tracked.job
    }

    fn lookup_job(&self, key: &Key) -> &Job {
        &self
            .jobs
            .get(key)
            .expect("Every key must have an associated job")
            .job
    }

    async fn advance_workflow(&mut self, key: Key) {
        let next = self
            .jobs
            .get_mut(&key)
            .map(|tracked| {
                let next = tracked.step;
                tracked.step += 1;
                next
            })
            .expect("All workflow jobs have a step counter");
        match WORKFLOW_ORDER.get(next).copied() {
            Some(effect) => {
                if self.dispatch_effect(effect, key.clone()).await.is_err() {
                    self.send_failed(key).await;
                }
            }
            None => {
                error!("Overran workflow steps! Key: {key}");
                self.send_failed(key).await;
            }
        }
    }

    async fn send_failed(&mut self, key: Key) {
        self.metrics.record_job_failed();
        let job = self.remove_work(&key);
        let _ = self.coordinator_port.send(JobOutcome::Failed(job)).await;
    }

    async fn dispatch_effect(&self, effect: Effect, key: Key) -> Result<(), ()> {
        match effect {
            Effect::Snooze => self.snooze(key).await,
            Effect::Publish => self.publish(key).await,
        }
    }

    async fn snooze(&self, key: Key) -> Result<(), ()> {
        let job = self.lookup_job(&key);
        let snooze_input = SnoozeInput {
            key,
            wake: job.status.wake,
        };
        self.snooze_port.send(snooze_input).await.map_err(|_| ())
    }

    async fn publish(&self, key: Key) -> Result<(), ()> {
        let job = self.lookup_job(&key);
        let details = PublishDetails::from(job);
        let publish = if job.user_initiated {
            Publish::User(details)
        } else {
            Publish::Automated(details)
        };
        self.publish_port.send(publish).await.map_err(|_| ())
    }

    async fn handle_snooze_result(&mut self, result: SnoozeOutcome) {
        match result {
            SnoozeOutcome::Accepted { key } => {
                self.advance_workflow(key).await;
            }
            SnoozeOutcome::Expired { key } => {
                self.metrics.record_snooze_wake();
                let _ = self.coordinator_port.send(JobOutcome::Wake(key)).await;
            }
            SnoozeOutcome::InvalidWake { key } => {
                self.send_failed(key).await;
            }
        }
    }

    async fn handle_publish_outcome(&mut self, outcome: PublishOutcome) {
        match outcome {
            PublishOutcome::Batch(batch) => {
                for result in batch {
                    self.dispatch_publish_result(result).await;
                }
            }
            PublishOutcome::Single(single) => {
                self.dispatch_publish_result(single).await;
            }
        }
    }

    async fn dispatch_publish_result(&mut self, result: PublishResult) {
        let succeeded = result.is_ok();
        let key = result.into_inner();
        let job = self.remove_work(&key);
        let outcome = if succeeded {
            self.metrics.record_job_committed();
            JobOutcome::Committed(job)
        } else {
            self.metrics.record_job_failed();
            JobOutcome::Failed(job)
        };

        let _ = self.coordinator_port.send(outcome).await;
    }
}
