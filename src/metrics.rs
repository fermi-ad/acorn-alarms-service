//! Low-cardinality in-process metrics for overload, queue pressure proxies, retries, latency,
//! workflow throughput, and snooze timer activity.
//!
//! Metrics are recorded synchronously using cheap atomics and mutex-protected histograms.
//! Export is intentionally left out of this module so hot paths remain simple and non-blocking.
//!
//! ## Counter and gauge groups
//!
//! - **Overload / ingress** (`overload_entries`, `overload_exits`, `automated_queue_full`,
//!   `user_queue_full_rejections`): track queue pressure and overload mode transitions.
//! - **Publish engine** (`publish_attempts`, `publish_retries`, `publish_failures`,
//!   `in_flight_publish_attempts`): track Kafka publish activity and retry behavior.
//! - **Workflow handler** (`jobs_dispatched`, `jobs_committed`, `jobs_failed`, `snooze_wakes`):
//!   track job throughput through the per-key effect pipeline.
//! - **Snooze scheduler** (`snooze_sets`, `snooze_cancels`, `snooze_invalid_wakes`,
//!   `snooze_expirations`, `in_flight_snooze_timers`): track timer activity in the snooze
//!   scheduler.
//! - **Queue capacities** (`automated_queue_capacity`, `user_queue_capacity`,
//!   `job_queue_capacity`, `publish_queue_capacity`, `snooze_queue_capacity`): fixed at
//!   construction from [`QueueCapacityConfig`]; reflected in every snapshot without mutation.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::runtime::QueueCapacityConfig;

#[cfg(test)]
mod tests;

const CONFIRMATION_LATENCY_BUCKETS_MS: &[u64] = &[10, 50, 100, 250, 500, 1_000, 2_000];
const PUBLISH_LATENCY_BUCKETS_MS: &[u64] = &[5, 10, 25, 50, 100, 250, 500, 1_000];
const OVERLOAD_DURATION_BUCKETS_MS: &[u64] = &[10, 50, 100, 250, 500, 1_000, 5_000];

/// Outcome kind for a single publish attempt, used with [`Metrics::record_publish_completion`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishOutcomeKind {
    Success,
    Failure,
}

/// Cheap, clone-friendly handle to the shared metrics state.
///
/// All methods are lock-free except the histogram observers, which take a short `Mutex` lock.
#[derive(Clone, Debug)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    // ── overload / ingress ──────────────────────────────────────────────────
    overload_entries: AtomicU64,
    overload_exits: AtomicU64,
    automated_queue_full: AtomicU64,
    user_queue_full_rejections: AtomicU64,

    // ── publish engine ──────────────────────────────────────────────────────
    publish_attempts: AtomicU64,
    publish_retries: AtomicU64,
    publish_failures: AtomicU64,

    // ── workflow handler ────────────────────────────────────────────────────
    jobs_dispatched: AtomicU64,
    jobs_committed: AtomicU64,
    jobs_failed: AtomicU64,
    snooze_wakes: AtomicU64,

    // ── snooze scheduler ────────────────────────────────────────────────────
    snooze_sets: AtomicU64,
    snooze_cancels: AtomicU64,
    snooze_invalid_wakes: AtomicU64,
    snooze_expirations: AtomicU64,

    // ── queue capacities (set at construction, never mutated) ───────────────
    queue_configured_capacity: QueueCapacityMetrics,

    // ── in-flight gauges ────────────────────────────────────────────────────
    retained_automated_keys: AtomicUsize,
    /// Number of publish attempts currently outstanding inside the publish engine.
    ///
    /// This counts work that has been handed to the Kafka producer but whose outcome has not yet
    /// been received. It does **not** count jobs queued in the workflow handler waiting to reach
    /// the publish step.
    in_flight_publish_attempts: AtomicUsize,
    /// Number of snooze timers currently active inside the snooze scheduler's `DelayQueue`.
    ///
    /// Incremented when a `Snooze::Set` is accepted; decremented when the timer fires
    /// (`SnoozeOutcome::Expired`) or is cancelled (`Snooze::Cancel`).
    in_flight_snooze_timers: AtomicUsize,

    // ── histograms ──────────────────────────────────────────────────────────
    confirmation_latency_ms: Mutex<Histogram>,
    publish_latency_ms: Mutex<Histogram>,
    overload_duration_ms: Mutex<Histogram>,
}

#[derive(Debug)]
struct QueueCapacityMetrics {
    automated: usize,
    user: usize,
    job: usize,
    publish: usize,
    snooze: usize,
}

/// Point-in-time snapshot of all metrics.  Cheap to clone and log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsSnapshot {
    // ── overload / ingress ──────────────────────────────────────────────────
    pub overload_entries: u64,
    pub overload_exits: u64,
    pub automated_queue_full: u64,
    pub user_queue_full_rejections: u64,

    // ── publish engine ──────────────────────────────────────────────────────
    pub publish_attempts: u64,
    pub publish_retries: u64,
    pub publish_failures: u64,

    // ── workflow handler ────────────────────────────────────────────────────
    pub jobs_dispatched: u64,
    pub jobs_committed: u64,
    pub jobs_failed: u64,
    pub snooze_wakes: u64,

    // ── snooze scheduler ────────────────────────────────────────────────────
    pub snooze_sets: u64,
    pub snooze_cancels: u64,
    pub snooze_invalid_wakes: u64,
    pub snooze_expirations: u64,

    // ── queue capacities ────────────────────────────────────────────────────
    pub automated_queue_capacity: usize,
    pub user_queue_capacity: usize,
    pub job_queue_capacity: usize,
    pub publish_queue_capacity: usize,
    pub snooze_queue_capacity: usize,

    // ── in-flight gauges ────────────────────────────────────────────────────
    pub retained_automated_keys: usize,
    pub in_flight_publish_attempts: usize,
    pub in_flight_snooze_timers: usize,

    // ── histograms ──────────────────────────────────────────────────────────
    pub confirmation_latency_ms: HistogramSnapshot,
    pub publish_latency_ms: HistogramSnapshot,
    pub overload_duration_ms: HistogramSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistogramSnapshot {
    pub buckets_ms: Vec<u64>,
    pub counts: Vec<u64>,
    pub overflow: u64,
    pub samples: u64,
    pub total_ms: u64,
}

#[derive(Debug)]
struct Histogram {
    buckets_ms: Vec<u64>,
    counts: Vec<u64>,
    overflow: u64,
    samples: u64,
    total_ms: u64,
}

impl Metrics {
    /// Construct a new `Metrics` instance.
    ///
    /// Queue capacities are fixed at construction time from `queue_config` and are reflected in
    /// every subsequent [`snapshot`](Self::snapshot) without further mutation.
    pub fn new(queue_config: &QueueCapacityConfig) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                overload_entries: AtomicU64::new(0),
                overload_exits: AtomicU64::new(0),
                automated_queue_full: AtomicU64::new(0),
                user_queue_full_rejections: AtomicU64::new(0),
                publish_attempts: AtomicU64::new(0),
                publish_retries: AtomicU64::new(0),
                publish_failures: AtomicU64::new(0),
                jobs_dispatched: AtomicU64::new(0),
                jobs_committed: AtomicU64::new(0),
                jobs_failed: AtomicU64::new(0),
                snooze_wakes: AtomicU64::new(0),
                snooze_sets: AtomicU64::new(0),
                snooze_cancels: AtomicU64::new(0),
                snooze_invalid_wakes: AtomicU64::new(0),
                snooze_expirations: AtomicU64::new(0),
                queue_configured_capacity: QueueCapacityMetrics::from(queue_config),
                retained_automated_keys: AtomicUsize::new(0),
                in_flight_publish_attempts: AtomicUsize::new(0),
                in_flight_snooze_timers: AtomicUsize::new(0),
                confirmation_latency_ms: Mutex::new(Histogram::new(
                    CONFIRMATION_LATENCY_BUCKETS_MS,
                )),
                publish_latency_ms: Mutex::new(Histogram::new(PUBLISH_LATENCY_BUCKETS_MS)),
                overload_duration_ms: Mutex::new(Histogram::new(OVERLOAD_DURATION_BUCKETS_MS)),
            }),
        }
    }

    // ── ingress / overload ──────────────────────────────────────────────────

    pub fn record_automated_queue_full(&self) {
        self.inner
            .automated_queue_full
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_user_queue_full_rejection(&self) {
        self.inner
            .user_queue_full_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_overload_entry(&self, retained_keys: usize) {
        self.inner.overload_entries.fetch_add(1, Ordering::Relaxed);
        self.inner
            .retained_automated_keys
            .store(retained_keys, Ordering::Relaxed);
    }

    pub fn record_retained_automated_keys(&self, retained_keys: usize) {
        self.inner
            .retained_automated_keys
            .store(retained_keys, Ordering::Relaxed);
    }

    pub fn record_overload_exit(&self, duration: Duration) {
        self.inner.overload_exits.fetch_add(1, Ordering::Relaxed);
        self.inner
            .retained_automated_keys
            .store(0, Ordering::Relaxed);
        self.inner
            .overload_duration_ms
            .lock()
            .expect("overload histogram lock poisoned")
            .observe(duration);
    }

    // ── publish engine ──────────────────────────────────────────────────────

    pub fn record_publish_attempt_started(&self) {
        self.inner.publish_attempts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .in_flight_publish_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_publish_retry(&self) {
        self.inner.publish_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the completion of a single publish attempt.
    ///
    /// Decrements `in_flight_publish_attempts` and observes the latency histogram.
    /// Only `Failure` increments a counter; `Success` is the happy path and needs no counter.
    pub fn record_publish_completion(&self, outcome: PublishOutcomeKind, latency: Duration) {
        self.inner
            .in_flight_publish_attempts
            .fetch_sub(1, Ordering::Relaxed);
        self.inner
            .publish_latency_ms
            .lock()
            .expect("publish histogram lock poisoned")
            .observe(latency);

        match outcome {
            PublishOutcomeKind::Success => {}
            PublishOutcomeKind::Failure => {
                self.inner.publish_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_confirmation_latency(&self, latency: Duration) {
        self.inner
            .confirmation_latency_ms
            .lock()
            .expect("confirmation histogram lock poisoned")
            .observe(latency);
    }

    // ── workflow handler ────────────────────────────────────────────────────

    /// Increment the count of jobs received by the workflow handler.
    pub fn record_job_dispatched(&self) {
        self.inner.jobs_dispatched.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the count of jobs that completed with `JobOutcome::Committed`.
    pub fn record_job_committed(&self) {
        self.inner.jobs_committed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the count of jobs that completed with `JobOutcome::Failed`.
    pub fn record_job_failed(&self) {
        self.inner.jobs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the count of snooze timer expirations that produced `JobOutcome::Wake`.
    pub fn record_snooze_wake(&self) {
        self.inner.snooze_wakes.fetch_add(1, Ordering::Relaxed);
    }

    // ── snooze scheduler ────────────────────────────────────────────────────

    /// Increment the count of `Snooze::Set` commands accepted with a valid future timestamp.
    pub fn record_snooze_set(&self) {
        self.inner.snooze_sets.fetch_add(1, Ordering::Relaxed);
        self.inner
            .in_flight_snooze_timers
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the count of `Snooze::Cancel` commands that removed a live timer.
    ///
    /// This is only called when a timer was actually present in the `DelayQueue`; cancelling a
    /// non-existent timer is a no-op and is not counted.  Also decrements
    /// `in_flight_snooze_timers`.
    pub fn record_snooze_cancel(&self) {
        self.inner.snooze_cancels.fetch_add(1, Ordering::Relaxed);
        self.inner
            .in_flight_snooze_timers
            .fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment the count of `Snooze::Set` commands rejected due to an invalid (past) timestamp.
    pub fn record_snooze_invalid_wake(&self) {
        self.inner
            .snooze_invalid_wakes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the count of snooze timers that fired and emitted `SnoozeOutcome::Expired`.
    ///
    /// Also decrements `in_flight_snooze_timers`.
    pub fn record_snooze_expiration(&self) {
        self.inner
            .snooze_expirations
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .in_flight_snooze_timers
            .fetch_sub(1, Ordering::Relaxed);
    }

    // ── snapshot ────────────────────────────────────────────────────────────

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            overload_entries: self.inner.overload_entries.load(Ordering::Relaxed),
            overload_exits: self.inner.overload_exits.load(Ordering::Relaxed),
            automated_queue_full: self.inner.automated_queue_full.load(Ordering::Relaxed),
            user_queue_full_rejections: self
                .inner
                .user_queue_full_rejections
                .load(Ordering::Relaxed),
            publish_attempts: self.inner.publish_attempts.load(Ordering::Relaxed),
            publish_retries: self.inner.publish_retries.load(Ordering::Relaxed),
            publish_failures: self.inner.publish_failures.load(Ordering::Relaxed),
            jobs_dispatched: self.inner.jobs_dispatched.load(Ordering::Relaxed),
            jobs_committed: self.inner.jobs_committed.load(Ordering::Relaxed),
            jobs_failed: self.inner.jobs_failed.load(Ordering::Relaxed),
            snooze_wakes: self.inner.snooze_wakes.load(Ordering::Relaxed),
            snooze_sets: self.inner.snooze_sets.load(Ordering::Relaxed),
            snooze_cancels: self.inner.snooze_cancels.load(Ordering::Relaxed),
            snooze_invalid_wakes: self.inner.snooze_invalid_wakes.load(Ordering::Relaxed),
            snooze_expirations: self.inner.snooze_expirations.load(Ordering::Relaxed),
            automated_queue_capacity: self.inner.queue_configured_capacity.automated,
            user_queue_capacity: self.inner.queue_configured_capacity.user,
            job_queue_capacity: self.inner.queue_configured_capacity.job,
            publish_queue_capacity: self.inner.queue_configured_capacity.publish,
            snooze_queue_capacity: self.inner.queue_configured_capacity.snooze,
            retained_automated_keys: self.inner.retained_automated_keys.load(Ordering::Relaxed),
            in_flight_publish_attempts: self
                .inner
                .in_flight_publish_attempts
                .load(Ordering::Relaxed),
            in_flight_snooze_timers: self.inner.in_flight_snooze_timers.load(Ordering::Relaxed),
            confirmation_latency_ms: self
                .inner
                .confirmation_latency_ms
                .lock()
                .expect("confirmation histogram lock poisoned")
                .snapshot(),
            publish_latency_ms: self
                .inner
                .publish_latency_ms
                .lock()
                .expect("publish histogram lock poisoned")
                .snapshot(),
            overload_duration_ms: self
                .inner
                .overload_duration_ms
                .lock()
                .expect("overload histogram lock poisoned")
                .snapshot(),
        }
    }
}

impl From<&QueueCapacityConfig> for QueueCapacityMetrics {
    fn from(value: &QueueCapacityConfig) -> Self {
        Self {
            automated: value.automated,
            user: value.user,
            job: value.job,
            publish: value.publish,
            snooze: value.snooze,
        }
    }
}

impl Histogram {
    fn new(buckets_ms: &[u64]) -> Self {
        Self {
            buckets_ms: buckets_ms.to_vec(),
            counts: vec![0; buckets_ms.len()],
            overflow: 0,
            samples: 0,
            total_ms: 0,
        }
    }

    fn observe(&mut self, duration: Duration) {
        let millis = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.samples += 1;
        self.total_ms = self.total_ms.saturating_add(millis);

        if let Some(index) = self.buckets_ms.iter().position(|bucket| millis <= *bucket) {
            self.counts[index] += 1;
        } else {
            self.overflow += 1;
        }
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets_ms: self.buckets_ms.clone(),
            counts: self.counts.clone(),
            overflow: self.overflow,
            samples: self.samples,
            total_ms: self.total_ms,
        }
    }
}
