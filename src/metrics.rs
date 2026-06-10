//! Low-cardinality in-process metrics for overload, queue pressure proxies, retries, and latency.
//!
//! The service records metrics synchronously with cheap atomics and mutex-protected histograms.
//! Export is intentionally left out of this module so hot paths remain simple and non-blocking.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
mod tests;

const CONFIRMATION_LATENCY_BUCKETS_MS: &[u64] = &[10, 50, 100, 250, 500, 1_000, 2_000];
const PUBLISH_LATENCY_BUCKETS_MS: &[u64] = &[5, 10, 25, 50, 100, 250, 500, 1_000];
const OVERLOAD_DURATION_BUCKETS_MS: &[u64] = &[10, 50, 100, 250, 500, 1_000, 5_000];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueKind {
    Automated,
    Priority,
    Effect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishOutcomeKind {
    Success,
    Failure,
    Superseded,
}

#[derive(Clone, Debug)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    overload_entries: AtomicU64,
    overload_exits: AtomicU64,
    automated_queue_full: AtomicU64,
    user_queue_full_rejections: AtomicU64,
    publish_attempts: AtomicU64,
    publish_retries: AtomicU64,
    publish_failures: AtomicU64,
    publish_superseded: AtomicU64,
    queue_configured_capacity: QueueCapacityMetrics,
    retained_automated_keys: AtomicUsize,
    in_flight_publish_attempts: AtomicUsize,
    confirmation_latency_ms: Mutex<Histogram>,
    publish_latency_ms: Mutex<Histogram>,
    overload_duration_ms: Mutex<Histogram>,
}

#[derive(Debug)]
struct QueueCapacityMetrics {
    automated: AtomicUsize,
    priority: AtomicUsize,
    effect: AtomicUsize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub overload_entries: u64,
    pub overload_exits: u64,
    pub automated_queue_full: u64,
    pub user_queue_full_rejections: u64,
    pub publish_attempts: u64,
    pub publish_retries: u64,
    pub publish_failures: u64,
    pub publish_superseded: u64,
    pub automated_queue_capacity: usize,
    pub priority_queue_capacity: usize,
    pub effect_queue_capacity: usize,
    pub retained_automated_keys: usize,
    pub in_flight_publish_attempts: usize,
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
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                overload_entries: AtomicU64::new(0),
                overload_exits: AtomicU64::new(0),
                automated_queue_full: AtomicU64::new(0),
                user_queue_full_rejections: AtomicU64::new(0),
                publish_attempts: AtomicU64::new(0),
                publish_retries: AtomicU64::new(0),
                publish_failures: AtomicU64::new(0),
                publish_superseded: AtomicU64::new(0),
                queue_configured_capacity: QueueCapacityMetrics::default(),
                retained_automated_keys: AtomicUsize::new(0),
                in_flight_publish_attempts: AtomicUsize::new(0),
                confirmation_latency_ms: Mutex::new(Histogram::new(
                    CONFIRMATION_LATENCY_BUCKETS_MS,
                )),
                publish_latency_ms: Mutex::new(Histogram::new(PUBLISH_LATENCY_BUCKETS_MS)),
                overload_duration_ms: Mutex::new(Histogram::new(OVERLOAD_DURATION_BUCKETS_MS)),
            }),
        }
    }

    pub fn set_queue_capacity(&self, queue: QueueKind, capacity: usize) {
        match queue {
            QueueKind::Automated => self
                .inner
                .queue_configured_capacity
                .automated
                .store(capacity, Ordering::Relaxed),
            QueueKind::Priority => self
                .inner
                .queue_configured_capacity
                .priority
                .store(capacity, Ordering::Relaxed),
            QueueKind::Effect => self
                .inner
                .queue_configured_capacity
                .effect
                .store(capacity, Ordering::Relaxed),
        }
    }

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

    pub fn record_publish_attempt_started(&self) {
        self.inner.publish_attempts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .in_flight_publish_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_publish_retry(&self) {
        self.inner.publish_retries.fetch_add(1, Ordering::Relaxed);
    }

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
            PublishOutcomeKind::Superseded => {
                self.inner
                    .publish_superseded
                    .fetch_add(1, Ordering::Relaxed);
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
            publish_superseded: self.inner.publish_superseded.load(Ordering::Relaxed),
            automated_queue_capacity: self
                .inner
                .queue_configured_capacity
                .automated
                .load(Ordering::Relaxed),
            priority_queue_capacity: self
                .inner
                .queue_configured_capacity
                .priority
                .load(Ordering::Relaxed),
            effect_queue_capacity: self
                .inner
                .queue_configured_capacity
                .effect
                .load(Ordering::Relaxed),
            retained_automated_keys: self.inner.retained_automated_keys.load(Ordering::Relaxed),
            in_flight_publish_attempts: self
                .inner
                .in_flight_publish_attempts
                .load(Ordering::Relaxed),
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

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for QueueCapacityMetrics {
    fn default() -> Self {
        Self {
            automated: AtomicUsize::new(0),
            priority: AtomicUsize::new(0),
            effect: AtomicUsize::new(0),
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
