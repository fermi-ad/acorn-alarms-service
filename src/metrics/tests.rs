//! Tests for the `Metrics` counters, gauges, and histograms.

use std::time::Duration;

use super::*;
use crate::test_utils::DEFAULT_TEST_QUEUE_CONFIG;

#[test]
fn snapshot_reflects_counter_gauge_and_histogram_updates() {
    let metrics = Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG);

    metrics.record_automated_queue_full();
    metrics.record_user_queue_full_rejection();
    metrics.record_overload_entry(7);
    metrics.record_retained_automated_keys(3);
    metrics.record_overload_exit(Duration::from_millis(120));

    metrics.record_publish_attempt_started();
    metrics.record_publish_retry();
    metrics.record_publish_completion(PublishOutcomeKind::Failure, Duration::from_millis(40));

    metrics.record_publish_attempt_started();
    metrics.record_publish_completion(PublishOutcomeKind::Success, Duration::from_millis(15));

    metrics.record_confirmation_latency(Duration::from_millis(80));

    let snapshot = metrics.snapshot();

    // ── queue capacities ────────────────────────────────────────────────────
    assert_eq!(snapshot.automated_queue_capacity, 10);
    assert_eq!(snapshot.user_queue_capacity, 10);
    assert_eq!(snapshot.job_queue_capacity, 10);
    assert_eq!(snapshot.publish_queue_capacity, 10);
    assert_eq!(snapshot.snooze_queue_capacity, 10);

    // ── overload / ingress ──────────────────────────────────────────────────
    assert_eq!(snapshot.automated_queue_full, 1);
    assert_eq!(snapshot.user_queue_full_rejections, 1);
    assert_eq!(snapshot.overload_entries, 1);
    assert_eq!(snapshot.overload_exits, 1);
    assert_eq!(snapshot.retained_automated_keys, 0);

    // ── publish engine ──────────────────────────────────────────────────────
    assert_eq!(snapshot.publish_attempts, 2);
    assert_eq!(snapshot.publish_retries, 1);
    assert_eq!(snapshot.publish_failures, 1);
    assert_eq!(snapshot.in_flight_publish_attempts, 0);

    // ── histograms ──────────────────────────────────────────────────────────
    assert_eq!(snapshot.confirmation_latency_ms.samples, 1);
    assert_eq!(snapshot.confirmation_latency_ms.total_ms, 80);
    assert_eq!(snapshot.publish_latency_ms.samples, 2);
    assert_eq!(snapshot.publish_latency_ms.total_ms, 55);
    assert_eq!(snapshot.overload_duration_ms.samples, 1);
    assert_eq!(snapshot.overload_duration_ms.total_ms, 120);
}

#[test]
fn histogram_overflow_is_tracked() {
    let metrics = Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG);

    metrics.record_confirmation_latency(Duration::from_secs(5));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.confirmation_latency_ms.samples, 1);
    assert_eq!(snapshot.confirmation_latency_ms.overflow, 1);
}

#[test]
fn workflow_counters_are_tracked() {
    let metrics = Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG);

    metrics.record_job_dispatched();
    metrics.record_job_dispatched();
    metrics.record_job_committed();
    metrics.record_job_failed();
    metrics.record_snooze_wake();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.jobs_dispatched, 2);
    assert_eq!(snapshot.jobs_committed, 1);
    assert_eq!(snapshot.jobs_failed, 1);
    assert_eq!(snapshot.snooze_wakes, 1);
}

#[test]
fn snooze_counters_are_tracked() {
    let metrics = Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG);

    // Set two timers
    metrics.record_snooze_set();
    metrics.record_snooze_set();
    // Cancel one live timer
    metrics.record_snooze_cancel();
    // Invalid wake
    metrics.record_snooze_invalid_wake();
    // One expiration
    metrics.record_snooze_expiration();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.snooze_sets, 2);
    assert_eq!(snapshot.snooze_cancels, 1);
    assert_eq!(snapshot.snooze_invalid_wakes, 1);
    assert_eq!(snapshot.snooze_expirations, 1);
}

#[test]
fn in_flight_snooze_timers_increments_and_decrements() {
    let metrics = Metrics::new(&DEFAULT_TEST_QUEUE_CONFIG);

    // Two timers inserted
    metrics.record_snooze_set();
    metrics.record_snooze_set();
    assert_eq!(metrics.snapshot().in_flight_snooze_timers, 2);

    // Cancel a live timer — decrements the gauge
    metrics.record_snooze_cancel();
    assert_eq!(metrics.snapshot().in_flight_snooze_timers, 1);

    // One expiration — also decrements the gauge
    metrics.record_snooze_expiration();
    assert_eq!(metrics.snapshot().in_flight_snooze_timers, 0);
}
