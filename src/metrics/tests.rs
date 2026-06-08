use std::time::Duration;

use super::*;

#[test]
fn snapshot_reflects_counter_gauge_and_histogram_updates() {
    let metrics = Metrics::new();

    metrics.set_queue_capacity(QueueKind::Automated, 4096);
    metrics.set_queue_capacity(QueueKind::Priority, 128);
    metrics.set_queue_capacity(QueueKind::Effect, 2048);

    metrics.record_automated_queue_full();
    metrics.record_user_queue_full_rejection();
    metrics.record_overload_entry(7);
    metrics.record_retained_automated_keys(3);
    metrics.record_overload_exit(Duration::from_millis(120));

    metrics.record_publish_attempt_started();
    metrics.record_publish_retry();
    metrics.record_publish_completion(PublishOutcomeKind::Failure, Duration::from_millis(40));

    metrics.record_publish_attempt_started();
    metrics.record_publish_completion(PublishOutcomeKind::Superseded, Duration::from_millis(15));

    metrics.record_confirmation_latency(Duration::from_millis(80));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.automated_queue_capacity, 4096);
    assert_eq!(snapshot.priority_queue_capacity, 128);
    assert_eq!(snapshot.effect_queue_capacity, 2048);

    assert_eq!(snapshot.automated_queue_full, 1);
    assert_eq!(snapshot.user_queue_full_rejections, 1);
    assert_eq!(snapshot.overload_entries, 1);
    assert_eq!(snapshot.overload_exits, 1);
    assert_eq!(snapshot.retained_automated_keys, 0);

    assert_eq!(snapshot.publish_attempts, 2);
    assert_eq!(snapshot.publish_retries, 1);
    assert_eq!(snapshot.publish_failures, 1);
    assert_eq!(snapshot.publish_superseded, 1);
    assert_eq!(snapshot.in_flight_publish_attempts, 0);

    assert_eq!(snapshot.confirmation_latency_ms.samples, 1);
    assert_eq!(snapshot.confirmation_latency_ms.total_ms, 80);
    assert_eq!(snapshot.publish_latency_ms.samples, 2);
    assert_eq!(snapshot.publish_latency_ms.total_ms, 55);
    assert_eq!(snapshot.overload_duration_ms.samples, 1);
    assert_eq!(snapshot.overload_duration_ms.total_ms, 120);
}

#[test]
fn histogram_overflow_is_tracked() {
    let metrics = Metrics::new();

    metrics.record_confirmation_latency(Duration::from_secs(5));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.confirmation_latency_ms.samples, 1);
    assert_eq!(snapshot.confirmation_latency_ms.overflow, 1);
}
