//! Redis Stream reader for EPICS alarm events.
//!
//! Connects to a Redis Stream via `rust-pubsub-lib`'s [`RedisStreamSubscriber`]
//! and forwards each alarm entry to [`AlarmsReporter`].
//!
//! # Architecture
//!
//! The public entry-point [`start_redis_reader`] is a thin wrapper that handles
//! configuration (env-var lookup, URL construction) and subscriber creation, then
//! delegates all message-processing work to the private [`run_alarm_stream`]
//! function.  Keeping the loop in a separate function makes it independently
//! testable: integration tests can inject a pre-built stream (e.g. one backed by
//! [`RedisTestHarness`](rust_pubsub_lib::RedisTestHarness)) without going through
//! the env-var / URL plumbing.

use std::sync::Arc;
use std::{collections::HashMap, error::Error};

use chrono::Utc;
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{
    MapMessage, Message, PubSubError, Publisher, RedisStreamSubscriber, Subscriber,
};
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};

use crate::proto::common::alarm::{
    Status,
    status::{Severity, Source, State},
};
use crate::proto::google::protobuf::Timestamp;
use crate::report::AlarmsReporter;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod tests;

const ALARM_REDIS_HOST: &str = "EPICS_ALARM_REDIS_HOST";
const ALARM_REDIS_PORT: &str = "EPICS_ALARM_REDIS_PORT";
const ALARM_REDIS_STREAM_KEY: &str = "EPICS_ALARM_REDIS_KEY";

const DEFAULT_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_REDIS_PORT: &str = "6379";
const DEFAULT_STREAM_KEY: &str = "acorn:alarms";

/// Connects to the configured Redis Stream and forwards alarm entries to
/// [`AlarmsReporter`] until the stream ends.
///
/// Connection parameters are read from environment variables, falling back to
/// built-in defaults when the variables are absent:
///
/// | Variable                    | Default         |
/// |-----------------------------|-----------------|
/// | `EPICS_ALARM_REDIS_HOST`    | `127.0.0.1`     |
/// | `EPICS_ALARM_REDIS_PORT`    | `6379`          |
/// | `EPICS_ALARM_REDIS_KEY`     | `acorn:alarms`  |
///
/// Stream errors are logged and skipped; `rust-pubsub-lib` handles reconnection
/// automatically.  Entries whose `device` field is missing or empty are also
/// skipped with a warning.
///
/// The actual message-processing loop lives in [`run_alarm_stream`], which can
/// be called directly in integration tests with a pre-built stream.
pub async fn start_redis_reader<P: Publisher>(
    reporter: Arc<Mutex<AlarmsReporter<P>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let host = env_var::get(ALARM_REDIS_HOST).or_else(|| DEFAULT_REDIS_HOST.to_string());
    let port = env_var::get(ALARM_REDIS_PORT).or_else(|| DEFAULT_REDIS_PORT.to_string());
    let stream_key =
        env_var::get(ALARM_REDIS_STREAM_KEY).or_else(|| DEFAULT_STREAM_KEY.to_string());

    let url = format!("redis://{}:{}/", host, port);

    debug!(
        target = "redis_stream",
        url = %url,
        stream = %stream_key,
        "Connecting to Redis stream reader via pubsub-lib"
    );

    let mut subscriber = RedisStreamSubscriber::new(url, stream_key);
    let stream = subscriber.get_stream::<MapMessage>().await?;

    debug!(
        target = "redis_stream",
        "Redis stream subscriber started, waiting for alarms"
    );

    run_alarm_stream(reporter, stream).await
}

/// Drives the alarm-processing loop over an already-constructed stream.
///
/// Each item yielded by `stream` is processed as follows:
///
/// - **`Err`** — the error is logged at `debug` level and the loop continues;
///   `rust-pubsub-lib` handles reconnection automatically.
/// - **`Ok`** — the [`MapMessage`] payload is parsed by
///   [`build_status_from_redis`] into a [`Status`].  If the resulting
///   `device` field is empty the entry is skipped with a `warn`-level log.
///   Otherwise the status is forwarded to [`AlarmsReporter::report`].
///
/// The function returns `Ok(())` when the stream is exhausted (i.e. the
/// underlying channel is closed).
async fn run_alarm_stream<P, S>(
    reporter: Arc<Mutex<AlarmsReporter<P>>>,
    mut stream: S,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    P: Publisher,
    S: Stream<Item = Result<MapMessage, PubSubError>> + Unpin,
{
    while let Some(item) = stream.next().await {
        match item {
            Err(e) => {
                debug!(
                    target = "redis_stream",
                    error = ?e,
                    "Redis stream error — pubsub-lib will reconnect automatically"
                );
                continue;
            }
            Ok(msg) => {
                let payload = msg.extract_value();
                let status = build_status_from_redis(payload);
                if status.device.is_empty() {
                    warn!(
                        target = "redis_stream",
                        "Missing required device field in alarm entry"
                    );
                    continue;
                }

                debug!(
                    target = "redis_stream",
                    device = %status.device,
                    severity = ?status.severity,
                    source = ?status.source,
                    "Parsed alarm fields"
                );

                {
                    let mut reporter = reporter.lock().await;
                    reporter.report(status).await;
                }
            }
        }
    }

    Ok(())
}

fn build_status_from_redis(mut redis_entries: HashMap<String, String>) -> Status {
    let device = redis_entries
        .remove("device")
        .unwrap_or_default()
        .to_uppercase();

    let severity_str = redis_entries
        .remove("severity")
        .unwrap_or_default()
        .to_uppercase();

    let (severity_enum, state_enum) = match severity_str.as_str() {
        "NO_ALARM" => (Severity::Unknown, State::Ok),
        "LOW" | "MINOR" => (Severity::Low, State::Alarmed),
        "HIGH" | "MAJOR" => (Severity::High, State::Alarmed),
        _ => (Severity::Unknown, State::Unknown),
    };

    let source_enum = match redis_entries
        .remove("source")
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "ANALOG" => Source::Analog,
        "DIGITAL" => Source::Digital,
        "EPICS" => Source::Epics,
        _ => Source::Unknown,
    };

    Status {
        device,
        severity: severity_enum as i32,
        state: state_enum as i32,
        source: source_enum as i32,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: Utc::now().timestamp(),
            nanos: 0,
        }),
        epics_type: String::default(),
        user: String::default(),
        wake: None,
    }
}
