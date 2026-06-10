//! Redis Stream reader for EPICS alarm events.
//!
//! Connects to a Redis Stream via `rust-pubsub-lib`'s [`RedisStreamSubscriber`]
//! and forwards each alarm entry to the engine ingress.
//!
//! The public entry-point [`start_redis_reader`] handles configuration and
//! subscriber creation, then delegates message processing to
//! [`run_alarm_stream`] so the stream loop remains easy to test in isolation.

use std::{collections::HashMap, error::Error};

use chrono::Utc;
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{MapMessage, Message, PubSubError, RedisStreamSubscriber, Subscriber};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, error, warn};

use crate::{
    engine::ingress::AutomatedIngressHandle,
    proto::{
        common::alarm::{
            Status,
            status::{Severity, Source, State},
        },
        google::protobuf::Timestamp,
    },
};

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

/// Connects to the configured Redis Stream and forwards alarm entries until the
/// stream ends.
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
/// automatically. Entries whose `device` field is missing or empty are also
/// skipped with a warning.
///
/// Queue policy for automated ingress is bounded-and-await. When the coordinator
/// is saturated, this reader awaits queue capacity instead of buffering an
/// unbounded backlog in process memory.
///
/// The actual message-processing loop lives in [`run_alarm_stream`], which can
/// be called directly in integration tests with a pre-built stream.
pub async fn start_redis_reader(
    automated_channel: AutomatedIngressHandle,
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

    run_alarm_stream(automated_channel, stream).await
}

async fn run_alarm_stream<S>(
    automated_channel: AutomatedIngressHandle,
    mut stream: S,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
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

                if let Err(send_error) = automated_channel.send_automated_update(status).await {
                    log_automated_send_error(send_error);
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

    let time_secs = redis_entries
        .remove("timestamp")
        .and_then(|time_str| match time_str.trim().parse() {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                error!("Failed to parse timestamp string to int. Error: {e}: '{time_str}'");
                None
            }
        })
        .unwrap_or_else(|| Utc::now().timestamp());

    Status {
        device,
        severity: severity_enum as i32,
        state: state_enum as i32,
        source: source_enum as i32,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: time_secs,
            nanos: 0,
        }),
        epics_type: String::default(),
        user: String::default(),
        wake: None,
    }
}

fn log_automated_send_error(send_error: mpsc::error::SendError<Status>) {
    error!(
        "The alarms state machine has stopped working while forwarding automated updates: {send_error}"
    );
}
