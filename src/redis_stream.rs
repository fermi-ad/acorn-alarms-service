use redis::{Value, streams::StreamReadReply};
use rust_env_var_lib::env_var;
use std::error::Error;
use tracing::{debug, warn};

use crate::proto::common::alarm::{
    Status,
    status::{Severity, Source, State},
};
use crate::proto::google::protobuf::Timestamp;
use crate::report::AlarmsReporter;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;

use std::sync::Arc;
use tokio::sync::Mutex;

const ALARM_REDIS_HOST: &str = "EPICS_ALARM_REDIS_HOST";
const ALARM_REDIS_PORT: &str = "EPICS_ALARM_REDIS_PORT";
const ALARM_REDIS_STREAM_KEY: &str = "EPICS_ALARM_REDIS_KEY";

const DEFAULT_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_REDIS_PORT: &str = "6379";
const DEFAULT_STREAM_KEY: &str = "acorn:alarms";

pub async fn start_redis_reader(
    reporter: Arc<Mutex<AlarmsReporter<KafkaPublisher>>>,
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
        "Connecting to Redis stream reader"
    );

    let client = redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut last_id = "0-0".to_string();

    debug!(
        target = "redis_stream",
        "Starting Redis reader from 0-0 (loading recent alarms), waiting for alarms"
    );

    loop {
        let reply: Result<Option<StreamReadReply>, redis::RedisError> = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(1000)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg(&last_id)
            .query_async(&mut conn)
            .await;

        let reply = match reply {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => {
                debug!(
                    target = "redis_stream",
                    error = ?e,
                    "Redis XREAD timeout"
                );
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        for stream in reply.keys {
            for entry in stream.ids {
                let device = map_to_string(&entry.map, "device");
                let severity = map_to_string(&entry.map, "severity");
                let state = map_to_string(&entry.map, "state");
                let source = map_to_string(&entry.map, "source");

                debug!(
                    target = "redis_stream",
                    stream = %stream_key,
                    id = %entry.id,
                    fields = ?entry.map,
                    "Received alarm from Redis stream"
                );

                if device.is_none() {
                    warn!(
                        target = "redis_stream",
                        id = %entry.id,
                        "Missing required device field in Redis entry"
                    );
                    continue;
                }

                debug!(
                    target = "redis_stream",
                    device = ?device,
                    severity = ?severity,
                    state = ?state,
                    source = ?source,
                    "Parsed alarm fields"
                );

                let device = device.unwrap().trim().to_uppercase();

                let status = build_status_from_redis(device, severity, state, source);

                let mut reporter = reporter.lock().await;
                reporter.report(status);

                last_id = entry.id.clone();
            }
        }
    }
}

fn map_to_string(map: &std::collections::HashMap<String, Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| match v {
        Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        Value::SimpleString(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        _ => None,
    })
}

fn build_status_from_redis(
    device: String,
    severity: Option<String>,
    state: Option<String>,
    source: Option<String>,
) -> Status {
    let severity_enum = match severity.unwrap_or_default().to_uppercase().as_str() {
        "LOW" | "MINOR" => Severity::Low,
        "HIGH" | "MAJOR" => Severity::High,
        _ => Severity::Unknown,
    };

    let state_enum = match state.unwrap_or_default().to_uppercase().as_str() {
        "OK" | "NORMAL" => State::Ok,
        "ALARMED" | "ALARM" => State::Alarmed,
        "BYPASSED" => State::Bypassed,
        "LATCHED" => State::Latched,
        "ACKNOWLEDGED" | "ACK" => State::Acknowledged,
        _ => State::Unknown,
    };

    let source_enum = match source.unwrap_or_default().to_uppercase().as_str() {
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
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        }),
        epics_type: String::default(),
        user: String::default(),
        wake: None,
    }
}