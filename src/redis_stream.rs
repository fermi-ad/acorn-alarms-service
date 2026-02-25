use redis::{Value, streams::StreamReadReply};
use std::{env, error::Error};
use tracing::{info, warn};

use crate::report::AlarmsReporter;
use crate::proto::common::alarm::{
    Status,
    status::{Severity, Source, State},
};
use crate::proto::google::protobuf::Timestamp;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;

const ALARM_REDIS_HOST: &str = "EPICS_ALARM_REDIS_HOST";
const ALARM_REDIS_PORT: &str = "EPICS_ALARM_REDIS_PORT";
const ALARM_REDIS_STREAM_KEY: &str = "EPICS_ALARM_REDIS_KEY";

const DEFAULT_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_REDIS_PORT: &str = "6379";
const DEFAULT_STREAM_KEY: &str = "acorn:alarms";

pub async fn start_redis_reader() -> Result<(), Box<dyn Error + Send + Sync>> {
    let host = get_env(ALARM_REDIS_HOST, DEFAULT_REDIS_HOST);
    let port = get_env(ALARM_REDIS_PORT, DEFAULT_REDIS_PORT);
    let stream_key = get_env(ALARM_REDIS_STREAM_KEY, DEFAULT_STREAM_KEY);

    let url = format!("redis://{}:{}/", host, port);

    info!(
        target = "redis_stream",
        url = %url,
        stream = %stream_key,
        "Connecting to Redis stream reader"
    );

    let client = redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut reporter = AlarmsReporter::<KafkaPublisher>::new();

    let mut last_id = "0-0".to_string();

    info!(
        target = "redis_stream",
        "Starting Redis reader from 0-0 (load recent alarm state on startup)"
    );

    info!(
        target = "redis_stream",
        "Redis reader ready, waiting for alarms"
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
            Ok(r) => r,
            Err(e) => {
                warn!(
                    target = "redis_stream",
                    error = ?e,
                    "Redis XREAD failed"
                );
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        let Some(reply) = reply else {
            continue;
        };

        for stream in reply.keys {
           for entry in stream.ids {
    let device = map_to_string(&entry.map, "device");
    let severity = map_to_string(&entry.map, "severity");
    let state = map_to_string(&entry.map, "state");
    let source = map_to_string(&entry.map, "source");

    info!(
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

    info!(
        target = "redis_stream",
        device = ?device,
        severity = ?severity,
        state = ?state,
        source = ?source,
        "Parsed alarm fields"
    );

    let status = build_status_from_redis(
        device.clone().unwrap(),
        severity.clone(),
        state.clone(),
        source.clone(),
    );

    reporter.report(status);

    last_id = entry.id.clone();
}
        }
    }
}

fn get_env(name: &str, default: &str) -> String {
    match env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

fn map_to_string(map: &std::collections::HashMap<String, Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| match v {
        Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        Value::SimpleString(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        Value::Nil => None,
        _ => None,
    })
}

fn build_status_from_redis(
    device: String,
    severity: Option<String>,
    state: Option<String>,
    source: Option<String>,
) -> Status {
    // ----- Severity mapping -----
    let severity_enum = match severity
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "LOW" | "MINOR" => Severity::Low,
        "HIGH" | "MAJOR" => Severity::High,
        _ => Severity::Unknown,
    };

    // ----- State mapping -----
    let state_enum = match state
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "OK" => State::Ok,
        "ALARMED" | "ALARM" => State::Alarmed,
        "BYPASSED" => State::Bypassed,
        "LATCHED" => State::Latched,
        "ACKNOWLEDGED" | "ACK" => State::Acknowledged,
        _ => State::Unknown,
    };

    // ----- Source mapping -----
    let source_enum = match source
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
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        }),
        epics_type: String::default(),
        user: String::default(),
        wake: None,
    }
}