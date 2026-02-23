use redis::{Client, Value, streams::StreamReadReply};
use std::{env, error::Error};
use tracing::{info, warn};

const ALARM_REDIS_HOST: &str = "EPICS_ALARM_REDIS_HOST";
const ALARM_REDIS_PORT: &str = "EPICS_ALARM_REDIS_PORT";
const ALARM_REDIS_STREAM_KEY: &str = "EPICS_ALARM_REDIS_KEY";

const DEFAULT_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_REDIS_PORT: &str = "6379";
const DEFAULT_STREAM_KEY: &str = "acorn:alarms";

pub async fn start_redis_reader() -> Result<(), Box<dyn Error>> {
    let host = get_env(ALARM_REDIS_HOST, DEFAULT_REDIS_HOST);
    let port = get_env(ALARM_REDIS_PORT, DEFAULT_REDIS_PORT);
    let stream_key = get_env(ALARM_REDIS_STREAM_KEY, DEFAULT_STREAM_KEY);

    let url = format!("redis://{}:{}/", host, port);

    // Connection log
    info!(
        target = "redis_stream",
        url = %url,
        stream = %stream_key,
        "Connecting to Redis stream reader"
    );

    let client: Client = redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // "$" : only new alarms and "0-0" : replay from beginning
    let mut last_id = "$".to_string();

    loop {
        let reply: Option<StreamReadReply> = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(1000)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg(&last_id)
            .query_async(&mut conn)
            .await?;

        let Some(reply) = reply else {
            continue;
        };

        for stream in reply.keys {
            for entry in stream.ids {
                //  Entry received log
                info!(
                    target = "redis_stream",
                    stream = %stream_key,
                    id = %entry.id,
                    fields = ?entry.map,
                    "Received alarm from Redis stream"
                );

                //  parsing logs
                if let Some(device) = map_to_string(&entry.map, "device") {
                    info!(
                        target = "redis_stream",
                        device = %device,
                        severity = %map_to_string(&entry.map, "severity").unwrap_or_default(),
                        "Parsed alarm fields"
                    );
                } else {
                    warn!(
                        target = "redis_stream",
                        id = %entry.id,
                        "Missing required device field in Redis entry"
                    );
                }

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
