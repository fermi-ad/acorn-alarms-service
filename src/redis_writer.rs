use redis::Client;
use std::env;
use std::error::Error;
use tracing::{error, info};

const DEFAULT_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_REDIS_PORT: &str = "6379";
const DEFAULT_STREAM_KEY: &str = "acorn:alarms";

fn get_env(name: &str, default: &str) -> String {
    match env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

pub async fn write_alarm_to_redis(
    device: &str,
    alarm_type: &str,
    severity: &str,
    message: &str,
) -> Result<(), Box<dyn Error>> {
    let host = get_env("EPICS_ALARM_REDIS_HOST", DEFAULT_REDIS_HOST);
    let port = get_env("EPICS_ALARM_REDIS_PORT", DEFAULT_REDIS_PORT);
    let stream_key = get_env("EPICS_ALARM_REDIS_KEY", DEFAULT_STREAM_KEY);

    let url = format!("redis://{}:{}/", host, port);

    // Connection log
    info!(
        target = "redis_writer",
        url = %url,
        stream = %stream_key,
        "Connecting to Redis for alarm publish"
    );

    let client = Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Publish log 
    info!(
        target = "redis_writer",
        device = %device,
        alarm_type = %alarm_type,
        severity = %severity,
        message = %message,
        "Publishing alarm to Redis stream"
    );

    // XADD to stream
    let result: Result<String, redis::RedisError> = redis::cmd("XADD")
        .arg(&stream_key)
        .arg("*")
        .arg("device")
        .arg(device)
        .arg("type")
        .arg(alarm_type)
        .arg("severity")
        .arg(severity)
        .arg("message")
        .arg(message)
        .query_async(&mut conn)
        .await;

    match result {
        Ok(entry_id) => {
            info!(
                target = "redis_writer",
                stream = %stream_key,
                entry_id = %entry_id,
                "Alarm successfully written to Redis"
            );
        }
        Err(e) => {
            error!(
                target = "redis_writer",
                error = %e,
                device = %device,
                "Failed to write alarm to Redis"
            );
            return Err(Box::new(e));
        }
    }

    Ok(())
}
