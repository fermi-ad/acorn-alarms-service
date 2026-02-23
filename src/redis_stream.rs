use redis::{Client, Value, streams::StreamReadReply};
use std::{env, error::Error};

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
    println!("Connecting to Redis: {}", url);
    println!("Reading stream: {}", stream_key);

    let client: Client = redis::Client::open(url)?;

    let mut conn = client.get_multiplexed_async_connection().await?;

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

        let Some(reply) = reply else { continue };

        for stream in reply.keys {
            for entry in stream.ids {
                tracing::info!("Redis {} -> id={}", stream_key, entry.id);

                for (k, v) in entry.map.iter() {
                    println!("  {} = {}", k, value_to_string(v));
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

fn value_to_string(v: &Value) -> String {
    match v {
        Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::SimpleString(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Nil => "nil".to_string(),
        other => format!("{:?}", other),
    }
}
