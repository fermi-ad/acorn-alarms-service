//! Binary entrypoint for the Acorn alarms service.

use std::time::Duration;

use rust_env_var_lib::env_var;
use rust_pubsub_lib::{KafkaSnapshot, Publisher, kafka_impl::KafkaPublisher};
use tokio::{signal, spawn, time};
use tonic::transport::Server;
use tracing::{Level, debug, error, info, warn};

use adapters::{grpc::AlarmCommandsService, redis::start_redis_reader};
use proto::services::alarm_commands::alarm_commands_server::AlarmCommandsServer;
use runtime::{QueueCapacityConfig, hydration::load_startup_hydration};

mod adapters;
mod effects;
mod engine;
mod metrics;
mod model;
mod runtime;
mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

#[cfg(test)]
mod test_utils;

const CONTROLS_KAFKA_HOST: &str = "CONTROLS_KAFKA_HOST";
const DEFAULT_CONTROLS_HOST: &str = "kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092";
const CONTROLS_ALARMS_TOPIC: &str = "CONTROLS_ALARMS_TOPIC";
const DEFAULT_CONTROLS_TOPIC: &str = "alarms";

const AEOLUS_PROXY_HOST: &str = "AEOLUS_PROXY_HOST";
const DEFAULT_AEOLUS_PROXY: &str =
    "https://alarms-bridge-acnet.controls-appdev.svc.adkube.fnal.gov:6802";

const ALARMS_AUTOMATED_QUEUE_CAPACITY: &str = "ALARMS_AUTOMATED_QUEUE_CAPACITY";
const AUTOMATED_CAP_DEFAULT: usize = 4096;
const ALARMS_JOB_QUEUE_CAPACITY: &str = "ALARMS_JOB_QUEUE_CAPACITY";
const JOB_CAP_DEFAULT: usize = 4096;
const ALARMS_USER_QUEUE_CAPACITY: &str = "ALARMS_USER_QUEUE_CAPACITY";
const USER_CAP_DEFAULT: usize = 128;
const ALARMS_PUBLISH_QUEUE_CAPACITY: &str = "ALARMS_PUBLISH_QUEUE_CAPACITY";
const PUBLISH_CAP_DEFAULT: usize = 4096;
const ALARMS_SNOOZE_QUEUE_CAPACITY: &str = "ALARMS_SNOOZE_QUEUE_CAPACITY";
const SNOOZE_CAP_DEFAULT: usize = 128;

const ALARMS_METRICS_LOG_INTERVAL_SECS: &str = "ALARMS_METRICS_LOG_INTERVAL_SECS";
const METRICS_LOG_INTERVAL_DEFAULT_SECS: u64 = 30;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging setup
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

    info!("Acorn Alarms Service starting…");

    let kafka_host = get_kafka_host();
    let kafka_topic = get_kafka_topic();
    let hydrated_statuses = load_startup_hydration::<KafkaSnapshot>(
        get_aeolus_proxy_host(),
        kafka_host.clone(),
        kafka_topic.clone(),
    )
    .await?;
    let publisher = KafkaPublisher::new(kafka_host, kafka_topic);
    let queue_config = get_queue_sizes();

    let ingress = runtime::start(publisher, queue_config, hydrated_statuses).await;
    let metrics_log_interval = get_metrics_log_interval();

    let grpc_service = AlarmCommandsService {
        user_channel: ingress.user_tx.clone(),
        metrics: ingress.metrics.clone(),
    };

    spawn(log_metrics_periodically(
        ingress.metrics.clone(),
        metrics_log_interval,
    ));

    // Start gRPC server
    spawn(async move {
        info!("Starting gRPC server on port 6802");

        Server::builder()
            .add_service(AlarmCommandsServer::new(grpc_service))
            .serve("[::]:6802".parse().unwrap())
            .await
            .expect("gRPC server failed");
    });

    spawn(async move {
        loop {
            match start_redis_reader(ingress.automated_tx.clone()).await {
                Ok(()) => warn!("Redis stream ended unexpectedly — reconnecting"),
                Err(err) => error!("Redis stream error — reconnecting\n{err}"),
            }
        }
    });

    signal::ctrl_c().await?;
    Ok(())
}

async fn log_metrics_periodically(metrics: metrics::Metrics, interval: Duration) {
    let mut ticker = time::interval(interval);

    loop {
        ticker.tick().await;
        debug!(snapshot = ?metrics.snapshot(), "metrics snapshot");
    }
}

fn get_aeolus_proxy_host() -> String {
    env_var::get(AEOLUS_PROXY_HOST).or_else(|| DEFAULT_AEOLUS_PROXY.to_string())
}

fn get_kafka_host() -> String {
    env_var::get(CONTROLS_KAFKA_HOST).or_else(|| DEFAULT_CONTROLS_HOST.to_string())
}

fn get_kafka_topic() -> String {
    env_var::get(CONTROLS_ALARMS_TOPIC).or_else(|| DEFAULT_CONTROLS_TOPIC.to_string())
}

fn get_metrics_log_interval() -> Duration {
    Duration::from_secs(
        env_var::get(ALARMS_METRICS_LOG_INTERVAL_SECS).or(METRICS_LOG_INTERVAL_DEFAULT_SECS),
    )
}

fn get_queue_sizes() -> QueueCapacityConfig {
    let automated = env_var::get(ALARMS_AUTOMATED_QUEUE_CAPACITY).or(AUTOMATED_CAP_DEFAULT);
    let user = env_var::get(ALARMS_USER_QUEUE_CAPACITY).or(USER_CAP_DEFAULT);
    let job = env_var::get(ALARMS_JOB_QUEUE_CAPACITY).or(JOB_CAP_DEFAULT);
    let publish = env_var::get(ALARMS_PUBLISH_QUEUE_CAPACITY).or(PUBLISH_CAP_DEFAULT);
    let snooze = env_var::get(ALARMS_SNOOZE_QUEUE_CAPACITY).or(SNOOZE_CAP_DEFAULT);

    QueueCapacityConfig {
        automated,
        user,
        job,
        publish,
        snooze,
    }
}
