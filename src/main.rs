use std::sync::Arc;

use grpc_server::AlarmCommandsService;
use proto::services::alarm_commands::alarm_commands_server::AlarmCommandsServer;
use report::AlarmsReporter;

use rust_env_var_lib::env_var;
use rust_pubsub_lib::{Publisher, kafka_impl::KafkaPublisher};
use tokio::signal;
use tokio::spawn;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tracing::{Level, error, info, warn};

mod grpc_server;
mod proto;
mod redis_stream;
mod report;

#[cfg(test)]
mod test_utils;

const CONTROLS_KAFKA_HOST: &str = "CONTROLS_KAFKA_HOST";
const DEFAULT_CONTROLS_HOST: &str = "kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092";

const CONTROLS_ALARMS_TOPIC: &str = "CONTROLS_ALARMS_TOPIC";
const DEFAULT_CONTROLS_TOPIC: &str = "alarms";

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

    let reporter = Arc::new(Mutex::new(AlarmsReporter::new(get_kafka_publisher())));

    let grpc_service = AlarmCommandsService {
        reporter: reporter.clone(),
    };

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
            match redis_stream::start_redis_reader(reporter.clone()).await {
                Ok(()) => warn!("Redis stream ended unexpectedly — reconnecting"),
                Err(err) => error!("Redis stream error — reconnecting\n{err}"),
            }
        }
    });

    signal::ctrl_c().await?;
    Ok(())
}

fn get_kafka_publisher() -> KafkaPublisher {
    let host = env_var::get(CONTROLS_KAFKA_HOST).or_else(|| DEFAULT_CONTROLS_HOST.to_string());

    let topic = env_var::get(CONTROLS_ALARMS_TOPIC).or_else(|| DEFAULT_CONTROLS_TOPIC.to_string());

    KafkaPublisher::new(host, topic)
}
