mod grpc_server;
mod proto;
mod redis_stream;
mod report;

use crate::grpc_server::AlarmCommandsService;
use crate::proto::services::alarm_commands::v1::alarm_commands_server::AlarmCommandsServer;
use crate::redis_stream::start_redis_reader;

use report::AlarmsReporter;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;

use std::sync::Arc;
use tokio::sync::Mutex;

use tonic::transport::Server;
use tracing::{Level, info};

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

    let reporter = Arc::new(Mutex::new(AlarmsReporter::<KafkaPublisher>::new()));

    let grpc_service = AlarmCommandsService {
        reporter: reporter.clone(),
    };

    // Start gRPC server
    tokio::spawn(async move {
        info!("Starting gRPC server on port 50051");

        Server::builder()
            .add_service(AlarmCommandsServer::new(grpc_service))
            .serve("[::]:50051".parse().unwrap())
            .await
            .expect("gRPC server failed");
    });

    tokio::spawn(start_redis_reader(reporter.clone()));

    tokio::signal::ctrl_c().await?;
    Ok(())
}
