mod proto;
mod redis_stream;
mod report;

use crate::redis_stream::start_redis_reader;
use report::AlarmsReporter;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;
use std::sync::{Arc, Mutex};
use tracing::{Level, error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging setup
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

    info!("Acorn Alarms Service starting…");

    // Shared Kafka reporter
    let reporter = Arc::new(Mutex::new(AlarmsReporter::<KafkaPublisher>::new()));

    let reporter_clone = reporter.clone();

    // Redis reader background task
    tokio::spawn(async move {
        loop {
            if let Err(e) = start_redis_reader(reporter_clone.clone()).await {
                error!("Redis reader stopped: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    });

    // Keep service alive
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
