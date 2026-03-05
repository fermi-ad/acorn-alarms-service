mod proto;
mod redis_stream;
mod report;

use crate::redis_stream::start_redis_reader;
use report::AlarmsReporter;
use rust_pubsub_lib::kafka_impl::KafkaPublisher;
use tracing::{Level, info};

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
    let mut reporter = AlarmsReporter::<KafkaPublisher>::new();

    loop {
        if let Err(err) = start_redis_reader(&mut reporter).await {
            tracing::error!("Redis reader error: {:?}", err);
        }
    }
}
