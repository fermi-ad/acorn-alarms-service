mod proto;
use tracing::{Level, error, info};
mod report;
mod redis_stream;
use crate::redis_stream::start_redis_reader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging setup
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Unable to set global subscriber");

    info!("Acorn Alarms Service starting…");

    // Redis reader background task
    tokio::spawn(async {
        loop {
            if let Err(e) = start_redis_reader().await {
                error!("Redis reader stopped: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    });

    // DevDB disabled for Redis-only runtime

    // DevDB + EPICS + DPM sections intentionally disabled
    

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}