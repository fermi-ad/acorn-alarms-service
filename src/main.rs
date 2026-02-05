mod devdb_client;
mod epics;
mod epics_device_db;

mod dpm;
use dpm::DpmData;

mod proto;
use proto::common::device::value::Value;
use proto::services::devdb::dev_db_client::DevDbClient;
use proto::services::ioc_alarms::ioc_alarms_client::IocAlarmsClient;

mod report;
use report::AlarmsReporter;

use rust_pubsub_lib::{Publisher, kafka_impl::KafkaPublisher};
use tokio::signal;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use rust_env_var_lib::env_var;

use crate::dpm::AlarmRequest;
use tracing::{Level, debug, error, info};

const DEV_DB_ADDR: &str = "DEV_DB_ADDR";
const DEFAULT_DEV_DB_ADDR: &str = "http://localhost:6802";
const DPM_ADDR: &str = "DPM_ADDR";
const DEFAULT_DPM_ADDR: &str = "http://localhost:50051";

const EPICS_DEV_DB_ADDR: &str = "EPICS_DEV_DB_ADDR";
const DEFAULT_EPICS_DEV_DB_ADDR: &str = "http://10.200.24.128:6802";

const DPM_CHUNK_SIZE: &str = "DPM_CHUNK_SIZE";
const DEFAULT_DPM_CHUNK_SIZE: usize = 100;

fn handle_daq_data<P: Publisher>(data: DpmData, alarms_reporter: &mut AlarmsReporter<P>) {
    match data {
        DpmData::Reading(reading) => {
            debug!(
                "Reading!\ndevice: {:?}\nalarm type: {:?}\ntimestamp: {:?}\nreading: {:?}",
                reading.device, reading.alarm_type, reading.timestamp, reading.data
            );
            let mut should_report = false;
            let active_alarm = match reading.data {
                Value::AnaAlarm(alrm) => {
                    should_report = true;
                    alrm.alarm_enable && alrm.alarm_status
                }
                Value::DigAlarm(alrm) => {
                    should_report = true;
                    alrm.alarm_enable && alrm.alarm_status
                }
                Value::Text(sevr) => {
                    should_report = true;
                    sevr != "NO_ALARM"
                }
                _ => false,
            };
            if should_report {
                alarms_reporter.report(reading.index, reading.timestamp, active_alarm);
            }
        }
        DpmData::Status(status) => {
            error!(
                "Status!\nDevice: {:?}\nAlarm Type: {:?}\nFacility Code: {:?}\nStatus Code: {:?}\nMessage: {:?}",
                status.device,
                status.alarm_type,
                status.facility_code,
                status.status_code,
                status.message
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

    let cancellation_token = CancellationToken::new();
    let main_cancel_token = cancellation_token.clone();

    // Set up Ctrl+C handler for graceful shutdown
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdown signal received (Ctrl+C)");
                main_cancel_token.cancel();
            }
            Err(err) => {
                error!("Error listening for Ctrl+C: {}", err);
            }
        }
    });

    info!("Acorn Alarms Service starting…");

    let endpoint = env_var::get(DEV_DB_ADDR).or(String::from(DEFAULT_DEV_DB_ADDR));

    let mut client = DevDbClient::connect(endpoint.to_string()).await?;

    let names = vec!["G:AMANDA".to_string()];

    let mut device_list: Vec<AlarmRequest> = vec![];

    match devdb_client::get_alarm_info(&mut client, names).await {
        Ok(alarms) => {
            device_list = alarms;

            // total alarm blocks (1 AlarmRequest == 1 alarm block)
            let total_blocks = device_list.len();

            // unique device names
            use std::collections::HashSet;
            let unique_devices: HashSet<String> =
                device_list.iter().map(|a| a.device.clone()).collect();

            // counts by alarm type
            let mut analog_count = 0usize;
            let mut digital_count = 0usize;

            for a in &device_list {
                match a.alarm_type {
                    crate::dpm::AlarmType::Analog => analog_count += 1,
                    crate::dpm::AlarmType::Digital => digital_count += 1,
                    crate::dpm::AlarmType::Value => {}
                }
            }

            info!(
                "DevDB -> total alarm blocks: {}, unique devices: {}, analog blocks: {}, digital blocks: {}",
                total_blocks,
                unique_devices.len(),
                analog_count,
                digital_count
            );
        }
        Err(e) => error!("DevDB error: {:?}", e),
    }

    //  connect and query EPICS Device DB
    let epics_endpoint =
        env_var::get(EPICS_DEV_DB_ADDR).or(String::from(DEFAULT_EPICS_DEV_DB_ADDR));

    let mut epics_client = IocAlarmsClient::connect(epics_endpoint.to_string()).await?;

    let epics_names = vec!["PIP2IT:pHB650_CRYO_TX103:TempK".to_string()];

    let mut epics_device_list = vec![];
    match epics_device_db::get_alarm_info(&mut epics_client, epics_names).await {
        Ok(alarms) => epics_device_list = alarms,
        Err(e) => error!("EPICS Device DB error: {e:?}"),
    }

    device_list.append(&mut epics_device_list);

    // --- DPM (DAQ) test ---
    let dpm_endpoint = env_var::get(DPM_ADDR).or(String::from(DEFAULT_DPM_ADDR));
    let alarms_reporter = std::sync::Arc::new(tokio::sync::Mutex::new(AlarmsReporter::<
        KafkaPublisher,
    >::new()));

    info!("Calling DPM with {} alarm blocks", device_list.len());

    // chunk up device list
    let chunk_size: usize = env_var::get(DPM_CHUNK_SIZE)
        .or(DEFAULT_DPM_CHUNK_SIZE.to_string())
        .parse()
        .unwrap_or(DEFAULT_DPM_CHUNK_SIZE);
    info!("Using DPM chunk size = {}", chunk_size);
    info!(
        "Total batches: {}, batch size: {}",
        device_list.len() / chunk_size + 1,
        chunk_size
    );

    let mut task_set = tokio::task::JoinSet::new();

    for chunk in device_list.chunks(chunk_size) {
        let batch = chunk.to_vec();
        let endpoint = dpm_endpoint.clone();
        let cancel_token = cancellation_token.clone();
        let alarms_reporter = alarms_reporter.clone();

        // spawn tasks for each chunk
        task_set.spawn(async move {
            loop {
                if cancel_token.is_cancelled() {
                    break;
                }

                info!("Attempting connection for batch of {}", batch.len());
                let result =
                    dpm::fetch_alarms(endpoint.clone(), batch.clone(), cancel_token.clone()).await;

                match result {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            match item {
                                Ok(data) => {
                                    let mut reporter = alarms_reporter.lock().await;
                                    handle_daq_data(data, &mut reporter);
                                }
                                Err(e) => {
                                    error!("Stream decoding error: {}. Retrying...", e);
                                    break; // Break stream loop to reconnect
                                }
                            }
                        }
                    }
                    Err(e) => error!("Failed to connect to DPM: {:?}", e),
                }
            }
        });
    }

    info!("All streams active. Press Ctrl+C to stop.");
    cancellation_token.cancelled().await;

    info!("Shutting down tasks...");
    task_set.shutdown().await;

    info!("Acorn Alarms Service stopped");
    Ok(())
}
