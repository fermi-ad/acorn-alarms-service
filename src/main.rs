mod devdb_client;
mod epics;
mod epics_device_db;

mod dpm;
use dpm::DpmData;

mod proto;
use devdb_client::DevDBClient;
use proto::common::device::value::Value;
use proto::services::ioc_alarms::ioc_alarms_client::IocAlarmsClient;

mod report;
use report::AlarmsReporter;

use rust_pubsub_lib::{Publisher, kafka_impl::KafkaPublisher};
use tokio_stream::StreamExt;

use rust_env_var_lib::env_var;

use crate::dpm::AlarmRequest;
use tracing::{Level, error, info};

const DEV_DB_ADDR: &str = "DEV_DB_ADDR";
const DEFAULT_DEV_DB_ADDR: &str = "http://10.200.24.105:6802";

const EPICS_DEV_DB_ADDR: &str = "EPICS_DEV_DB_ADDR";
const DEFAULT_EPICS_DEV_DB_ADDR: &str = "http://10.200.24.128:6802";

const DPM_ADDR: &str = "DPM_ADDR";
const DEFAULT_DPM_ADDR: &str = "http://localhost:50051";

fn handle_daq_data<P: Publisher>(data: DpmData, alarms_reporter: &mut AlarmsReporter<P>) {
    match data {
        DpmData::Reading(reading) => {
            info!(
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

    info!("Acorn Alarms Service starting…");

    //  connect and query ACNET Device DB
    let endpoint = env_var::get(DEV_DB_ADDR).or(String::from(DEFAULT_DEV_DB_ADDR));

    let mut client = DevDBClient::connect(&endpoint).await?;

    let names = vec![];

    let mut device_list: Vec<AlarmRequest> = vec![];

    match client.get_all_alarm_info(names).await {
        Ok(alarms) => {
            device_list = alarms;
            // total alarm blocks (1 AlarmRequest == 1 alarm block == 1 DRF sent later)
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
                    //new
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
        Err(e) => error!(" DevDB error: {e:?}"),
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
    let mut alarms_reporter = AlarmsReporter::<KafkaPublisher>::new();

    info!("Calling DPM with {} alarm blocks", device_list.len());

    match dpm::fetch_alarms(dpm_endpoint, device_list).await {
        Ok(mut stream) => {
            while let Some(data) = stream.next().await {
                match data {
                    Ok(dpm_data) => {
                        handle_daq_data(dpm_data, &mut alarms_reporter);
                    }
                    Err(e) => error!("DPM stream error: {:?}", e),
                }
            }
            error!("DPM stream ended (connection closed or server disconnected)");
        }
        Err(e) => error!("DPM ERROR: {:?}", e),
    };

    Ok(())
}
