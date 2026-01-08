mod devdb_client;
mod epics;
mod epics_device_db;

mod dpm;
use dpm::DpmData;

mod proto;
use proto::common::device::value::Value;
use proto::services::{
    devdb::dev_db_client::DevDbClient, ioc_alarms::ioc_alarms_client::IocAlarmsClient,
};

mod report;
use report::AlarmsReporter;

use rust_pubsub_lib::{Publisher, kafka_impl::KafkaPublisher};
use tokio_stream::StreamExt;

use rust_env_var_lib::env_var;

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

    let mut client = DevDbClient::connect(endpoint.to_string()).await?;

    let names = vec![
        "G:AMANDA".to_string(),
        "M:OUTTMP".to_string(),
        "B:MH1".to_string(),
        "I:21AIRP".to_string(),
        "I:IP100F".to_string(),
        "B:BL0260".to_string(),
        "Z:ACLTST".to_string(),
    ];

    let mut device_list = vec![];
    match devdb_client::get_alarm_info(&mut client, names).await {
        Ok(alarms) => device_list = alarms,
        Err(e) => error!("ACNET Device DB error: {e:?}"),
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
        }
        Err(e) => error!("DPM ERROR: {:?}", e),
    };

    Ok(())
}
