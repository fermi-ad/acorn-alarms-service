mod devdb_client;
use devdb_client::DevDBClient;

mod dpm;
use dpm::{AlarmRequest, DpmData};

mod proto;
use proto::common::device::value::Value;

mod epics;

mod report;
use report::AlarmsReporter;

use rust_pubsub_lib::{Publisher, kafka_impl::KafkaPublisher};
use tokio_stream::StreamExt;

use rust_env_var_lib::env_var;

use tracing::{Level, error, info};

const DEV_DB_ADDR: &str = "DEV_DB_ADDR";
const DEFAULT_DEV_DB_ADDR: &str = "http://10.200.24.105:6802";

fn handle_daq_data<P: Publisher>(data: DpmData, alarms_reporter: &mut AlarmsReporter<P>) {
    match data {
        DpmData::DpmReading(reading) => {
            info!(
                "Reading!\nindex: {:?}\ntimestamp: {:?}\nreading: {:?}",
                reading.index, reading.timestamp, reading.data
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
                _ => false,
            };
            if should_report {
                alarms_reporter.report(reading.index, reading.timestamp, active_alarm);
            }
        }
        DpmData::DpmStatus(status) => {
            error!(
                "Status!\nIndex: {:?}\nFacility Code: {:?}\nStatus Code: {:?}\nMessage: {:?}",
                status.index, status.facility_code, status.status_code, status.message
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

    let endpoint = env_var::get(DEV_DB_ADDR).or(String::from(DEFAULT_DEV_DB_ADDR));
    let mut client = DevDBClient::connect(&endpoint).await?;

    let names = vec![];

    match client.get_device_info(names.clone()).await {
        Ok(summary) => {
            info!("DEVICE INFO = {:#?}", summary);
        }
        Err(e) => error!("DevDB error: {e:?}"),
    }

    match client.get_all_alarm_info(names).await {
        Ok(alarms) => info!("ALARM INFO = {:#?}", alarms),
        Err(e) => error!("DevDB alarm error: {e:?}"),
    }

    // --- DPM (DAQ) test ---
    // TODO: Move this to env variable
    let dpm_endpoint = "http://[::1]:50051/";
    let drf_list = vec![
        AlarmRequest {
            device: "G:AMANDA".to_string(),
            alarm_type: dpm::AlarmType::AnalogAlarm,
        },
        AlarmRequest {
            device: "G:AMANDA".to_string(),
            alarm_type: dpm::AlarmType::DigitalAlarm,
        },
    ];
    let mut alarms_reporter = AlarmsReporter::<KafkaPublisher>::new();
    match dpm::fetch_alarms(dpm_endpoint, drf_list).await {
        Ok(mut stream) => {
            while let Some(data) = stream.next().await {
                match data {
                    Ok(reading) => {
                        handle_daq_data(dpm::parse_reply(&reading)?, &mut alarms_reporter);
                    }
                    Err(e) => error!("DPM stream error: {:?}", e),
                }
            }
        }
        Err(e) => error!("DPM ERROR: {:?}", e),
    };

    Ok(())
}
