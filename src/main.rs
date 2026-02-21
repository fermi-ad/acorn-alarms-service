mod devdb_client;
mod dpm;
use dpm::{DpmData, DpmReading};
mod epics_device_db;
mod proto;
use proto::{
    common::{
        alarm::{
            Status,
            status::{Severity, Source, State},
        },
        device::value::Value,
    },
    google::protobuf::Timestamp,
    services::{devdb::dev_db_client::DevDbClient, ioc_alarms::ioc_alarms_client::IocAlarmsClient},
};
mod report;
use report::AlarmsReporter;
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{Publisher, kafka_impl::KafkaPublisher};
use tokio_stream::StreamExt;
use tracing::{Level, error, info};
mod redis_stream;
use crate::redis_stream::start_redis_reader;

const DEV_DB_ADDR: &str = "DEV_DB_ADDR";
const DEFAULT_DEV_DB_ADDR: &str = "https://grpc-devdb.controls-appdev.svc.adkube.fnal.gov:6802";

const EPICS_DEV_DB_ADDR: &str = "EPICS_DEV_DB_ADDR";
const DEFAULT_EPICS_DEV_DB_ADDR: &str =
    "https://grpc-ioc-alarms.controls-appdev.svc.adkube.fnal.gov:6802";

const DPM_ADDR: &str = "DPM_ADDR";
const DEFAULT_DPM_ADDR: &str = "http://131.225.120.107:50051";


fn create_alarm_from_data(reading: DpmReading) -> Option<Status> {
    let (source, state) = match reading.data {
        Value::AnaAlarm(alrm) => {
            let state = if !alrm.alarm_enable {
                State::Bypassed
            } else if alrm.alarm_status {
                State::Alarmed
            } else {
                State::Ok
            };
            (Source::Analog, state)
        }
        Value::DigAlarm(alrm) => {
            let state = if !alrm.alarm_enable {
                State::Bypassed
            } else if alrm.alarm_status {
                State::Alarmed
            } else {
                State::Ok
            };
            (Source::Digital, state)
        }
        Value::Text(sevr) => {
            let state = if sevr == "NO_ALARM" {
                State::Ok
            } else {
                State::Alarmed
            };
            (Source::Epics, state)
        }
        _ => return None,
    };

    Some(Status {
        device: reading.device,
        source: source as i32,
        state: state as i32,
        severity: Severity::Unknown as i32,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: reading.timestamp.timestamp(),
            nanos: reading.timestamp.timestamp_subsec_nanos() as i32,
        }),
        epics_type: String::default(),
        user: String::default(),
        wake: None,
    })
}

fn handle_daq_data<P: Publisher>(data: DpmData, alarms_reporter: &mut AlarmsReporter<P>) {
    match data {
        DpmData::Reading(reading) => {
            info!(
                "Reading!\ndevice: {:?}\nalarm type: {:?}\ntimestamp: {:?}\nreading: {:?}",
                reading.device, reading.alarm_type, reading.timestamp, reading.data
            );
            if let Some(alarm) = create_alarm_from_data(reading) {
                alarms_reporter.report(alarm);
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
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set global subscriber");

    info!("Acorn Alarms Service starting…");

    tokio::spawn(async {
        if let Err(e) = start_redis_reader().await {
            error!("Redis reader error: {:?}", e);
        }
    });
    start_redis_reader().await?;
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

    // DAQ
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
