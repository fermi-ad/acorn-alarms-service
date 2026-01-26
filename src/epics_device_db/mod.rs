use crate::dpm::{AlarmRequest, AlarmType};
use crate::proto::services::ioc_alarms::{
    IocAlarmsRequest, IocAlarmsResponse, ioc_alarms_client::IocAlarmsClient,
};
use anyhow::Result;
use tonic::{Request, transport::Channel};

pub async fn get_alarm_info(
    client: &mut IocAlarmsClient<Channel>,
    names: Vec<String>,
) -> Result<Vec<AlarmRequest>> {
    tracing::info!(
        "Requesting alarm info from EPICS Device DB for {} devices: {:?}",
        names.len(),
        names
    );

    let request = Request::new(IocAlarmsRequest {
        pv_name_list: names,
    });

    let response: IocAlarmsResponse = client.get_ioc_alarms(request).await?.into_inner();

    tracing::info!(
        "EPICS Device DB returned {} alarm info entries",
        response.alarm_info.len()
    );

    let alarm_requests: Vec<AlarmRequest> = response
        .alarm_info
        .iter()
        .map(|alarm_info| AlarmRequest {
            device: alarm_info.pv_name.clone(),
            alarm_type: AlarmType::Value,
        })
        .collect();

    tracing::info!(
        "EPICS Device DB built {} AlarmRequests for DPM",
        alarm_requests.len()
    );

    Ok(alarm_requests)
}
