use anyhow::Result;
use tonic::{Request};
use crate::proto::services::ioc_alarms::*;
use crate::proto::services::ioc_alarms::ioc_alarms_client::IocAlarmsClient;

pub async fn fetch_ioc_alarm(
    endpoint: &str,
    pv_name: String,
) -> Result<IocAlarmsResponse> {
    let mut client = IocAlarmsClient::connect(endpoint.to_string()).await?;

    let req = Request::new(IocAlarmsRequest { pv_name });

    let resp = client
        .get_ioc_alarms(req)
        .await?
        .into_inner();

    Ok(resp)
}

