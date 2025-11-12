use anyhow::Result;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

pub mod ioc_alarms {
    tonic::include_proto!("services.ioc_alarms");
}

use ioc_alarms::ioc_alarms_client::IocAlarmsClient;
use ioc_alarms::{IocAlarmsRequest, IocAlarmsResponse};

async fn make_channel(endpoint: &str) -> Result<Channel> {
    let mut ep = Endpoint::from_shared(endpoint.to_string())?;
    if endpoint.starts_with("https://") {
        ep = ep.tls_config(ClientTlsConfig::new())?;
    }
    Ok(ep.connect().await?)
}

pub async fn fetch_ioc_alarm(endpoint: &str, pv_name: &str) -> Result<IocAlarmsResponse> {
    let channel = make_channel(endpoint).await?;
    let mut client = IocAlarmsClient::new(channel);

    let req = Request::new(IocAlarmsRequest {
        pv_name: pv_name.to_string(),
    });

    let resp = client.get_ioc_alarms(req).await?.into_inner();
    Ok(resp)
}
