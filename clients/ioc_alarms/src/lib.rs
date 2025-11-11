use anyhow::Result;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

pub mod ioc {
    tonic::include_proto!("ioc_alarms");
}
use ioc::{ioc_alarms_client::IocAlarmsClient, IocAlarmsRequest};

async fn make_channel(endpoint: &str) -> Result<Channel> {
    let mut ep = Endpoint::from_shared(endpoint.to_string())?;
    if endpoint.starts_with("https://") {
        ep = ep.tls_config(ClientTlsConfig::new())?;
    } else {
        ep = ep;
    }
    Ok(ep.connect().await?)
}

/// Call getIocAlarms for one PV.
pub async fn fetch_ioc_alarm(endpoint: &str, pv_name: &str) -> Result<ioc::IocAlarmsResponse> {
    let channel = make_channel(endpoint).await?;
    let mut client = IocAlarmsClient::new(channel);
    let req = Request::new(IocAlarmsRequest {
        pv_name: pv_name.to_string(),
    });
    let resp = client.get_ioc_alarms(req).await?.into_inner();
    Ok(resp)
}
