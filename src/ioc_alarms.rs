use anyhow::Result;
use tonic::transport::Channel;
use tonic::Request;

pub mod services {
    pub mod ioc_alarms {
        tonic::include_proto!("services.ioc_alarms");
    }
}

use services::ioc_alarms::ioc_alarms_client::IocAlarmsClient;
use services::ioc_alarms::{IocAlarmsRequest, IocAlarmsResponse};

pub async fn fetch_ioc_alarm(endpoint: &str, pv_name: String) -> Result<IocAlarmsResponse> {
    let channel = Channel::from_shared(endpoint.to_string())?
        .connect()
        .await?;

    let mut client = IocAlarmsClient::new(channel);

    let req = Request::new(IocAlarmsRequest { pv_name });

    let resp = client.get_ioc_alarms(req).await?.into_inner();

    Ok(resp)
}
