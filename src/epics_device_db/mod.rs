use crate::proto::services::ioc_alarms::ioc_alarms_client::IocAlarmsClient;
use anyhow::Result;

pub struct EpicsDevDBClient {
    inner: IocAlarmsClient<tonic::transport::Channel>,
}

impl EpicsDevDBClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let inner = IocAlarmsClient::connect(endpoint.to_string()).await?;
        Ok(Self { inner })
    }
}
