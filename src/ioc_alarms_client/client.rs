use anyhow::Result;
use tonic::Request;

use crate::proto::services::ioc_alarms::{ioc_alarms_client::IocAlarmsClient, IocAlarmsRequest};

use super::model::IocAlarm;

pub struct IOCAlarmsClient {
    inner: IocAlarmsClient<tonic::transport::Channel>,
}

impl IOCAlarmsClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let inner = IocAlarmsClient::connect(endpoint.to_string()).await?;
        Ok(Self { inner })
    }

    pub async fn fetch_alarm(&mut self, pv_name: String) -> Result<Option<IocAlarm>> {
        let req = Request::new(IocAlarmsRequest {
            pv_name_list: vec![pv_name],
        });

        let resp = self.inner.get_ioc_alarms(req).await?.into_inner();

        Ok(IocAlarm::from_proto(resp))
    }
}
