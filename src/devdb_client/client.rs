use anyhow::Result;
use tonic::Request;

use crate::proto::services::devdb::{
    dev_db_client::DevDbClient, AlarmInfoReply, DeviceInfoReply, DeviceList,
};

use super::model::{AlarmInfoExpanded, DeviceSummary};

pub struct DevDBClient {
    inner: DevDbClient<tonic::transport::Channel>,
}

impl DevDBClient {
    /// Connect to DevDB gRPC endpoint
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let inner = DevDbClient::connect(endpoint.to_string()).await?;
        Ok(Self { inner })
    }

    // --------------------------------------------------------------------
    // FETCH BASIC DEVICE INFO
    // --------------------------------------------------------------------
    pub async fn get_device_info(&mut self, names: Vec<String>) -> Result<Vec<DeviceSummary>> {
        let req = Request::new(DeviceList { device: names });

        let reply: DeviceInfoReply = self.inner.get_device_info(req).await?.into_inner();

        let summaries = reply
            .set
            .iter()
            .filter_map(DeviceSummary::from_proto)
            .collect();

        Ok(summaries)
    }

    // --------------------------------------------------------------------
    // FETCH ALARM INFO
    // --------------------------------------------------------------------
    pub async fn get_all_alarm_info(
        &mut self,
        names: Vec<String>,
    ) -> Result<Vec<AlarmInfoExpanded>> {
        let req = Request::new(DeviceList { device: names });

        let reply: AlarmInfoReply = self.inner.get_all_alarm_info(req).await?.into_inner();

        let alarms = reply
            .alarm_info
            .iter()
            .map(AlarmInfoExpanded::from_proto)
            .collect();

        Ok(alarms)
    }
}
