use crate::proto::services::devdb::{
    AlarmInfo, AlarmInfoReply, DeviceInfoReply, DeviceList, dev_db_client::DevDbClient,
};
use anyhow::Result;
use tonic::Request;

pub struct DevDBClient {
    inner: DevDbClient<tonic::transport::Channel>,
}

impl DevDBClient {
    /// Connect to DevDB gRPC endpoint
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let inner = DevDbClient::connect(endpoint.to_string()).await?;
        Ok(Self { inner })
    }

    /// Fetch basic device info
    pub async fn get_device_info(&mut self, names: Vec<String>) -> Result<DeviceInfoReply> {
        let req = Request::new(DeviceList { device: names });

        let reply = self.inner.get_device_info(req).await?.into_inner();

        Ok(reply)
    }

    /// Fetch alarm info
    pub async fn get_all_alarm_info(&mut self, names: Vec<String>) -> Result<Vec<AlarmInfo>> {
        let req = Request::new(DeviceList { device: names });

        let reply: AlarmInfoReply = self.inner.get_all_alarm_info(req).await?.into_inner();

        Ok(reply.alarm_info)
    }
}
