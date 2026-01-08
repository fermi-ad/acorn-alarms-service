use crate::{
    dpm::AlarmRequest,
    proto::services::devdb::{AlarmInfo, AlarmInfoReply, DeviceList, dev_db_client::DevDbClient},
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

    /// Fetch alarm info
    pub async fn get_all_alarm_info(&mut self, names: Vec<String>) -> Result<Vec<AlarmRequest>> {
        let req = Request::new(DeviceList { device: names });

        let reply: AlarmInfoReply = self.inner.get_all_alarm_info(req).await?.into_inner();

        let alarm_requests: Vec<AlarmRequest> = reply
            .alarm_info
            .iter()
            .map(|alarm_info| build_alarm_request(&alarm_info))
            .collect();

        Ok(alarm_requests)
    }
}
fn build_alarm_request(alarm_info: &AlarmInfo) -> AlarmRequest {
    AlarmRequest {
        device: alarm_info.device_name.clone(),
        // 1 -> signifies analaog alarm block
        // 5 -> signifies digital alarm block
        // Anything else should result in an error
        alarm_type: match alarm_info.alarm_block.as_ref().unwrap().pi {
            1 => crate::dpm::AlarmType::AnalogAlarm,
            5 => crate::dpm::AlarmType::DigitalAlarm,
            _ => panic!(),
        },
    }
}
