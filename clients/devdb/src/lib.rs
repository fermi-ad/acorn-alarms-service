// Existing generated module from DevDB.proto
pub mod devdb {
    tonic::include_proto!("devdb");
}

use devdb::dev_db_client::DevDbClient;
use devdb::{DeviceInfoReply, DeviceList};

pub async fn fetch_device_info(
    endpoint: &str,
    devices: &[String],
) -> anyhow::Result<DeviceInfoReply> {
    let mut client = DevDbClient::connect(endpoint.to_string()).await?;
    let req = tonic::Request::new(DeviceList {
        device: devices.to_vec(),
    });
    let resp = client.get_device_info(req).await?.into_inner();
    Ok(resp)
}
