use anyhow::Result;
use tonic::Request;

use crate::proto::services::devdb::dev_db_client::DevDbClient;
use crate::proto::services::devdb::*;

pub async fn fetch_device_info(endpoint: &str, names: Vec<String>) -> Result<DeviceInfoReply> {
    let mut client = DevDbClient::connect(endpoint.to_string()).await?;
    let req = Request::new(DeviceList { device: names });
    let resp = client.get_device_info(req).await?.into_inner();
    Ok(resp)
}
