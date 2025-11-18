use anyhow::Result;
use tonic::transport::Channel;
use tonic::Request;

pub mod services {
    pub mod devdb {
        tonic::include_proto!("services.devdb");
    }
}

use services::devdb::dev_db_client::DevDbClient;
use services::devdb::{DeviceInfoReply, DeviceList};

pub async fn fetch_device_info(endpoint: &str, devices: Vec<String>) -> Result<DeviceInfoReply> {
    let channel = Channel::from_shared(endpoint.to_string())?
        .connect()
        .await?;

    let mut client = DevDbClient::new(channel);

    let req = Request::new(DeviceList { device: devices });

    let resp = client.get_device_info(req).await?.into_inner();

    Ok(resp)
}
