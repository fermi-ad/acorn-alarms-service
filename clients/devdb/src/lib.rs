use anyhow::Result;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

pub mod devdb {
    tonic::include_proto!("services.devdb");
}

use devdb::dev_db_client::DevDbClient;
use devdb::{DeviceInfoReply, DeviceList};

async fn make_channel(endpoint: &str) -> Result<Channel> {
    let mut ep = Endpoint::from_shared(endpoint.to_string())?;
    if endpoint.starts_with("https://") {
        ep = ep.tls_config(ClientTlsConfig::new())?;
    }
    Ok(ep.connect().await?)
}

pub async fn fetch_device_info(endpoint: &str, devices: Vec<String>) -> Result<DeviceInfoReply> {
    let channel = make_channel(endpoint).await?;
    let mut client = DevDbClient::new(channel);

    let req = Request::new(DeviceList { device: devices });
    let resp = client.get_device_info(req).await?.into_inner();

    Ok(resp)
}
