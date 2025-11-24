use anyhow::Result;
use tonic::Request;

use crate::proto::services::devdb;
use devdb::dev_db_client::DevDbClient;
use devdb::{AlarmInfoReply, DeviceInfoReply, DeviceList};

//use crate::proto::services::devdb::dev_db_client::DevDbClient;
//use crate::proto::services::devdb::{DeviceInfoReply, DeviceList};

use super::model::DeviceSummary;

pub struct DevDBClient {
    inner: DevDbClient<tonic::transport::Channel>,
}

impl DevDBClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let inner = DevDbClient::connect(endpoint.to_string()).await?;
        Ok(Self { inner })
    }
}
