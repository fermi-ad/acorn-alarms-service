use anyhow::Result;
use tonic::transport::Channel;
use tonic::Request;

use crate::proto::services::devdb::{dev_db_client::DevDbClient, DeviceInfoReply, DeviceList};

use super::model::DeviceSummary;

pub struct DevDBClient {
    inner: DevDbClient<Channel>,
}

impl DevDBClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let inner = DevDbClient::connect(endpoint.to_string()).await?;
        Ok(Self { inner })
    }

    pub async fn fetch_raw(&mut self, devices: Vec<String>) -> Result<DeviceInfoReply> {
        let req = Request::new(DeviceList { device: devices });
        let reply = self.inner.get_device_info(req).await?.into_inner();
        Ok(reply)
    }

    pub async fn fetch_summary(&mut self, devices: Vec<String>) -> Result<Vec<DeviceSummary>> {
        let raw = self.fetch_raw(devices).await?;
        let mut out = Vec::new();

        for entry in raw.set {
            let name = entry.name.clone();

            if let Some(res) = entry.result {
                match res {
                    crate::proto::services::devdb::info_entry::Result::Device(info) => {
                        out.push(DeviceSummary {
                            name,
                            description: Some(info.description),
                            reading_units: info
                                .reading
                                .as_ref()
                                .and_then(|p| p.primary_units.clone()),
                            setting_units: info
                                .setting
                                .as_ref()
                                .and_then(|p| p.primary_units.clone()),
                            num_status_bits: info
                                .status
                                .as_ref()
                                .map(|d| d.bits.len())
                                .unwrap_or(0),
                            num_control_cmds: info
                                .control
                                .as_ref()
                                .map(|c| c.cmds.len())
                                .unwrap_or(0),
                            error_message: None,
                        });
                    }
                    crate::proto::services::devdb::info_entry::Result::ErrMsg(msg) => {
                        out.push(DeviceSummary {
                            name,
                            description: None,
                            reading_units: None,
                            setting_units: None,
                            num_status_bits: 0,
                            num_control_cmds: 0,
                            error_message: Some(msg),
                        });
                    }
                }
            }
        }

        Ok(out)
    }
}
