use crate::proto::services::daq::daq_client::DaqClient;
use crate::proto::services::daq::*;
use anyhow::Result;
use tonic::Request;

pub async fn fetch_readings(endpoint: &str, drf_list: Vec<String>) -> Result<Vec<ReadingReply>> {
    let mut client = DaqClient::connect(endpoint.to_string()).await?;

    let req = Request::new(ReadingList { drf: drf_list });

    let mut stream = client.read(req).await?.into_inner();

    let mut out = Vec::new();
    while let Some(item) = stream.message().await? {
        out.push(item);
    }

    Ok(out)
}
