use anyhow::Result;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

pub mod services {
    pub mod daq {
        tonic::include_proto!("services.daq");
    }
}

use services::daq::daq_client::DaqClient;
use services::daq::{ReadingList, ReadingReply};

pub async fn fetch_readings(endpoint: &str, drfs: Vec<String>) -> Result<Vec<ReadingReply>> {
    let channel = Channel::from_shared(endpoint.to_string())?
        .connect()
        .await?;

    let mut client = DaqClient::new(channel);

    let req = Request::new(ReadingList { drf: drfs });

    let mut stream: Streaming<ReadingReply> = client.read(req).await?.into_inner();

    let mut out = Vec::new();
    while let Some(msg) = stream.message().await? {
        out.push(msg);
    }

    Ok(out)
}

