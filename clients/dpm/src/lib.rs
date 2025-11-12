use anyhow::Result;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Streaming};

// Include both generated protobuf code trees
pub mod common {
    pub mod device {
        tonic::include_proto!("common.device");
    }
    pub mod status {
        tonic::include_proto!("common.status");
    }
}

pub mod services {
    pub mod daq {
        tonic::include_proto!("services.daq");
    }
}

use services::daq::daq_client::DaqClient;
use services::daq::{ReadingList, ReadingReply};

/// Create a gRPC channel with optional TLS depending on the endpoint.
async fn make_channel(endpoint: &str) -> Result<Channel> {
    let mut ep = Endpoint::from_shared(endpoint.to_string())?;
    if endpoint.starts_with("https://") {
        ep = ep.tls_config(ClientTlsConfig::new())?;
    }
    Ok(ep.connect().await?)
}

/// Fetch readings from the DPM service.
/// This method handles the streaming response from the `Read` RPC.
pub async fn fetch_readings(endpoint: &str, drf_list: Vec<String>) -> Result<Vec<ReadingReply>> {
    let channel = make_channel(endpoint).await?;
    let mut client = DaqClient::new(channel);

    let req = Request::new(ReadingList { drf: drf_list });

    // The Read() RPC returns a streaming response
    let mut stream: Streaming<ReadingReply> = client.read(req).await?.into_inner();

    let mut results = Vec::new();
    while let Some(reply) = stream.message().await? {
        results.push(reply);
    }

    Ok(results)
}
