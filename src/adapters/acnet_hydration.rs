use rust_grpc_lib::pool;
use tonic::transport::Channel;

use crate::{
    model::key::Key,
    proto::{
        google::protobuf::Empty,
        services::aeolus_snapshot_proxy::aeolus_proxy_client::AeolusProxyClient,
    },
    runtime::hydration::{HydratedStatuses, HydrationError},
};

pub async fn load_acnet_hydration(host: String) -> Result<HydratedStatuses, HydrationError> {
    let mut aeolus_proxy: AeolusProxyClient<Channel> =
        pool::get(&host).map_err(HydrationError::AeolusProxyConnectionFailed)?;

    let response = aeolus_proxy
        .get_alarms_snapshot(Empty {})
        .await
        .map_err(HydrationError::AcnetSnapshotReadFailed)?;

    let hydrated = response
        .into_inner()
        .snapshot
        .into_iter()
        .map(|status| (Key::from(&status), status))
        .collect();

    Ok(hydrated)
}
