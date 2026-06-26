//! Adapter implementations for external ingress sources (gRPC, Redis, EPICS hydration).

pub mod acnet_hydration;
pub mod epics_hydration;
pub mod grpc;
pub mod redis;
