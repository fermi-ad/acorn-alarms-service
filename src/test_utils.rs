//! Shared test fixtures and helpers for binary-crate modules.

use std::sync::{Arc, Mutex};

use rust_pubsub_lib::{ByteMessage, Message, PubSubError, Publisher};

use crate::{
    proto::{
        common::alarm::{
            Status,
            status::{Severity, Source, State},
        },
        google::protobuf::Timestamp,
    },
    runtime::{self, AlarmStateIngress, QueueCapacityConfig, hydration::HydratedStatuses},
};

pub const DEFAULT_TEST_QUEUE_CONFIG: QueueCapacityConfig = QueueCapacityConfig {
    automated: 10,
    user: 10,
    job: 10,
    publish: 10,
    snooze: 10,
};

/// Test publisher that records the latest published message and can simulate failures.
#[derive(Debug)]
pub struct TestPub {
    latest: Arc<Mutex<Option<ByteMessage>>>,
    throw_err: bool,
}

impl TestPub {
    /// Creates a publisher that always returns an error after recording the message.
    pub fn init_throwing() -> Self {
        Self {
            latest: Arc::default(),
            throw_err: true,
        }
    }

    /// Creates a publisher with an internal message store.
    pub fn init() -> Self {
        Self::new(String::new(), String::new())
    }

    /// Creates a publisher that writes the latest message into the provided shared store.
    pub fn init_inspectable(dropbox: Arc<Mutex<Option<ByteMessage>>>) -> Self {
        Self {
            latest: dropbox,
            throw_err: false,
        }
    }
}

#[tonic::async_trait]
impl Publisher for TestPub {
    fn new(_host: String, _topic: String) -> Self {
        Self {
            latest: Arc::default(),
            throw_err: false,
        }
    }

    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        let mut latest_val = self.latest.lock().expect("latest lock poisoned");
        let _ = latest_val.insert(message.into_bytes());
        (!self.throw_err)
            .then_some(())
            .ok_or_else(PubSubError::default)
    }
}

pub async fn get_runtime() -> AlarmStateIngress {
    runtime::start(
        TestPub::init(),
        DEFAULT_TEST_QUEUE_CONFIG,
        HydratedStatuses::new(),
    )
    .await
}

pub async fn get_throwing_runtime() -> AlarmStateIngress {
    runtime::start(
        TestPub::init_throwing(),
        DEFAULT_TEST_QUEUE_CONFIG,
        HydratedStatuses::new(),
    )
    .await
}

/// Builds a minimal [`Status`] for use in tests.
pub fn make_status(device: &str, state: State, source: Source) -> Status {
    Status {
        device: device.to_string(),
        state: state as i32,
        severity: Severity::Low as i32,
        source: source as i32,
        acknowledgeable: false,
        time: Some(Timestamp {
            seconds: 0,
            nanos: 0,
        }),
        epics_type: String::new(),
        user: String::new(),
        wake: None,
    }
}
