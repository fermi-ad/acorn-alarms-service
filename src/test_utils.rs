use std::sync::Mutex;

use rust_pubsub_lib::{Message, PubSubError, Publisher, StringMessage};

#[derive(Debug)]
pub struct TestPub {
    pub latest: Mutex<Option<StringMessage>>,
    throw_err: bool,
}

impl TestPub {
    pub fn init_throwing() -> Self {
        Self {
            latest: Mutex::new(None),
            throw_err: true,
        }
    }

    pub fn init() -> Self {
        Self::new(String::new(), String::new())
    }

    /// Convenience accessor for tests that need to read the last published message.
    pub fn get_latest(&self) -> Option<StringMessage> {
        self.latest.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl Publisher for TestPub {
    fn new(_host: String, _topic: String) -> Self {
        Self {
            latest: Mutex::new(None),
            throw_err: false,
        }
    }

    async fn publish<M: Message>(&self, message: M) -> Result<(), PubSubError> {
        if self.throw_err {
            return Err(PubSubError::default());
        }
        let string_msg = StringMessage::from(message.into_bytes());
        *self.latest.lock().unwrap() = Some(string_msg);
        Ok(())
    }
}
