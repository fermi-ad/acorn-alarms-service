use rust_pubsub_lib::{Message, PubSubError, Publisher};

#[derive(Debug)]
pub struct TestPub {
    pub latest: Option<Message>,
    throw_err: bool,
}

impl TestPub {
    pub fn init_throwing() -> Self {
        Self {
            latest: None,
            throw_err: true,
        }
    }
}

impl Publisher for TestPub {
    fn new(_host: String, _topic: String) -> Self {
        Self {
            latest: None,
            throw_err: false,
        }
    }

    fn publish(&mut self, message: Message) -> Result<(), PubSubError> {
        if self.throw_err {
            return Err(PubSubError::default());
        }

        self.latest = Some(message);
        Ok(())
    }
}
