use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};

pub struct Message {
    pub payload: Bytes,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

pub struct Topic {
    pub message: Vec<Message>,
    pub consumer_groups: HashMap<String, u64>,
}

pub enum Command {
    Publish {
        topic_name: String,
        payload: Bytes,
        responder: oneshot::Sender<Result<u64, String>>,
    },
}

pub struct BrokerState {
    pub topics: HashMap<String, Topic>,
}

impl BrokerState {
    pub async fn listen(mut self, mut rx: mpsc::Receiver<Command>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Publish {
                    topic_name,
                    payload,
                    responder,
                } => {
                    let topic = self.topics.entry(topic_name).or_insert(Topic {
                        message: vec![],
                        consumer_groups: HashMap::new(),
                    });
                    let offset = topic.message.len() as u64;
                    let message = Message {
                        payload,
                        offset,
                        timestamp: Utc::now(),
                    };
                    topic.message.push(message);
                    let _ = responder.send(Ok(offset));
                }
            }
        }
    }
}
