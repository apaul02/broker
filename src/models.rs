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
    Fetch {
        topic_name: String,
        offset: u64,
        responder: oneshot::Sender<Result<Bytes, String>>,
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
                Command::Fetch {
                    topic_name,
                    offset,
                    responder,
                } => {
                    let res = self.topics.get(&topic_name);
                    if let Some(payload) = res {
                        if offset < payload.message.len() as u64 {
                            let _ = responder
                                .send(Ok(payload.message[offset as usize].payload.clone()));
                        } else {
                            let _ = responder.send(Err("Offset Out of Bounds".to_string()));
                        }
                    } else {
                        let _ = responder.send(Err("Topic not found".to_string()));
                    }
                }
            }
        }
    }
}
