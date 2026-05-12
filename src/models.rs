use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

pub struct Message {
    pub payload: Bytes,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

pub struct Topic {
    pub message: Vec<Message>,
    pub consumer_groups: HashMap<String, u64>,
    pub file: tokio::io::BufWriter<tokio::fs::File>,
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
    FetchNext {
        topic_name: String,
        group_name: String,
        responder: oneshot::Sender<Result<Bytes, String>>,
    },
    Ack {
        topic_name: String,
        group_name: String,
        responder: oneshot::Sender<Result<(), String>>,
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
                    if !self.topics.contains_key(&topic_name) {
                        let file_name = format!("{}.log", topic_name);

                        let f = tokio::fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(file_name)
                            .await
                            .unwrap();
                        let writer = tokio::io::BufWriter::new(f);
                        let new_topic = Topic {
                            message: vec![],
                            consumer_groups: HashMap::new(),
                            file: writer,
                        };
                        self.topics.insert(topic_name.clone(), new_topic);
                    }
                    let topic = self.topics.get_mut(&topic_name).unwrap();
                    let offset = topic.message.len() as u64;

                    let result = topic.append_to_disk(&payload, offset).await;
                    if let Err(e) = result {
                        let _ = responder.send(Err(e.to_string()));
                    } else {
                        let message = Message {
                            payload,
                            offset,
                            timestamp: Utc::now(),
                        };
                        topic.message.push(message);
                        let _ = responder.send(Ok(offset));
                    }
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
                Command::FetchNext {
                    topic_name,
                    group_name,
                    responder,
                } => {
                    if let Some(topic) = self.topics.get_mut(&topic_name) {
                        let offset = topic.consumer_groups.entry(group_name).or_insert(0);
                        if *offset >= topic.message.len() as u64 {
                            let _ = responder.send(Err("No new Message".to_string()));
                        } else {
                            let message = topic.message[*offset as usize].payload.clone();
                            let _ = responder.send(Ok(message));
                        }
                    } else {
                        let _ = responder.send(Err("Topic doesnt exists".to_string()));
                    }
                }
                Command::Ack {
                    topic_name,
                    group_name,
                    responder,
                } => {
                    if let Some(topic) = self.topics.get_mut(&topic_name) {
                        let offset = topic.consumer_groups.entry(group_name).or_insert(0);
                        *offset += 1;
                        let _ = responder.send(Ok(()));
                    } else {
                        let _ = responder.send(Err("Topic doesnt exists".to_string()));
                    }
                }
            }
        }
    }
    pub async fn restore() -> BrokerState {
        let mut topics = HashMap::new();

        let mut entries = tokio::fs::read_dir(".").await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();

            if path.extension().is_some_and(|e| e == "log") {
                let topic_name = path.file_stem().unwrap().to_str().unwrap().to_string();

                let mut file = tokio::fs::File::open(&path).await.unwrap();
                let mut messages = vec![];

                loop {
                    let payload_len = match file.read_u64_le().await {
                        Ok(len) => len,
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => panic!("Fatal error reading log: {}", e),
                    };

                    let offset = file.read_u64_le().await.unwrap();

                    let mut payload_buf = vec![0u8; payload_len as usize];

                    file.read_exact(&mut payload_buf).await.unwrap();

                    messages.push(Message {
                        payload: Bytes::from(payload_buf),
                        offset,
                        timestamp: Utc::now(),
                    });
                }

                let append_file = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .await
                    .unwrap();
                let writer = tokio::io::BufWriter::new(append_file);

                let topic = Topic {
                    message: messages,
                    consumer_groups: HashMap::new(),
                    file: writer,
                };
                topics.insert(topic_name, topic);
            }
        }

        BrokerState { topics }
    }
}

impl Topic {
    pub async fn append_to_disk(&mut self, payload: &Bytes, offset: u64) -> std::io::Result<()> {
        self.file.write_u64_le(payload.len() as u64).await?;
        self.file.write_u64_le(offset).await?;
        self.file.write_all(payload).await?;
        self.file.flush().await?;

        Ok(())
    }
}
