use bytes::Bytes;
use tokio::io::AsyncBufReadExt;
use tokio::{io::AsyncWriteExt, sync::mpsc};

use crate::models::{BrokerState, Command};

pub mod models;
#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<Command>(1024);
    let state = BrokerState::restore().await;
    println!(
        "Broker booted! Recovered {} topics from disk.",
        state.topics.len()
    );
    tokio::spawn(async move {
        state.listen(rx).await;
    });
    let socket = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();
    loop {
        let (stream, _) = socket.accept().await.unwrap();
        let t = tx.clone();
        let (read, mut writer) = stream.into_split();
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(read);
            loop {
                let mut s = String::new();
                let res = reader.read_line(&mut s).await;
                if let Ok(0) = res {
                    break;
                }
                let mut parts = s.split_whitespace();
                let cmd = parts.next();
                if let Some(c) = cmd {
                    match c {
                        "PUBLISH" => {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            if let (Some(topic), Some(payload)) = (parts.next(), parts.next()) {
                                let command = Command::Publish {
                                    topic_name: topic.to_string(),
                                    payload: Bytes::from(payload.to_string()),
                                    responder: resp_tx,
                                };
                                t.send(command).await.unwrap();

                                let res = resp_rx.await.unwrap();
                                if let Ok(_a) = res {
                                    let _ = writer.write(b"Success\n").await;
                                }
                            } else {
                                let _ = writer.write(b"ERROR: BAD FORMAT\n").await;
                            }
                        }
                        "FETCH" => {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            if let (Some(topic), Some(offset)) = (parts.next(), parts.next()) {
                                let offset = offset.parse::<u64>();
                                if let Ok(o) = offset {
                                    let command = Command::Fetch {
                                        topic_name: topic.to_string(),
                                        offset: o,
                                        responder: resp_tx,
                                    };
                                    t.send(command).await.unwrap();
                                    let res = resp_rx.await.unwrap();
                                    match res {
                                        Ok(payload) => {
                                            let data = [payload.as_ref(), b"\n"].concat();
                                            let _ = writer.write(&data).await;
                                        }
                                        Err(e) => {
                                            let response = format!("{}\n", e);
                                            let _ = writer.write(response.as_bytes()).await;
                                        }
                                    }
                                } else {
                                    let _ = writer.write(b"Error parsing offset\n").await;
                                }
                            }
                        }
                        "FETCH_NEXT" => {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            if let (Some(group_name), Some(topic_name)) =
                                (parts.next(), parts.next())
                            {
                                let command = Command::FetchNext {
                                    topic_name: topic_name.to_string(),
                                    group_name: group_name.to_string(),
                                    responder: resp_tx,
                                };
                                t.send(command).await.unwrap();
                                let res = resp_rx.await.unwrap();
                                match res {
                                    Ok(payload) => {
                                        let data = [payload.as_ref(), b"\n"].concat();
                                        let _ = writer.write(&data).await;
                                    }
                                    Err(e) => {
                                        let response = format!("{}\n", e);
                                        let _ = writer.write(response.as_bytes()).await;
                                    }
                                }
                            } else {
                                let _ = writer.write(b"Error parsing command\n").await;
                            }
                        }
                        "ACK" => {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            if let (Some(group_name), Some(topic_name)) =
                                (parts.next(), parts.next())
                            {
                                let command = Command::Ack {
                                    topic_name: topic_name.to_string(),
                                    group_name: group_name.to_string(),
                                    responder: resp_tx,
                                };
                                t.send(command).await.unwrap();
                                let res = resp_rx.await.unwrap();
                                match res {
                                    Ok(()) => {
                                        let _ = writer.write(b"ACK_ON\n").await;
                                    }
                                    Err(e) => {
                                        let response = format!("{}\n", e);
                                        let _ = writer.write(response.as_bytes()).await;
                                    }
                                }
                            } else {
                                let _ = writer.write(b"Error parsing command\n").await;
                            }
                        }
                        _ => {
                            let _ = writer.write(b"WILL BE IMPLEMENTED\n").await;
                        }
                    }
                }
            }
        });
    }
}
