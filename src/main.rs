use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::models::{BrokerState, Command};

pub mod models;
#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<Command>(1024);
    let state = BrokerState {
        topics: HashMap::new(),
    };
    tokio::spawn(async move {
        state.listen(rx).await;
    });
    let socket = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();
    loop {
        let a = socket.accept().await.unwrap();
        let t = tx.clone();
        tokio::spawn(async move {
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            let cmd = Command::Publish {
                topic_name: "Idk".to_string(),
                payload: Bytes::from("yooo"),
                responder: resp_tx,
            };
            t.send(cmd).await.unwrap();
        });
    }
}
