use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    store::Store,
};
use anyhow::Result;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{debug, error, info};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Role {
    Master,
    Replica,
}

type ReplicaSender = mpsc::Sender<RespValue>;

#[derive(Debug)]
pub struct ReplicationState {
    role: Role,
    master_replid: String,
    master_repl_offset: usize,
    replicas: tokio::sync::Mutex<Vec<ReplicaSender>>,
}

impl ReplicationState {
    pub fn new_from_config(config: &crate::config::Config) -> Self {
        let role = if config.replicaof.is_some() {
            Role::Replica
        } else {
            Role::Master
        };

        Self {
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            replicas: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn master_replid(&self) -> &str {
        &self.master_replid
    }

    pub async fn replica_count(&self) -> usize {
        self.replicas.lock().await.len()
    }

    pub fn info_string(&self) -> String {
        format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            if self.role == Role::Master { "master" } else { "slave" },
            self.master_replid,
            self.master_repl_offset
        )
    }

    pub async fn add_replica(&self, tx: ReplicaSender) {
        let mut replicas = self.replicas.lock().await;
        replicas.push(tx);
    }

    pub fn propagate(self: Arc<Self>, cmd: ParsedCommand) {
        tokio::spawn(async move {
            let replicas = self.replicas.lock().await;
            let resp_array = cmd.into_resp_array();

            for replica_tx in replicas.iter() {
                if let Err(e) = replica_tx.send(resp_array.clone()).await {
                    error!("Failed to propagate command to replica: {}", e);
                }
            }
        });
    }
}

pub async fn start_replica_mode(
    master_addr: String,
    listening_port: u16,
    _store: Arc<Store>,
    _replication: Arc<ReplicationState>,
) {
    info!("Attempting to connect to master at {master_addr}");
    match TcpStream::connect(master_addr).await {
        Ok(stream) => {
            info!("Successfully connected to master.");
            if let Err(e) = perform_handshake(stream, listening_port).await {
                error!("Handshake with master failed: {e}");
            }
        }
        Err(e) => {
            error!("Failed to connect to master: {e}");
        }
    }
}

async fn perform_handshake(stream: TcpStream, listening_port: u16) -> Result<()> {
    let mut framed = Framed::new(stream, RespDecoder);

    framed.send(RespValue::Array(vec![RespValue::BulkString(Bytes::from_static(b"PING"))])).await?;
    let _ = framed.next().await.unwrap()?;

    framed.send(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
        RespValue::BulkString(Bytes::from_static(b"listening-port")),
        RespValue::BulkString(Bytes::from(listening_port.to_string())),
    ])).await?;
    let _ = framed.next().await.unwrap()?;

    framed.send(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
        RespValue::BulkString(Bytes::from_static(b"capa")),
        RespValue::BulkString(Bytes::from_static(b"psync2")),
    ])).await?;
    let _ = framed.next().await.unwrap()?;

    framed.send(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"PSYNC")),
        RespValue::BulkString(Bytes::from_static(b"?")),
        RespValue::BulkString(Bytes::from_static(b"-1")),
    ])).await?;
    let _ = framed.next().await.unwrap()?;

    info!("Handshake complete. Listening for commands from master.");

    let mut offset = 0usize;
    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("Replica received value from master: {value:?}");
                let raw_bytes = value.encode_to_bytes();
                let parsed_command = ParsedCommand::from_resp(value)?;

                if parsed_command.command() == Command::ReplConf {
                    if parsed_command.arg(0).map_or(false, |a| a.eq_ignore_ascii_case(b"getack")) {
                        let ack_response = RespValue::Array(vec![
                            RespValue::BulkString(Bytes::from_static(b"REPLCONF")),
                            RespValue::BulkString(Bytes::from_static(b"ACK")),
                            RespValue::BulkString(Bytes::from(offset.to_string())),
                        ]);
                        framed.send(ack_response).await?;
                    }
                }
                offset += raw_bytes.len();
            }
            Some(Err(e)) => {
                error!("Error receiving from master: {e}");
                break;
            }
            None => {
                info!("Connection to master closed.");
                break;
            }
        }
    }

    Ok(())
}

pub async fn serve_replica(mut rx: mpsc::Receiver<RespValue>, mut stream: TcpStream) {
    info!("New replica serving task started.");

    while let Some(cmd) = rx.recv().await {
        let bytes = cmd.encode_to_bytes();
        if let Err(e) = stream.write_all(&bytes).await {
            error!("Failed to propagate command to replica: {}. Closing connection.", e);
            break;
        }
    }
    info!("Replica serving task finished.");
}