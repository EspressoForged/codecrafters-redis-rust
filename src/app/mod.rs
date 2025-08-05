use crate::app::{
    command::{Command, ParsedCommand},
    store::Store,
};
use crate::app::protocol::{RespDecoder, RespValue};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, error};

pub mod command;
pub mod error;
pub mod protocol;
pub mod store;

/// The entry point for handling a single client connection.
pub async fn handle_connection(stream: TcpStream, store: Arc<Store>) {
    let mut framed = Framed::new(stream, RespDecoder);

    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("received RESP value: {value:?}");
                let response = handle_command(value, &store).await;
                if let Err(e) = framed.send(response).await {
                    error!("failed to send response: {e}");
                    return;
                }
            }
            Some(Err(e)) => {
                error!("error reading from stream: {e}");
                return;
            }
            None => {
                // Stream closed
                return;
            }
        }
    }
}

async fn handle_command(value: RespValue, store: &Store) -> RespValue {
    let parsed_command = match ParsedCommand::from_resp(value) {
        Ok(cmd) => cmd,
        Err(e) => return RespValue::Error(Bytes::from(e.to_string())),
    };

    debug!("parsed command: {parsed_command:?}");

    match parsed_command.command() {
        Command::Ping => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => parsed_command
            .arg(0)
            .map(|arg| RespValue::BulkString(arg.clone()))
            .unwrap_or_else(|| {
                RespValue::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'echo' command",
                ))
            }),
        Command::Set => handle_set(parsed_command, store).await,
        Command::Get => handle_get(parsed_command, store).await,
        _ => RespValue::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            parsed_command.command()
        ))),
    }
}

async fn handle_set(parsed_command: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(value)) = (parsed_command.arg(0), parsed_command.arg(1)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'set' command"));
    };

    let mut expiry = None;
    if let Some(option) = parsed_command.arg(2) {
        if option.eq_ignore_ascii_case(b"px") {
            if let Some(millis_str) = parsed_command.arg(3) {
                if let Some(millis) = std::str::from_utf8(millis_str)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    expiry = Some(Duration::from_millis(millis));
                } else {
                    return RespValue::Error(Bytes::from_static(b"ERR value is not an integer or out of range"));
                }
            } else {
                 return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
            }
        } else {
            return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    store.set(key.clone(), value.clone(), expiry);
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

async fn handle_get(parsed_command: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed_command.arg(0) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'get' command"));
    };

    match store.get(key) {
        Some(value) => RespValue::BulkString(value),
        None => RespValue::NullBulkString,
    }
}