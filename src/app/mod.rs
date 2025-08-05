use crate::app::{
    command::{Command, ParsedCommand},
    error::AppError,
    protocol::{RespDecoder, RespValue},
    store::{Store, WRONGTYPE_ERROR},
    wait::WaiterRegistry,
};
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
pub mod wait;

/// The entry point for handling a single client connection.
pub async fn handle_connection(
    stream: TcpStream,
    store: Arc<Store>,
    waiters: Arc<WaiterRegistry>,
) {
    let mut framed = Framed::new(stream, RespDecoder);

    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("received RESP value: {value:?}");
                let response = handle_command(value, &store, &waiters).await;
                if let Err(e) = framed.send(response).await {
                    error!("failed to send response: {e}");
                    return;
                }
            }
            Some(Err(e)) => {
                error!("error reading from stream: {e}");
                return;
            }
            None => return, // Stream closed
        }
    }
}

async fn handle_command(
    value: RespValue,
    store: &Store,
    waiters: &WaiterRegistry,
) -> RespValue {
    let parsed_command = match ParsedCommand::from_resp(value) {
        Ok(cmd) => cmd,
        Err(e) => return RespValue::Error(Bytes::from(e.to_string())),
    };

    debug!("parsed command: {parsed_command:?}");

    match parsed_command.command() {
        Command::Ping => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => handle_echo(parsed_command),
        Command::Set => handle_set(parsed_command, store),
        Command::Get => handle_get(parsed_command, store),
        Command::LPush => handle_push(parsed_command, store, waiters, true),
        Command::RPush => handle_push(parsed_command, store, waiters, false),
        Command::LPop => handle_lpop(parsed_command, store),
        Command::LLen => handle_llen(parsed_command, store),
        Command::LRange => handle_lrange(parsed_command, store),
        Command::BLPop => handle_blpop(parsed_command, store, waiters).await,
        _ => RespValue::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            parsed_command.command()
        ))),
    }
}

// --- Command Handlers ---

fn handle_echo(parsed: ParsedCommand) -> RespValue {
    parsed
        .arg(0)
        .map(|arg| RespValue::BulkString(arg.clone()))
        .unwrap_or_else(|| {
            RespValue::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'echo' command",
            ))
        })
}

fn handle_set(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(value)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'set' command"));
    };

    let mut expiry = None;
    if let Some(option) = parsed.arg(2) {
        if option.eq_ignore_ascii_case(b"px") {
            if let Some(millis_str) = parsed.arg(3) {
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

    if let Err(e) = store.set_string(key.clone(), value.clone(), expiry) {
        return RespValue::Error(Bytes::from(e.to_string()));
    }
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

fn handle_get(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'get' command"));
    };
    match store.get_string(key) {
        Ok(Some(value)) => RespValue::BulkString(value),
        Ok(None) => RespValue::NullBulkString,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_push(parsed: ParsedCommand, store: &Store, waiters: &WaiterRegistry, left: bool) -> RespValue {
    let Some(key) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    let values = parsed.args_from(1);
    if values.is_empty() {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    }

    let new_len = if left {
        store.lpush(key.clone(), values)
    } else {
        store.rpush(key.clone(), values)
    };

    match new_len {
        Ok(len) => {
            waiters.notify_one(key);
            RespValue::Integer(len as i64)
        }
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_lpop(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    let count = parsed
        .arg(1)
        .and_then(|s| std::str::from_utf8(s).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);

    match store.lpop(key, count) {
        Ok(Some(popped)) => {
            if popped.len() == 1 && count == 1 {
                RespValue::BulkString(popped.into_iter().next().unwrap())
            } else {
                RespValue::Array(popped.into_iter().map(RespValue::BulkString).collect())
            }
        }
        Ok(None) => RespValue::NullBulkString,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

async fn handle_blpop(parsed: ParsedCommand, store: &Store, waiters: &WaiterRegistry) -> RespValue {
    let Some(keys_arg) = parsed.args_from(0)
        .split_last()
        .map(|(last, elements)| (elements, last)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let (keys, timeout_str) = keys_arg;
    let timeout_secs = match std::str::from_utf8(timeout_str).ok().and_then(|s| s.parse::<f64>().ok()) {
        Some(t) => t,
        None => return RespValue::Error(Bytes::from_static(b"ERR timeout is not a float or out of range")),
    };

    for key in keys {
        if let Ok(Some(popped)) = store.lpop(key, 1) {
            if let Some(value) = popped.into_iter().next() {
                return RespValue::Array(vec![RespValue::BulkString(key.clone()), RespValue::BulkString(value)]);
            }
        }
    }

    let timeout = if timeout_secs == 0.0 {
        None
    } else {
        Some(Duration::from_secs_f64(timeout_secs))
    };

    let key = match waiters.wait_for_any(keys, timeout).await {
        Some(key) => key,
        None => return RespValue::NullBulkString,
    };

    if let Ok(Some(popped)) = store.lpop(&key, 1) {
        if let Some(value) = popped.into_iter().next() {
            return RespValue::Array(vec![RespValue::BulkString(key), RespValue::BulkString(value)]);
        }
    }
    
    // This can happen in a race condition if another client pops the value
    // between when we were notified and when we tried to pop.
    RespValue::NullBulkString
}


fn handle_llen(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };
    match store.llen(key) {
        Ok(len) => RespValue::Integer(len as i64),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_lrange(parsed: ParsedCommand, store: &Store) -> RespValue {
    let (Some(key), Some(start_str), Some(stop_str)) = (parsed.arg(0), parsed.arg(1), parsed.arg(2)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let start = match std::str::from_utf8(start_str).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(i) => i,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not an integer or out of range")),
    };
    let stop = match std::str::from_utf8(stop_str).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(i) => i,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not an integer or out of range")),
    };

    match store.lrange(key, start, stop) {
        Ok(items) => RespValue::Array(items.into_iter().map(RespValue::BulkString).collect()),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}