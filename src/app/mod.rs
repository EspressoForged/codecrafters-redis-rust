use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    store::Store,
    wait::WaiterRegistry,
};
use crate::config::Config;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, error, warn};

pub mod command;
pub mod error;
pub mod protocol;
pub mod rdb;
pub mod store;
pub mod wait;

/// Represents the state of a single client connection.
enum ConnectionState {
    Normal,
    InTransaction,
}

/// The entry point for handling a single client connection.
pub async fn handle_connection(
    stream: TcpStream,
    store: Arc<Store>,
    waiters: Arc<WaiterRegistry>,
    config: Arc<Config>,
) {
    let mut framed = Framed::new(stream, RespDecoder);
    let mut state = ConnectionState::Normal;
    let mut command_queue: Vec<ParsedCommand> = Vec::new();

    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("received RESP value: {value:?}");
                let parsed_command = match ParsedCommand::from_resp(value) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        if let Err(e) = framed
                            .send(RespValue::Error(Bytes::from(e.to_string())))
                            .await
                        {
                            error!("failed to send error response: {e}");
                        }
                        continue;
                    }
                };

                let response = handle_command(
                    parsed_command,
                    &store,
                    &waiters,
                    &config,
                    &mut state,
                    &mut command_queue,
                )
                .await;

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

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
    config: &Arc<Config>,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
) -> RespValue {
    if let Some(response) =
        handle_transaction_state(parsed.clone(), state, queue, store, waiters, config).await
    {
        return response;
    }

    if matches!(state, ConnectionState::InTransaction) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    match parsed.command() {
        Command::Ping => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => handle_echo(parsed),
        Command::Set => handle_set(parsed, store),
        Command::Get => handle_get(parsed, store),
        Command::Config => handle_config(parsed, config),
        Command::Keys => handle_keys(parsed, store),
        Command::Incr => handle_incr(parsed, store),
        Command::LPush => handle_push(parsed, store, waiters, true),
        Command::RPush => handle_push(parsed, store, waiters, false),
        Command::LPop => handle_lpop(parsed, store),
        Command::LLen => handle_llen(parsed, store),
        Command::LRange => handle_lrange(parsed, store),
        Command::BLPop => handle_blpop(parsed, store, waiters).await,
        _ => RespValue::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            parsed.command()
        ))),
    }
}

async fn handle_transaction_state(
    parsed: ParsedCommand,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
    config: &Arc<Config>,
) -> Option<RespValue> {
    match parsed.command() {
        Command::Multi => {
            if matches!(state, ConnectionState::InTransaction) {
                return Some(RespValue::Error(Bytes::from_static(
                    b"ERR MULTI calls can not be nested",
                )));
            }
            *state = ConnectionState::InTransaction;
            queue.clear();
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        }
        Command::Exec => {
            if !matches!(state, ConnectionState::InTransaction) {
                return Some(RespValue::Error(Bytes::from_static(
                    b"ERR EXEC without MULTI",
                )));
            }
            *state = ConnectionState::Normal;
            let mut responses = Vec::with_capacity(queue.len());
            for cmd in std::mem::take(queue) {
                let response = Box::pin(handle_command(
                    cmd,
                    store,
                    waiters,
                    config,
                    &mut ConnectionState::Normal,
                    &mut vec![],
                ))
                .await;
                responses.push(response);
            }
            Some(RespValue::Array(responses))
        }
        Command::Discard => {
            if !matches!(state, ConnectionState::InTransaction) {
                return Some(RespValue::Error(Bytes::from_static(
                    b"ERR DISCARD without MULTI",
                )));
            }
            *state = ConnectionState::Normal;
            queue.clear();
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        }
        _ => None,
    }
}

// --- Command Handlers ---

fn handle_config(parsed: ParsedCommand, config: &Config) -> RespValue {
    let Some(verb) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    };
    let Some(key_bytes) = parsed.arg(1) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    };

    if !verb.eq_ignore_ascii_case(b"get") {
        return RespValue::Error(Bytes::from_static(b"ERR CONFIG only supports GET"));
    }

    let key = match std::str::from_utf8(key_bytes) {
        Ok(s) => s,
        Err(_) => return RespValue::Error(Bytes::from_static(b"ERR invalid config key")),
    };

    match key.to_lowercase().as_str() {
        "dir" => RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"dir")),
            RespValue::BulkString(Bytes::from(config.dir.clone())),
        ]),
        "dbfilename" => RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"dbfilename")),
            RespValue::BulkString(Bytes::from(config.dbfilename.clone())),
        ]),
        _ => RespValue::Array(vec![]), // Unknown config key
    }
}

fn handle_keys(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(pattern) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'keys' command",
        ));
    };

    // CORRECTED: Dereference the `Bytes` object to a slice for comparison.
    if &pattern[..] != b"*" {
        warn!("Received KEYS command with non-'*' pattern, which is not supported.");
        return RespValue::Array(vec![]);
    }

    let keys = store.get_all_keys();
    RespValue::Array(keys.into_iter().map(RespValue::BulkString).collect())
}

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
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'set' command",
        ));
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
                    return RespValue::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
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
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'get' command",
        ));
    };
    match store.get_string(key) {
        Ok(Some(value)) => RespValue::BulkString(value),
        Ok(None) => RespValue::NullBulkString,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_incr(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'incr' command",
        ));
    };
    match store.incr(key) {
        Ok(new_value) => RespValue::Integer(new_value),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_push(
    parsed: ParsedCommand,
    store: &Store,
    waiters: &WaiterRegistry,
    left: bool,
) -> RespValue {
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
            if len > 0 {
                // Only notify if a list was actually modified
                waiters.notify_one(key);
            }
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
    let Some(keys_arg) = parsed
        .args_from(0)
        .split_last()
        .map(|(last, elements)| (elements, last))
    else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let (keys, timeout_str) = keys_arg;
    let timeout_secs = match std::str::from_utf8(timeout_str)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(t) => t,
        None => {
            return RespValue::Error(Bytes::from_static(
                b"ERR timeout is not a float or out of range",
            ))
        }
    };

    for key in keys {
        if let Ok(Some(popped)) = store.lpop(key, 1) {
            if let Some(value) = popped.into_iter().next() {
                return RespValue::Array(vec![
                    RespValue::BulkString(key.clone()),
                    RespValue::BulkString(value),
                ]);
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
            return RespValue::Array(vec![
                RespValue::BulkString(key),
                RespValue::BulkString(value),
            ]);
        }
    }

    warn!(
        "BLPOP notified for key '{:?}' but no value was available to pop",
        String::from_utf8_lossy(&key)
    );
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
    let (Some(key), Some(start_str), Some(stop_str)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

    let start = match std::str::from_utf8(start_str)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(i) => i,
        None => {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match std::str::from_utf8(stop_str)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(i) => i,
        None => {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };

    match store.lrange(key, start, stop) {
        Ok(items) => RespValue::Array(items.into_iter().map(RespValue::BulkString).collect()),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}
