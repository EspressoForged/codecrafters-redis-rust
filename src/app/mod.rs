use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    replication::ReplicationState,
    store::Store,
    wait::WaiterRegistry,
};
use crate::config::Config;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};
use tokio::time;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

pub mod command;
pub mod error;
pub mod protocol;
pub mod rdb;
pub mod replication;
pub mod store;
pub mod wait;

enum ConnectionState {
    Normal,
    InTransaction,
}

pub async fn handle_connection(
    stream: TcpStream,
    store: Arc<Store>,
    waiters: Arc<WaiterRegistry>,
    config: Arc<Config>,
    replication: Arc<ReplicationState>,
) {
    let mut framed = Framed::new(stream, RespDecoder);
    let mut state = ConnectionState::Normal;
    let mut command_queue: Vec<ParsedCommand> = Vec::new();
    let wait_notify = Arc::new(Notify::new()); // Notify for WAIT commands

    loop {
        match framed.next().await {
            Some(Ok(value)) => {
                debug!("received RESP value: {value:?}");
                let parsed_command = match ParsedCommand::from_resp(value) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        if let Err(e) = framed.send(RespValue::Error(Bytes::from(e.to_string()))).await {
                            error!("failed to send error response: {e}");
                        }
                        continue;
                    }
                };

                if parsed_command.command() == Command::PSync {
                    info!("Replica requested PSYNC, starting full resync.");
                    let response = RespValue::SimpleString(Bytes::from(format!(
                        "FULLRESYNC {} 0",
                        replication.master_replid()
                    )));
                    if let Err(e) = framed.send(response).await {
                        error!("Failed to send FULLRESYNC response: {e}");
                        return;
                    }
                    let rdb_bytes = rdb::empty_rdb_bytes();
                    if let Err(e) = framed.get_mut().write_all(&rdb_bytes).await {
                        error!("Failed to send RDB file: {e}");
                        return;
                    }
                    
                    let (tx, rx) = mpsc::channel(128);
                    let ack_offset = replication.add_replica(tx).await;
                    tokio::spawn(replication::serve_replica(rx, framed, ack_offset, Arc::clone(&wait_notify)));
                    info!("Connection transitioned to replica serving mode, breaking main loop.");
                    return;
                }
                
                let response = handle_command(
                    parsed_command, &store, &waiters, &config, &replication,
                    &mut state, &mut command_queue, &wait_notify,
                ).await;

                if let Err(e) = framed.send(response).await {
                    error!("failed to send response: {e}");
                    return;
                }
            }
            Some(Err(e)) => { error!("error reading from stream: {e}"); return; }
            None => return,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
    config: &Arc<Config>,
    replication: &Arc<ReplicationState>,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    wait_notify: &Arc<Notify>,
) -> RespValue {
    if let Some(response) = handle_transaction_state(parsed.clone(), state, queue, store, waiters, config, replication, wait_notify).await {
        return response;
    }
    
    if matches!(state, ConnectionState::InTransaction) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    let command = parsed.command();
    
    let is_write_command = matches!(command, Command::Set | Command::LPush | Command::RPush | Command::LPop | Command::Incr);
    if is_write_command {
        if replication.role() == replication::Role::Replica {
            return RespValue::Error(Bytes::from_static(b"READONLY You can't write against a read only replica."));
        }
        Arc::clone(replication).propagate(parsed.clone());
    }

    match command {
        Command::Ping => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => handle_echo(parsed),
        Command::Set => handle_set(parsed, store),
        Command::Get => handle_get(parsed, store),
        Command::Config => handle_config(parsed, config),
        Command::Keys => handle_keys(parsed, store),
        Command::Info => handle_info(parsed, replication),
        Command::ReplConf => handle_replconf(parsed),
        Command::Wait => handle_wait(parsed, replication, wait_notify).await,
        Command::Incr => handle_incr(parsed, store),
        Command::LPush => handle_push(parsed, store, waiters, true),
        Command::RPush => handle_push(parsed, store, waiters, false),
        Command::LPop => handle_lpop(parsed, store),
        Command::LLen => handle_llen(parsed, store),
        Command::LRange => handle_lrange(parsed, store),
        Command::BLPop => handle_blpop(parsed, store, waiters).await,
        Command::PSync => RespValue::Error(Bytes::from_static(b"ERR PSYNC cannot be called in this context")),
        _ => RespValue::Error(Bytes::from(format!("ERR unknown command '{}'", parsed.command()))),
    }
}

async fn handle_transaction_state(
    parsed: ParsedCommand,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
    config: &Arc<Config>,
    replication: &Arc<ReplicationState>,
    wait_notify: &Arc<Notify>,
) -> Option<RespValue> {
    match parsed.command() {
        Command::Multi => {
            if matches!(state, ConnectionState::InTransaction) { return Some(RespValue::Error(Bytes::from_static(b"ERR MULTI calls can not be nested"))); }
            *state = ConnectionState::InTransaction;
            queue.clear();
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        }
        Command::Exec => {
            if !matches!(state, ConnectionState::InTransaction) { return Some(RespValue::Error(Bytes::from_static(b"ERR EXEC without MULTI"))); }
            *state = ConnectionState::Normal;
            let mut responses = Vec::with_capacity(queue.len());
            for cmd in std::mem::take(queue) {
                let response = Box::pin(handle_command(cmd, store, waiters, config, replication, &mut ConnectionState::Normal, &mut vec![], wait_notify)).await;
                responses.push(response);
            }
            Some(RespValue::Array(responses))
        }
        Command::Discard => {
            if !matches!(state, ConnectionState::InTransaction) { return Some(RespValue::Error(Bytes::from_static(b"ERR DISCARD without MULTI"))); }
            *state = ConnectionState::Normal;
            queue.clear();
            Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
        }
        _ => None,
    }
}

fn handle_info(parsed: ParsedCommand, replication: &ReplicationState) -> RespValue {
    let Some(section) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'info' command"));
    };
    if section.eq_ignore_ascii_case(b"replication") {
        RespValue::BulkString(Bytes::from(replication.info_string()))
    } else {
        RespValue::BulkString(Bytes::from_static(b""))
    }
}

fn handle_replconf(_parsed: ParsedCommand) -> RespValue {
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

async fn handle_wait(parsed: ParsedCommand, replication: &ReplicationState, wait_notify: &Arc<Notify>) -> RespValue {
    let (Some(num_replicas_str), Some(timeout_str)) = (parsed.arg(0), parsed.arg(1)) else {
         return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'wait' command"));
    };
    let num_replicas = match std::str::from_utf8(num_replicas_str).ok().and_then(|s| s.parse::<usize>().ok()) {
        Some(n) => n,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not an integer or out of range")),
    };
    let timeout_ms = match std::str::from_utf8(timeout_str).ok().and_then(|s| s.parse::<u64>().ok()) {
        Some(t) => t,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not an integer or out of range")),
    };

    let target_offset = replication.master_repl_offset();
    
    // Fast path: check if enough replicas are already synchronized.
    let already_synced = replication.count_acks(target_offset).await;
    if already_synced >= num_replicas {
        return RespValue::Integer(already_synced as i64);
    }
    
    // Slow path: broadcast GETACK and wait.
    replication.broadcast_getack().await;

    // Register this command's waiting condition.
    // The `wait_notify` is cloned and sent to the registry.
    replication.wait_waiters.register_wait_notifier(Arc::clone(wait_notify)).await;

    let timeout_future = time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(timeout_future);

    loop {
        tokio::select! {
            _ = &mut timeout_future => {
                // Timeout elapsed, return the number of replicas that have synced so far.
                let final_count = replication.count_acks(target_offset).await;
                return RespValue::Integer(final_count as i64);
            }
            _ = wait_notify.notified() => {
                // An ACK was received, check if we've met the condition.
                let synced_count = replication.count_acks(target_offset).await;
                if synced_count >= num_replicas {
                    return RespValue::Integer(synced_count as i64);
                }
                // If not enough replicas are synced, and we were notified,
                // this means another ACK came in, so we loop to await more.
            }
        }
    }
}

fn handle_config(parsed: ParsedCommand, config: &Config) -> RespValue {
    let Some(verb) = parsed.arg(0) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'config' command"));
    };
    let Some(key_bytes) = parsed.arg(1) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'config' command"));
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
        _ => RespValue::Array(vec![]),
    }
}

fn handle_keys(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(pattern) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'keys' command"));
    };
    
    if &pattern[..] != b"*" {
        warn!("Received KEYS command with non-'*' pattern, which is not supported.");
        return RespValue::Array(vec![]);
    }

    let keys = store.get_all_keys();
    RespValue::Array(keys.into_iter().map(RespValue::BulkString).collect())
}

fn handle_echo(parsed: ParsedCommand) -> RespValue {
    parsed
        .first()
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
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'get' command"));
    };
    match store.get_string(key) {
        Ok(Some(value)) => RespValue::BulkString(value),
        Ok(None) => RespValue::NullBulkString,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_incr(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'incr' command"));
    };
    match store.incr(key) {
        Ok(new_value) => RespValue::Integer(new_value),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_push(parsed: ParsedCommand, store: &Store, waiters: &WaiterRegistry, left: bool) -> RespValue {
    let Some(key) = parsed.first() else {
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
                waiters.notify_one(key);
            }
            RespValue::Integer(len as i64)
        }
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_lpop(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
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
    let Some((keys, timeout_str)) = parsed.args_from(0)
        .split_last()
        .map(|(last, elements)| (elements, last)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

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

    waiters.wait_for_any(keys, timeout).await;
    
    for key in keys {
        if let Ok(Some(popped)) = store.lpop(key, 1) {
            if let Some(value) = popped.into_iter().next() {
                return RespValue::Array(vec![RespValue::BulkString(key.clone()), RespValue::BulkString(value)]);
            }
        }
    }
    
    RespValue::NullBulkString
}


fn handle_llen(parsed: ParsedCommand, store: &Store) -> RespValue {
    let Some(key) = parsed.first() else {
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