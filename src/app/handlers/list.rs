use crate::app::command::Command;
use crate::app::{command::ParsedCommand, protocol::RespValue, store::Store, wait::WaiterRegistry};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
// use tracing::warn;

pub async fn handle(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    match parsed.command() {
        Command::LPush => handle_push(parsed, store, waiters, true),
        Command::RPush => handle_push(parsed, store, waiters, false),
        Command::LPop => handle_lpop(parsed, store),
        Command::LLen => handle_llen(parsed, store),
        Command::LRange => handle_lrange(parsed, store),
        Command::BLPop => handle_blpop(parsed, store, waiters).await,
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown list command")),
    }
}

fn handle_push(
    parsed: ParsedCommand,
    store: &Store,
    waiters: &WaiterRegistry,
    left: bool,
) -> RespValue {
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
    let Some((keys, timeout_str)) = parsed
        .args_from(0)
        .split_last()
        .map(|(last, elements)| (elements, last))
    else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    };

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

    waiters.wait_for_any(keys, timeout).await;

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

    RespValue::NullArray
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
