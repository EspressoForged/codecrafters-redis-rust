use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    store::Store,
};
use bytes::Bytes;
use std::sync::Arc;

pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::ZAdd => handle_zadd(parsed, store),
        Command::ZCard => handle_zcard(parsed, store),
        Command::ZScore => handle_zscore(parsed, store),
        Command::ZRank => handle_zrank(parsed, store),
        Command::ZRange => handle_zrange(parsed, store),
        Command::ZRem => handle_zrem(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown sorted set command")),
    }
}

fn handle_zadd(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(score_bytes), Some(member)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zadd' command",
        ));
    };

    let score = match std::str::from_utf8(score_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };

    match store.zadd(key.clone(), score, member.clone()) {
        Ok(count) => RespValue::Integer(count as i64),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_zcard(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zcard' command",
        ));
    };
    match store.zcard(key) {
        Ok(count) => RespValue::Integer(count as i64),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_zscore(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zscore' command",
        ));
    };
    match store.zscore(key, member) {
        Ok(Some(score)) => RespValue::BulkString(Bytes::from(score.to_string())),
        Ok(None) => RespValue::NullBulkString,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_zrank(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrank' command",
        ));
    };
    match store.zrank(key, member) {
        Ok(Some(rank)) => RespValue::Integer(rank as i64),
        Ok(None) => RespValue::NullBulkString,
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_zrange(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(start_bytes), Some(stop_bytes)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrange' command",
        ));
    };

    let start = match std::str::from_utf8(start_bytes)
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
    let stop = match std::str::from_utf8(stop_bytes)
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

    match store.zrange(key, start, stop) {
        Ok(members) => RespValue::Array(members.into_iter().map(RespValue::BulkString).collect()),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_zrem(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrem' command",
        ));
    };
    match store.zrem(key, member) {
        Ok(count) => RespValue::Integer(count as i64),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}
