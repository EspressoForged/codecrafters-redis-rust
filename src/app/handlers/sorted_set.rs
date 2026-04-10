use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    sorted_set::SortedSet,
    store::{DataType, Store, StoreValue},
};
use bytes::Bytes;
use std::sync::{Arc, RwLock};

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

    let entry = store.db().entry(key.clone()).or_insert_with(|| StoreValue {
        data: DataType::SortedSet(RwLock::new(SortedSet::new())),
        expires_at: None,
    });

    match &entry.value().data {
        DataType::SortedSet(zset_lock) => {
            let mut zset = zset_lock.write().unwrap();
            RespValue::Integer(zset.add(score, member.clone()) as i64)
        }
        _ => RespValue::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
    }
}

fn handle_zcard(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zcard' command",
        ));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                let zset = zset_lock.read().unwrap();
                RespValue::Integer(zset.cardinality() as i64)
            }
            _ => RespValue::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        _ => RespValue::Integer(0), // Key doesn't exist
    }
}

fn handle_zscore(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zscore' command",
        ));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                let zset = zset_lock.read().unwrap();
                match zset.score_of(member) {
                    Some(score) => RespValue::BulkString(Bytes::from(score.to_string())),
                    None => RespValue::NullBulkString,
                }
            }
            _ => RespValue::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        _ => RespValue::NullBulkString, // Key doesn't exist
    }
}

fn handle_zrank(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrank' command",
        ));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                let zset = zset_lock.read().unwrap();
                match zset.rank_of(member) {
                    Some(rank) => RespValue::Integer(rank as i64),
                    None => RespValue::NullBulkString,
                }
            }
            _ => RespValue::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        _ => RespValue::NullBulkString, // Key doesn't exist
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

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                let zset = zset_lock.read().unwrap();
                let len = zset.cardinality() as i64;
                let start_idx = store.normalize_index(start, len);
                let stop_idx = store.normalize_index(stop, len);

                if start_idx > stop_idx {
                    return RespValue::Array(vec![]);
                }
                let members = zset.get_range(start_idx, stop_idx);
                RespValue::Array(members.into_iter().map(RespValue::BulkString).collect())
            }
            _ => RespValue::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        _ => RespValue::Array(vec![]), // Key doesn't exist
    }
}

fn handle_zrem(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'zrem' command",
        ));
    };

    match store.db().get(key) {
        Some(entry) if !store.is_expired(&entry) => match &entry.data {
            DataType::SortedSet(zset_lock) => {
                let mut zset = zset_lock.write().unwrap();
                RespValue::Integer(zset.remove(member) as i64)
            }
            _ => RespValue::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        _ => RespValue::Integer(0), // Key doesn't exist
    }
}
