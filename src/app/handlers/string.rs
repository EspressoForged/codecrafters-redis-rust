use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
    store::Store,
};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::Set => handle_set(parsed, store),
        Command::Get => handle_get(parsed, store),
        Command::Incr => handle_incr(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown string command")),
    }
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
    let Some(key) = parsed.first() else {
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
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'incr' command",
        ));
    };
    match store.incr(key) {
        Ok(new_value) => RespValue::Integer(new_value),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}
