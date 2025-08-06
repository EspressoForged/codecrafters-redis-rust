use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand,
    protocol::RespValue,
    stream::StreamId,
    store::Store,
    wait::WaiterRegistry,
};
use bytes::Bytes;
// use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub async fn handle(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    match parsed.command() {
        Command::XAdd => handle_xadd(parsed, store, waiters),
        Command::XRange => handle_xrange(parsed, store),
        Command::XRead => handle_xread(parsed, store, waiters).await,
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown stream command")),
    }
}

fn handle_xadd(parsed: ParsedCommand, store: &Arc<Store>, waiters: &Arc<WaiterRegistry>) -> RespValue {
    let (Some(key), Some(id_spec_bytes)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'xadd' command"));
    };
    let Ok(id_spec) = std::str::from_utf8(id_spec_bytes) else {
        return RespValue::Error(Bytes::from_static(b"ERR invalid stream ID specified in XADD"));
    };
    
    let fields_and_values = parsed.args_from(2);
    if fields_and_values.is_empty() || fields_and_values.len() % 2 != 0 {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'xadd' command"));
    }
    
    let fields: Vec<(Bytes, Bytes)> = fields_and_values
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    match store.xadd(key.clone(), id_spec, fields) {
        Ok(id) => {
            // After adding, notify any waiting XREAD clients.
            waiters.notify_one(key);
            RespValue::BulkString(Bytes::from(id.to_string()))
        },
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_xrange(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(start_bytes), Some(end_bytes)) = (parsed.arg(0), parsed.arg(1), parsed.arg(2)) else {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'xrange' command"));
    };
    let Ok(start_spec) = std::str::from_utf8(start_bytes) else {
        return RespValue::Error(Bytes::from_static(b"ERR invalid start ID for 'xrange' command"));
    };
    let Ok(end_spec) = std::str::from_utf8(end_bytes) else {
        return RespValue::Error(Bytes::from_static(b"ERR invalid end ID for 'xrange' command"));
    };

    match store.xrange(key, start_spec, end_spec) {
        Ok(entries) => {
            let result_array = entries.into_iter().map(|(id, fields)| {
                let mut field_array = Vec::with_capacity(fields.len() * 2);
                for (field, value) in fields {
                    field_array.push(RespValue::BulkString(field));
                    field_array.push(RespValue::BulkString(value));
                }
                RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from(id.to_string())),
                    RespValue::Array(field_array),
                ])
            }).collect();
            RespValue::Array(result_array)
        },
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

async fn handle_xread(parsed: ParsedCommand, store: &Arc<Store>, waiters: &Arc<WaiterRegistry>) -> RespValue {
    let mut args = parsed.args_from(0);
    let mut block_timeout = None;

    if args.first().map_or(false, |a| a.eq_ignore_ascii_case(b"block")) {
        if args.len() < 3 {
             return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let Ok(timeout_ms_str) = std::str::from_utf8(&args[1]) else {
             return RespValue::Error(Bytes::from_static(b"ERR invalid timeout in 'xread' command"));
        };
        let Ok(timeout_ms) = timeout_ms_str.parse::<u64>() else {
             return RespValue::Error(Bytes::from_static(b"ERR value is not an integer or out of range"));
        };

        block_timeout = if timeout_ms == 0 {
            None // Block indefinitely
        } else {
            Some(Duration::from_millis(timeout_ms))
        };
        args = &args[2..]; // Consume 'block' and the timeout
    }

    if args.first().map_or(false, |a| a.eq_ignore_ascii_case(b"streams")) {
        args = &args[1..]; // Consume 'streams'
    } else {
        return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
    }

    let num_keys = args.len() / 2;
    if num_keys == 0 {
        return RespValue::Error(Bytes::from_static(b"ERR wrong number of arguments for 'xread' command"));
    }
    
    let keys = &args[..num_keys];
    let id_specs_bytes = &args[num_keys..];

    let mut keys_and_ids = Vec::with_capacity(num_keys);
    for i in 0..num_keys {
        let Ok(id_spec) = std::str::from_utf8(&id_specs_bytes[i]) else {
            return RespValue::Error(Bytes::from_static(b"ERR invalid stream ID specified for 'xread' command"));
        };
        keys_and_ids.push((&keys[i], id_spec));
    }

    // First attempt: non-blocking read
    match store.xread(&keys_and_ids) {
        Ok(Some(results)) => return format_xread_response(results),
        Ok(None) => {
            if block_timeout.is_none() && parsed.arg(0).map_or(true, |a| !a.eq_ignore_ascii_case(b"block")) {
                // Not a blocking command, return empty array.
                return RespValue::NullBulkString;
            }
        },
        Err(e) => return RespValue::Error(Bytes::from(e.to_string())),
    }

    // If we are here, it means it's a blocking command and there's no data yet.
    waiters.wait_for_any(keys, block_timeout).await;

    // Second attempt after being woken up or timing out.
    match store.xread(&keys_and_ids) {
        Ok(Some(results)) => format_xread_response(results),
        Ok(None) => RespValue::NullBulkString, // Timed out or another client got the data
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn format_xread_response(results: Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>) -> RespValue {
     let result_array = results.into_iter().map(|(key, entries)| {
        let entry_array = entries.into_iter().map(|(id, fields)| {
            let mut field_array = Vec::with_capacity(fields.len() * 2);
            for (field, value) in fields {
                field_array.push(RespValue::BulkString(field));
                field_array.push(RespValue::BulkString(value));
            }
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from(id.to_string())),
                RespValue::Array(field_array),
            ])
        }).collect();
        RespValue::Array(vec![
            RespValue::BulkString(key),
            RespValue::Array(entry_array),
        ])
    }).collect();
    RespValue::Array(result_array)
}