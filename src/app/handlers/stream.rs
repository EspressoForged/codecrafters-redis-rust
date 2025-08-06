use crate::app::command::Command;
use crate::app::{
    command::ParsedCommand, protocol::RespValue, store::Store, stream::StreamId,
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

fn handle_xadd(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    let (Some(key), Some(id_spec_bytes)) = (parsed.arg(0), parsed.arg(1)) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xadd' command",
        ));
    };
    let Ok(id_spec) = std::str::from_utf8(id_spec_bytes) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR invalid stream ID specified in XADD",
        ));
    };

    let fields_and_values = parsed.args_from(2);
    if fields_and_values.is_empty() || fields_and_values.len() % 2 != 0 {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xadd' command",
        ));
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
        }
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_xrange(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(start_bytes), Some(end_bytes)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xrange' command",
        ));
    };
    let Ok(start_spec) = std::str::from_utf8(start_bytes) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR invalid start ID for 'xrange' command",
        ));
    };
    let Ok(end_spec) = std::str::from_utf8(end_bytes) else {
        return RespValue::Error(Bytes::from_static(
            b"ERR invalid end ID for 'xrange' command",
        ));
    };

    match store.xrange(key, start_spec, end_spec) {
        Ok(entries) => {
            let result_array = entries
                .into_iter()
                .map(|(id, fields)| {
                    let mut field_array = Vec::with_capacity(fields.len() * 2);
                    for (field, value) in fields {
                        field_array.push(RespValue::BulkString(field));
                        field_array.push(RespValue::BulkString(value));
                    }
                    RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from(id.to_string())),
                        RespValue::Array(field_array),
                    ])
                })
                .collect();
            RespValue::Array(result_array)
        }
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

async fn handle_xread(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
) -> RespValue {
    let mut args = parsed.args_from(0);
    let mut block_timeout = None;

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"block"))
    {
        if args.len() < 3 {
            return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let Ok(timeout_ms_str) = std::str::from_utf8(&args[1]) else {
            return RespValue::Error(Bytes::from_static(
                b"ERR invalid timeout in 'xread' command",
            ));
        };
        let Ok(timeout_ms) = timeout_ms_str.parse::<u64>() else {
            return RespValue::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        };

        block_timeout = if timeout_ms == 0 {
            None // Block indefinitely
        } else {
            Some(Duration::from_millis(timeout_ms))
        };
        args = &args[2..];
    }

    if args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"streams"))
    {
        args = &args[1..];
    } else {
        return RespValue::Error(Bytes::from_static(b"ERR syntax error"));
    }

    let num_keys = args.len() / 2;
    if num_keys == 0 || args.len() % 2 != 0 {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'xread' command",
        ));
    }

    let keys = &args[..num_keys];
    let id_specs_bytes = &args[num_keys..];

    // CORRECTED: Resolve the '$' ID for each stream *before* the first read attempt.
    let resolved_ids: Vec<String> = id_specs_bytes
        .iter()
        .zip(keys.iter())
        .map(|(id_bytes, key)| {
            if &id_bytes[..] == b"$" {
                store
                    .get_stream_last_id(key)
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "0-0".to_string())
            } else {
                String::from_utf8_lossy(id_bytes).to_string()
            }
        })
        .collect();

    let keys_and_ids: Vec<(&Bytes, &str)> = keys
        .iter()
        .zip(resolved_ids.iter())
        .map(|(k, id)| (k, id.as_str()))
        .collect();

    // First attempt: non-blocking read
    if let Ok(Some(results)) = store.xread(&keys_and_ids) {
        if !results.is_empty() {
            return format_xread_response(results);
        }
    }

    // If we are here, there's no data. If not blocking, return nil.
    if block_timeout.is_none()
        && parsed
            .arg(0)
            .is_none_or(|a| !a.eq_ignore_ascii_case(b"block"))
    {
        return RespValue::NullBulkString;
    }

    // If blocking, wait for a notification.
    waiters.wait_for_any(keys, block_timeout).await;

    // Second attempt after being woken up or timing out.
    // We use the *same resolved IDs* from before the block.
    match store.xread(&keys_and_ids) {
        Ok(Some(results)) if !results.is_empty() => format_xread_response(results),
        _ => RespValue::NullBulkString, // Timed out or another client got the data
    }
}

fn format_xread_response(results: Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>) -> RespValue {
    let result_array = results
        .into_iter()
        .map(|(key, entries)| {
            let entry_array = entries
                .into_iter()
                .map(|(id, fields)| {
                    let mut field_array = Vec::with_capacity(fields.len() * 2);
                    for (field, value) in fields {
                        field_array.push(RespValue::BulkString(field));
                        field_array.push(RespValue::BulkString(value));
                    }
                    RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from(id.to_string())),
                        RespValue::Array(field_array),
                    ])
                })
                .collect();
            RespValue::Array(vec![
                RespValue::BulkString(key),
                RespValue::Array(entry_array),
            ])
        })
        .collect();
    RespValue::Array(result_array)
}
