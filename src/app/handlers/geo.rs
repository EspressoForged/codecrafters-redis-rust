use crate::app::{
    command::{Command, ParsedCommand},
    // geo,
    protocol::RespValue,
    store::Store,
};
use bytes::Bytes;
use std::sync::Arc;

pub fn handle(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    match parsed.command() {
        Command::GeoAdd => handle_geoadd(parsed, store),
        Command::GeoPos => handle_geopos(parsed, store),
        Command::GeoDist => handle_geodist(parsed, store),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown geo command")),
    }
}

fn handle_geoadd(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(lon_bytes), Some(lat_bytes), Some(member)) =
        (parsed.arg(0), parsed.arg(1), parsed.arg(2), parsed.arg(3))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geoadd' command",
        ));
    };

    let longitude = match std::str::from_utf8(lon_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    let latitude = match std::str::from_utf8(lat_bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(f) => f,
        None => return RespValue::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };

    match store.geoadd(key.clone(), longitude, latitude, member.clone()) {
        Ok(count) => RespValue::Integer(count as i64),
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_geopos(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let Some(key) = parsed.first() else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geopos' command",
        ));
    };
    let members = parsed.args_from(1);

    match store.geopos(key, members) {
        Ok(positions) => {
            let result_array = positions
                .into_iter()
                .map(|opt_coords| match opt_coords {
                    Some(coords) => RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from(coords.longitude.to_string())),
                        RespValue::BulkString(Bytes::from(coords.latitude.to_string())),
                    ]),
                    None => RespValue::NullArray,
                })
                .collect();
            RespValue::Array(result_array)
        }
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}

fn handle_geodist(parsed: ParsedCommand, store: &Arc<Store>) -> RespValue {
    let (Some(key), Some(member1), Some(member2)) = (parsed.arg(0), parsed.arg(1), parsed.arg(2))
    else {
        return RespValue::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'geodist' command",
        ));
    };

    match store.geodist(key, member1, member2) {
        Ok(Some(distance)) => RespValue::BulkString(Bytes::from(format!("{:.4}", distance))),
        Ok(None) => RespValue::NullBulkString, // One or both members don't exist
        Err(e) => RespValue::Error(Bytes::from(e.to_string())),
    }
}
