use crate::app::store::{DataType, StoreValue};
use crate::config::Config;
use anyhow::Result;
use bytes::Bytes;
use nom::{
    bytes::complete::take,
    number::complete::{le_u32, le_u64, u8},
    IResult,
};
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::debug;

/// Loads the RDB file specified in the config and returns its contents.
pub fn load(config: &Config) -> Result<HashMap<Bytes, StoreValue>> {
    let path = config.rdb_file_path();
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let file_content = fs::read(path)?;
    let (remaining, _header) = parse_header(&file_content).map_err(|e| e.to_owned())?;
    let (remaining, db) = parse_database(remaining).map_err(|e| e.to_owned())?;

    debug!("RDB file parsed. Remaining bytes: {}", remaining.len());

    Ok(db)
}

fn parse_header(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = nom::bytes::complete::tag(b"REDIS")(input)?;
    let (input, _version) = nom::bytes::complete::take(4usize)(input)?;
    Ok((input, ()))
}

fn parse_database(mut input: &[u8]) -> IResult<&[u8], HashMap<Bytes, StoreValue>> {
    let mut db = HashMap::new();
    let mut current_expiry = None;

    loop {
        let (next_input, opcode) = u8(input)?;
        input = next_input;

        match opcode {
            0xFA => {
                // Auxiliary field
                let (next_input, _key) = parse_string(input)?;
                let (next_input, _value) = parse_string(next_input)?;
                input = next_input;
            }
            0xFB => {
                // Resizedb
                let (next_input, _) = parse_length(input)?;
                let (next_input, _) = parse_length(next_input)?;
                input = next_input;
            }
            0xFE => {
                // Database selector
                let (next_input, _) = parse_length(input)?;
                input = next_input;
            }
            0xFD => {
                // Expiry time in seconds
                let (next_input, timestamp_s) = le_u32(input)?;
                let expiry_time = UNIX_EPOCH + Duration::from_secs(timestamp_s as u64);
                current_expiry = Some(convert_systemtime_to_instant(expiry_time));
                input = next_input;
            }
            0xFC => {
                // Expiry time in milliseconds
                let (next_input, timestamp_ms) = le_u64(input)?;
                let expiry_time = UNIX_EPOCH + Duration::from_millis(timestamp_ms);
                current_expiry = Some(convert_systemtime_to_instant(expiry_time));
                input = next_input;
            }
            0xFF => break, // End of File
            0x00 => {
                // Value type: String
                let (next_input, key) = parse_string(input)?;
                let (next_input, value) = parse_string(next_input)?;

                let store_value = StoreValue {
                    data: DataType::String(Bytes::copy_from_slice(value)),
                    expires_at: current_expiry.take(),
                };

                db.insert(Bytes::copy_from_slice(key), store_value);
                input = next_input;
            }
            // We only support string value types for this challenge.
            value_type => {
                debug!("Unsupported value type in RDB: {value_type}. Skipping key-value pair.");
                let (next_input, _key) = parse_string(input)?;
                let (next_input, _value) = parse_string(next_input)?;
                current_expiry.take(); // Discard expiry for the skipped key
                input = next_input;
            }
        }
    }
    Ok((input, db))
}

fn parse_length(input: &[u8]) -> IResult<&[u8], u32> {
    let (input, length_type) = u8(input)?;
    match length_type >> 6 {
        0b00 => Ok((input, length_type as u32)),
        0b01 => {
            let (input, next_byte) = u8(input)?;
            let length = ((length_type & 0x3F) as u32) << 8 | next_byte as u32;
            Ok((input, length))
        }
        0b10 => {
            let (input, bytes) = take(4usize)(input)?;
            let length = u32::from_be_bytes(bytes.try_into().unwrap());
            Ok((input, length))
        }
        _ => {
            // This is for special string encodings (like integers), which we don't support.
            // For now, we'll treat it as an error/unsupported.
            panic!("Unsupported length encoding: {length_type:08b}");
        }
    }
}

fn parse_string(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, length) = parse_length(input)?;
    let (input, string_bytes) = take(length as usize)(input)?;
    Ok((input, string_bytes))
}

// Helper function to safely convert a wall-clock SystemTime to a monotonic Instant.
fn convert_systemtime_to_instant(t: SystemTime) -> Instant {
    let now_sys = SystemTime::now();
    let now_ins = Instant::now();

    if t > now_sys {
        // Time is in the future
        let dur = t.duration_since(now_sys).unwrap();
        now_ins + dur
    } else {
        // Time is in the past
        let dur = now_sys.duration_since(t).unwrap();
        now_ins - dur
    }
}
