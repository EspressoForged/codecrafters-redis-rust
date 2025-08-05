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
use tracing::{debug, warn};

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
        if input.is_empty() {
            // This can happen if the file ends without a 0xFF opcode, which is technically valid.
            break;
        }

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
                let (next_input, _) = parse_length_encoded(input)?;
                let (next_input, _) = parse_length_encoded(next_input)?;
                input = next_input;
            }
            0xFE => {
                // Database selector
                let (next_input, _) = parse_length_encoded(input)?;
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
            value_type => {
                // Key-Value pair
                // The value_type indicates how the value is encoded (string, list, etc.)
                // For this challenge, we only care about string encodings (0).
                let (next_input, key) = parse_string(input)?;
                let (next_input, value) = parse_string(next_input)?;

                if value_type == 0 {
                    // Type 0 is string encoding
                    let store_value = StoreValue {
                        data: DataType::String(Bytes::copy_from_slice(value)),
                        expires_at: current_expiry.take(),
                    };
                    db.insert(Bytes::copy_from_slice(key), store_value);
                } else {
                    warn!(
                        "Unsupported RDB value type: {}. Skipping key '{:?}'.",
                        value_type,
                        String::from_utf8_lossy(key)
                    );
                    current_expiry.take(); // Discard expiry for unsupported type
                }

                input = next_input;
            }
        }
    }
    Ok((input, db))
}

/// Parses a length-encoded field from the RDB file.
/// This can be a simple length, or a special format indicator.
fn parse_length_encoded(input: &[u8]) -> IResult<&[u8], (usize, bool)> {
    let (input, length_type) = u8(input)?;
    let is_special_format = (length_type & 0xC0) == 0xC0;

    let len = match length_type >> 6 {
        0b00 => (length_type & 0x3F) as usize,
        0b01 => {
            let (input, next_byte) = u8(input)?;
            return Ok((
                input,
                (
                    (((length_type & 0x3F) as usize) << 8) | next_byte as usize,
                    false,
                ),
            ));
        }
        0b10 => {
            let (input, bytes) = take(4usize)(input)?;
            return Ok((
                input,
                (
                    u32::from_be_bytes(bytes.try_into().unwrap()) as usize,
                    false,
                ),
            ));
        }
        0b11 => {
            // Special format
            match length_type {
                0xC0 => 1, // int8
                0xC1 => 2, // int16
                0xC2 => 4, // int32
                _ => {
                    warn!(
                        "Unsupported RDB special length encoding: {:02x}",
                        length_type
                    );
                    // We can't recover from this, so we'll treat it as an error.
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Tag,
                    )));
                }
            }
        }
        _ => unreachable!(), // The `>> 6` can only produce 0, 1, 2, 3
    };
    Ok((input, (len, is_special_format)))
}

/// Parses a string, which can be length-prefixed or a specially encoded integer.
fn parse_string(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, (length, _is_special)) = parse_length_encoded(input)?;
    let (input, string_bytes) = take(length)(input)?;
    Ok((input, string_bytes))
}

fn convert_systemtime_to_instant(t: SystemTime) -> Instant {
    let now_sys = SystemTime::now();
    let now_ins = Instant::now();

    if t > now_sys {
        // Time is in the future
        t.duration_since(now_sys)
            .map(|d| now_ins + d)
            .unwrap_or(now_ins)
    } else {
        // Time is in the past
        now_sys
            .duration_since(t)
            .map(|d| now_ins - d)
            .unwrap_or(now_ins)
    }
}
