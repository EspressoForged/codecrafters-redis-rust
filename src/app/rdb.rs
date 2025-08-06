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

/// Returns the binary content of a minimal, empty RDB file.
pub fn empty_rdb_bytes() -> Vec<u8> {
    const EMPTY_RDB_HEX: &str =
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fe00ff";
    let mut bytes = Vec::new();
    for i in 0..(EMPTY_RDB_HEX.len() / 2) {
        let hex_byte = &EMPTY_RDB_HEX[i * 2..i * 2 + 2];
        let byte = u8::from_str_radix(hex_byte, 16).unwrap();
        bytes.push(byte);
    }
    let rdb_payload = format!("${}\r\n", bytes.len());
    let mut response = rdb_payload.into_bytes();
    response.extend(bytes);
    response
}

// ... (rest of the file is unchanged, full content provided)
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
            0xFF => break,
            value_type => {
                let (next_input, key) = parse_string(input)?;
                let (next_input, value) = parse_string(next_input)?;

                if value_type == 0 {
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
                    current_expiry.take();
                }

                input = next_input;
            }
        }
    }
    Ok((input, db))
}

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
        0b11 => match length_type {
            0xC0 => 1,
            0xC1 => 2,
            0xC2 => 4,
            _ => {
                warn!(
                    "Unsupported RDB special length encoding: {:02x}",
                    length_type
                );
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        },
        _ => unreachable!(),
    };
    Ok((input, (len, is_special_format)))
}

fn parse_string(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, (length, _is_special)) = parse_length_encoded(input)?;
    let (input, string_bytes) = take(length)(input)?;
    Ok((input, string_bytes))
}

fn convert_systemtime_to_instant(t: SystemTime) -> Instant {
    let now_sys = SystemTime::now();
    let now_ins = Instant::now();

    if t > now_sys {
        t.duration_since(now_sys)
            .map(|d| now_ins + d)
            .unwrap_or(now_ins)
    } else {
        now_sys
            .duration_since(t)
            .map(|d| now_ins - d)
            .unwrap_or(now_ins)
    }
}
