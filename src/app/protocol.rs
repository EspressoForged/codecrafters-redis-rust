use crate::app::error::AppError;
use bytes::{Buf, Bytes, BytesMut};
use nom::{
    branch::alt, bytes::complete::{tag, take, take_until}, character::complete::{crlf, digit1}, combinator::{map, map_res}, sequence::preceded, IResult
};
use tokio_util::codec::{Decoder, Encoder};

/// Represents any valid RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<RespValue>),
}

//--- Decoder (Parser) ---

/// A `tokio-util` codec for decoding RESP messages from a byte stream.
pub struct RespDecoder;

impl Decoder for RespDecoder {
    type Item = RespValue;
    type Error = AppError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        match parse_message(src) {
            Ok((remaining, value)) => {
                let consumed = src.len() - remaining.len();
                src.advance(consumed);
                Ok(Some(value))
            }
            Err(nom::Err::Incomplete(_)) => Ok(None), // Not enough data, wait for more
            Err(e) => Err(AppError::ParseError(e.to_string())),
        }
    }
}

/// Parses a full RESP message using `nom`.
fn parse_message(input: &[u8]) -> IResult<&[u8], RespValue> {
    alt((
        parse_simple_string,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_array,
    ))(input)
}

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    map(
        preceded(tag("+"), nom::sequence::terminated(take_until("\r\n"), crlf)),
        |s: &[u8]| RespValue::SimpleString(Bytes::copy_from_slice(s)),
    )(input)
}

fn parse_error(input: &[u8]) -> IResult<&[u8], RespValue> {
    map(
        preceded(tag("-"), nom::sequence::terminated(take_until("\r\n"), crlf)),
        |s: &[u8]| RespValue::Error(Bytes::copy_from_slice(s)),
    )(input)
}

fn parse_integer(input: &[u8]) -> IResult<&[u8], RespValue> {
    map(
        preceded(
            tag(":"),
            nom::sequence::terminated(map_res(digit1, |s: &[u8]| std::str::from_utf8(s).unwrap().parse::<i64>()), crlf),
        ),
        RespValue::Integer,
    )(input)
}

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, len_str) = preceded(tag("$"), nom::sequence::terminated(take_until("\r\n"), crlf))(input)?;
    let len: i64 = std::str::from_utf8(len_str).unwrap().parse().unwrap();

    if len == -1 {
        return Ok((input, RespValue::NullBulkString));
    }

    let (input, content) = nom::sequence::terminated(take(len as usize), crlf)(input)?;
    Ok((input, RespValue::BulkString(Bytes::copy_from_slice(content))))
}

fn parse_array(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, count_str) = preceded(tag("*"), nom::sequence::terminated(take_until("\r\n"), crlf))(input)?;
    let count: i64 = std::str::from_utf8(count_str).unwrap().parse().unwrap();
    
    if count == -1 {
        return Ok((input, RespValue::Array(vec![])));
    }

    let (input, items) = nom::multi::count(parse_message, count as usize)(input)?;
    Ok((input, RespValue::Array(items)))
}

//--- Encoder (Serializer) ---

impl Encoder<RespValue> for RespDecoder {
    type Error = AppError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(i.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(s) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(s.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::NullBulkString => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RespValue::Array(items) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(items.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in items {
                    self.encode(item, dst)?;
                }
            }
        }
        Ok(())
    }
}