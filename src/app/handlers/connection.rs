use crate::app::{
    command::{Command, ParsedCommand},
    protocol::RespValue,
};
use bytes::Bytes;

pub fn handle(parsed: ParsedCommand) -> RespValue {
    match parsed.command() {
        Command::Ping => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => parsed
            .first()
            .map(|arg| RespValue::BulkString(arg.clone()))
            .unwrap_or_else(|| {
                RespValue::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'echo' command",
                ))
            }),
        _ => RespValue::Error(Bytes::from_static(b"ERR unknown connection command")),
    }
}