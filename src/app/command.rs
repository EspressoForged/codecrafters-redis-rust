use crate::app::{error::AppError, protocol::RespValue};
use bytes::Bytes;
use std::str::FromStr;
use strum_macros::{Display, EnumString};

/// Represents the set of commands the Redis server understands.
///
/// Using `strum` to derive `EnumString` allows for easy, case-insensitive
/// parsing from a string slice into a `Command` variant. `Display` allows
/// for easy conversion back to a string for logging.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Display, EnumString)]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Info,
    ReplConf,
    PSync,
    Wait,
    Config,
    Keys,
    Type,
    XAdd,
    XRange,
    XRead,
    Incr,
    Multi,
    Exec,
    Discard,
    RPush,
    LPush,
    LRange,
    LLen,
    LPop,
    BLPop,
    Subscribe,
    Publish,
    Unsubscribe,
}

/// Represents a command parsed from a RESP message, including its arguments.
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    command: Command,
    args: Vec<Bytes>,
}

impl ParsedCommand {
    /// Creates a `ParsedCommand` from a `RespValue`.
    ///
    /// The input `RespValue` must be an Array of Bulk Strings. The first
    /// element is the command name, and subsequent elements are arguments.
    pub fn from_resp(value: RespValue) -> Result<Self, AppError> {
        match value {
            RespValue::Array(values) => {
                let mut iter = values.into_iter();
                let Some(RespValue::BulkString(command_bytes)) = iter.next() else {
                    return Err(AppError::ParseError("Command must be a bulk string".into()));
                };

                let command_str = std::str::from_utf8(&command_bytes)
                    .map_err(|_| AppError::ParseError("Command contains invalid UTF-8".into()))?;

                let command = Command::from_str(command_str)
                    .map_err(|_| AppError::UnknownCommand(command_str.to_string()))?;

                let args = iter
                    .map(|val| match val {
                        RespValue::BulkString(bytes) => Ok(bytes),
                        _ => Err(AppError::ParseError("Argument must be a bulk string".into())),
                    })
                    .collect::<Result<Vec<Bytes>, _>>()?;

                Ok(ParsedCommand { command, args })
            }
            _ => Err(AppError::ParseError("Command must be a RESP array".into())),
        }
    }

    pub fn command(&self) -> Command {
        self.command
    }

    pub fn arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    /// Returns a slice of all arguments starting from a given index.
    pub fn args_from(&self, start_index: usize) -> &[Bytes] {
        if start_index >= self.args.len() {
            &[]
        } else {
            &self.args[start_index..]
        }
    }
}