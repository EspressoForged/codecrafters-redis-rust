use thiserror::Error;

/// Represents errors that can occur within the application logic.
#[derive(Error, Debug)]
pub enum AppError {
    /// Used when a client connection provides an incomplete command.
    #[error("incomplete command")]
    Incomplete,

    /// An error encountered during protocol parsing.
    #[error("protocol parse error: {0}")]
    ParseError(String),

    /// An unknown command was received.
    #[error("unknown command: {0}")]
    UnknownCommand(String),

    /// An I/O error from the underlying transport.
    /// The `#[from]` attribute automatically implements `From<std::io::Error>` for us.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Operation against a key holding the wrong kind of value.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}