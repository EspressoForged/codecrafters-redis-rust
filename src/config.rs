use clap::Parser;
use std::path::{Path, PathBuf};

/// A simple Redis-clone server implementation.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// The port number to listen on.
    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    /// The log level to use.
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// The directory where RDB files are stored.
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// The name of the RDB file.
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,
}

impl Config {
    /// Returns the socket address to listen on.
    pub fn listen_addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Returns the full path to the RDB file.
    pub fn rdb_file_path(&self) -> PathBuf {
        Path::new(&self.dir).join(&self.dbfilename)
    }
}
