use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// The port number to listen on.
    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    /// The log level to use.
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

impl Config {
    /// Returns the socket address to listen on.
    pub fn listen_addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}
