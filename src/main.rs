use anyhow::Result;
use clap::Parser;
use codecrafters_redis::{
    app,
    app::{store::Store, wait::WaiterRegistry}, // <-- Import WaiterRegistry
    config::Config,
    foundation::{net, shutdown},
    logging,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    logging::init(config.log_level.as_str());

    info!(
        "process ID: {}. Starting server on {}",
        std::process::id(),
        config.listen_addr()
    );

    let store = Arc::new(Store::new());
    let waiters = Arc::new(WaiterRegistry::new()); // <-- Create the waiter registry

    let listener = TcpListener::bind(config.listen_addr()).await?;

    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C signal handler");
    };

    // The connection handler now gets clones of both the store and the waiter registry.
    let connection_handler = {
        let store = Arc::clone(&store);
        let waiters = Arc::clone(&waiters);
        move |stream| {
            let store = Arc::clone(&store);
            let waiters = Arc::clone(&waiters);
            app::handle_connection(stream, store, waiters)
        }
    };

    let server = net::run(
        listener,
        shutdown::watch_for_signal(shutdown_signal),
        connection_handler,
    );

    info!("server running");

    if let Err(e) = server.await {
        error!(error = %e, "server failed");
    }

    info!("server has shut down");

    Ok(())
}