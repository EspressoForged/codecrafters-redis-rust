use anyhow::Result;
use clap::Parser;
use codecrafters_redis::{
    app,
    app::{rdb, store::Store, wait::WaiterRegistry},
    config::Config,
    foundation::{net, shutdown},
    logging,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::parse());
    logging::init(config.log_level.as_str());

    info!(
        "process ID: {}. Starting server on {}",
        std::process::id(),
        config.listen_addr()
    );

    // Load RDB file to hydrate the store before starting the server.
    let store = Arc::new(Store::new());
    match rdb::load(&config) {
        Ok(rdb_store) => {
            info!(
                "RDB file loaded successfully, {} keys found.",
                rdb_store.len()
            );
            store.load_from_rdb(rdb_store);
        }
        Err(e) => {
            warn!(
                "Failed to load RDB file: {}. Starting with an empty state.",
                e
            );
        }
    }

    let waiters = Arc::new(WaiterRegistry::new());

    let listener = TcpListener::bind(config.listen_addr()).await?;

    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C signal handler");
    };

    // The connection handler now gets the config as well.
    let connection_handler = {
        let store = Arc::clone(&store);
        let waiters = Arc::clone(&waiters);
        let config = Arc::clone(&config);
        move |stream| {
            let store = Arc::clone(&store);
            let waiters = Arc::clone(&waiters);
            let config = Arc::clone(&config);
            app::handle_connection(stream, store, waiters, config)
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
