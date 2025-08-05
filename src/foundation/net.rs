use crate::foundation::shutdown::Shutdown;
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

/// Runs the main server loop.
///
/// Listens for incoming TCP connections and spawns a new task for each one, 
/// passing the stream to the provided `connection_handler` function.
pub async fn run<F, Fut>(
    listener: TcpListener,
    shutdown_signal: impl Future<Output = ()> + Send + 'static,
    connection_handler: F,
) -> anyhow::Result<()>
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut shutdown = Shutdown::new(shutdown_signal);
    let handler = Arc::new(connection_handler);

    loop {
        let (stream, addr) = tokio::select! {
            res = listener.accept() => res?,
            _ = shutdown.wait_for_signal() => {
                info!("shutting down server");
                break;
            }
        };

        info!("accepted new connection from {addr}");

        let handler_clone = Arc::clone(&handler);

        // Spawn a new task to handle the connection.
        tokio::spawn(async move {
            handler_clone(stream).await;
            info!("connection with {addr} closed");
        });
    }

    Ok(())
}
