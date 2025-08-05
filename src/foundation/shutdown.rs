use std::future::Future;
use std::pin::Pin;
// use std::task::{Context, Poll};
use tracing::info;

/// A future that completes when a shutdown signal is received.
///
/// This struct wraps a future that represents the shutdown signal.
pub struct Shutdown {
    signal: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Shutdown {
    /// Creates a new `Shutdown` instance.
    pub fn new(signal: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            signal: Box::pin(signal),
        }
    }

    /// Waits for the shutdown signal to complete.
    pub async fn wait_for_signal(&mut self) {
        self.signal.as_mut().await
    }
}

/// Creates a future that completes when the provided shutdown signal future completes.
pub async fn watch_for_signal(signal: impl Future<Output = ()> + Send + 'static) {
    signal.await;
    info!("shutdown signal received");
}