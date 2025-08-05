use tracing_subscriber::{EnvFilter, FmtSubscriber};

/// Initializes the logging system.
///
/// This function sets up a global logger using `tracing_subscriber`.
/// The log level is determined by the `RUST_LOG` environment variable,
/// or defaults to the provided level if `RUST_LOG` is not set.
pub fn init(default_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set global default subscriber");
}
