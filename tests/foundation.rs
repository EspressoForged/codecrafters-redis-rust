use codecrafters_redis::foundation::{net, shutdown};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time;

#[tokio::test]
async fn test_server_starts_and_accepts_connection() {
    // Arrange: Bind a listener to a random available port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Arrange: Create a shutdown future that will never complete for this test.
    let shutdown_future = async {
        time::sleep(Duration::from_secs(10)).await;
    };

    // Arrange: Define a no-op connection handler for this foundation-level test.
    // It accepts the stream and immediately completes, doing no work.
    let no_op_handler = |_stream: TcpStream| async {};

    // Act: Run the server in a background task, providing the no-op handler.
    // This now correctly calls the three-argument version of `net::run`.
    tokio::spawn(net::run(
        listener,
        shutdown::watch_for_signal(shutdown_future),
        no_op_handler,
    ));

    // Give the server a moment to start up.
    time::sleep(Duration::from_millis(100)).await;

    // Act & Assert: Attempt to connect to the server.
    match TcpStream::connect(addr).await {
        Ok(_) => {
            // Success! The server accepted our connection.
        }
        Err(e) => {
            panic!("failed to connect to server: {e}");
        }
    }
}