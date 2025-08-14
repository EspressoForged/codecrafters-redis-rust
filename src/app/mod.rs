use crate::app::{
    command::{Command, ParsedCommand},
    protocol::{RespDecoder, RespValue},
    pubsub::PubSubHub,
    replication::ReplicationState,
    store::Store,
    wait::WaiterRegistry,
};
use crate::config::Config;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};
use tokio_util::codec::Framed;
use tracing::{error, info};

pub mod command;
pub mod error;
pub mod handlers;
pub mod protocol;
pub mod pubsub;
pub mod rdb;
pub mod replication;
pub mod sorted_set; // <-- NEW
pub mod store;
pub mod stream;
pub mod wait;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub enum ConnectionState {
    Normal,
    InTransaction,
    Subscribed {
        client_id: u64,
        subscriptions: HashSet<Bytes>,
        tx: mpsc::Sender<RespValue>,
        rx: mpsc::Receiver<RespValue>,
    },
}

pub async fn handle_connection(
    stream: TcpStream,
    store: Arc<Store>,
    waiters: Arc<WaiterRegistry>,
    config: Arc<Config>,
    replication: Arc<ReplicationState>,
    pubsub: Arc<PubSubHub>,
) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut framed = Framed::new(stream, RespDecoder);
    let mut state = ConnectionState::Normal;
    let mut command_queue: Vec<ParsedCommand> = Vec::new();
    let wait_notify = Arc::new(Notify::new());

    loop {
        if let ConnectionState::Subscribed {
            client_id,
            ref mut subscriptions,
            ref tx,
            ref mut rx,
        } = state
        {
            if handlers::pubsub::subscribed_loop(
                &mut framed,
                subscriptions,
                tx,
                rx,
                &pubsub,
                client_id,
            )
            .await
            .is_err()
            {
                info!("Client {client_id} disconnected from subscribed mode.");
                pubsub.unsubscribe_all(client_id, subscriptions);
                return;
            }
        }

        match framed.next().await {
            Some(Ok(value)) => {
                let parsed_command = match ParsedCommand::from_resp(value) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        if let Err(e) = framed
                            .send(RespValue::Error(Bytes::from(e.to_string())))
                            .await
                        {
                            error!("failed to send error response: {e}");
                        }
                        continue;
                    }
                };

                if parsed_command.command() == Command::Subscribe {
                    let (tx, rx) = mpsc::channel(128);
                    let (response, new_subs) = handlers::pubsub::subscribe(
                        parsed_command,
                        &pubsub,
                        client_id,
                        tx.clone(),
                        &HashSet::new(),
                    );
                    if let Err(e) = framed.send(response).await {
                        error!("failed to send subscribe response: {e}");
                        return;
                    }
                    state = ConnectionState::Subscribed {
                        client_id,
                        subscriptions: new_subs,
                        tx,
                        rx,
                    };
                    continue;
                }

                if parsed_command.command() == Command::PSync {
                    info!("Replica requested PSYNC, starting full resync.");
                    let response = RespValue::SimpleString(Bytes::from(format!(
                        "FULLRESYNC {} 0",
                        replication.master_replid()
                    )));
                    if let Err(e) = framed.send(response).await {
                        error!("Failed to send FULLRESYNC response: {e}");
                        return;
                    }
                    let rdb_bytes = rdb::empty_rdb_bytes();
                    if let Err(e) = framed.get_mut().write_all(&rdb_bytes).await {
                        error!("Failed to send RDB file: {e}");
                        return;
                    }

                    let (tx, rx) = mpsc::channel(128);
                    let ack_offset = replication.add_replica(tx).await;
                    tokio::spawn(replication::serve_replica(
                        rx,
                        framed,
                        ack_offset,
                        Arc::clone(&wait_notify),
                    ));
                    info!("Connection transitioned to replica serving mode, breaking main loop.");
                    return;
                }

                let response = handle_command(
                    parsed_command,
                    &store,
                    &waiters,
                    &config,
                    &replication,
                    &pubsub,
                    &mut state,
                    &mut command_queue,
                    &wait_notify,
                )
                .await;

                if let Err(e) = framed.send(response).await {
                    error!("failed to send response: {e}");
                    return;
                }
            }
            Some(Err(e)) => {
                error!("error reading from stream: {e}");
                return;
            }
            None => return,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_command(
    parsed: ParsedCommand,
    store: &Arc<Store>,
    waiters: &Arc<WaiterRegistry>,
    config: &Arc<Config>,
    replication: &Arc<ReplicationState>,
    pubsub: &Arc<PubSubHub>,
    state: &mut ConnectionState,
    queue: &mut Vec<ParsedCommand>,
    wait_notify: &Arc<Notify>,
) -> RespValue {
    if let Some(response) = handlers::transaction::handle_state(
        parsed.clone(),
        state,
        queue,
        store,
        waiters,
        config,
        replication,
        pubsub,
        wait_notify,
    )
    .await
    {
        return response;
    }

    if matches!(state, ConnectionState::InTransaction) {
        queue.push(parsed);
        return RespValue::SimpleString(Bytes::from_static(b"QUEUED"));
    }

    let command = parsed.command();

    let is_write_command = matches!(
        command,
        Command::Set
            | Command::LPush
            | Command::RPush
            | Command::LPop
            | Command::Incr
            | Command::XAdd
            | Command::ZAdd
            | Command::ZRem
    );
    if is_write_command {
        if replication.role() == replication::Role::Replica {
            return RespValue::Error(Bytes::from_static(
                b"READONLY You can't write against a read only replica.",
            ));
        }
        Arc::clone(replication).propagate(parsed.clone());
    }

    match command {
        Command::Ping | Command::Echo => handlers::connection::handle(parsed),
        Command::Set | Command::Get | Command::Incr => handlers::string::handle(parsed, store),
        Command::Config | Command::Keys | Command::Type => {
            handlers::server::handle(parsed, config, store)
        }
        Command::Info | Command::ReplConf | Command::Wait => {
            handlers::replication::handle(parsed, replication, wait_notify).await
        }
        Command::Publish => handlers::pubsub::publish(parsed, pubsub),
        Command::LPush
        | Command::RPush
        | Command::LPop
        | Command::LLen
        | Command::LRange
        | Command::BLPop => handlers::list::handle(parsed, store, waiters).await,
        Command::XAdd | Command::XRange | Command::XRead => {
            handlers::stream::handle(parsed, store, waiters).await
        }
        // NEW: Route all Z* commands to the new handler.
        Command::ZAdd
        | Command::ZCard
        | Command::ZScore
        | Command::ZRank
        | Command::ZRange
        | Command::ZRem => handlers::sorted_set::handle(parsed, store),
        _ => RespValue::Error(Bytes::from(format!(
            "ERR unknown command '{}'",
            parsed.command()
        ))),
    }
}
