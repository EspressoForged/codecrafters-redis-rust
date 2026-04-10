use crate::app::{
    command::{CommandHandler, ParsedCommand},
    AppContext, RespValue,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Notify;

pub mod connection;
pub mod geo;
pub mod list;
pub mod pubsub;
pub mod replication;
pub mod server;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod transaction;

pub struct ConnectionHandler;
#[async_trait]
impl CommandHandler for ConnectionHandler {
    async fn handle(
        &self,
        _ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        connection::handle(parsed)
    }
}

pub struct StringHandler;
#[async_trait]
impl CommandHandler for StringHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        string::handle(parsed, &ctx.store)
    }
}

pub struct ServerHandler;
#[async_trait]
impl CommandHandler for ServerHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        server::handle(parsed, &ctx.config, &ctx.store)
    }
}

pub struct ReplicationHandler;
#[async_trait]
impl CommandHandler for ReplicationHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        wait_notify: &Arc<Notify>,
    ) -> RespValue {
        replication::handle(parsed, &ctx.replication, wait_notify).await
    }
}

pub struct ListHandler;
#[async_trait]
impl CommandHandler for ListHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        list::handle(parsed, &ctx.store, &ctx.waiters).await
    }
}

pub struct StreamHandler;
#[async_trait]
impl CommandHandler for StreamHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        stream::handle(parsed, &ctx.store, &ctx.waiters).await
    }
}

pub struct SortedSetHandler;
#[async_trait]
impl CommandHandler for SortedSetHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        sorted_set::handle(parsed, &ctx.store)
    }
}

pub struct GeoHandler;
#[async_trait]
impl CommandHandler for GeoHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        geo::handle(parsed, &ctx.store)
    }
}

pub struct PubSubHandler;
#[async_trait]
impl CommandHandler for PubSubHandler {
    async fn handle(
        &self,
        ctx: &Arc<AppContext>,
        parsed: ParsedCommand,
        _wait_notify: &Arc<Notify>,
    ) -> RespValue {
        pubsub::publish(parsed, &ctx.pubsub)
    }
}
