//! Which store a `/v1` item names.
//!
//! An owned database and the personal database behave as before. A received
//! database is readable once its signed history has materialized into the
//! receiver store. A write commits only when this node holds a folded Write
//! role; after the local commit it authors a signed direct entry on this
//! member's own stream and appends to the received database's signed log. Otherwise
//! the write is refused. `changes` does not open a received database.

use std::future::Future;
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;

use super::bad;
use crate::exec::reply::HostError;
use crate::host::owned::ResolvedStore;
use crate::host::Host;
use crate::lastdb_uds::uds_router::valid_selector;
use crate::log_sync::OwnedMutation;
use crate::registered_share::{self, RegisteredShare};
use crate::share_cache::ShareCacheError;

const NOT_REGISTERED: &str = "database is not registered on this node";
const READ_ONLY: &str = "a received database is read only";
const CHANGES_UNSERVED: &str = "a received database is not served by /v1/x/changes";
const STORE_UNAVAILABLE: &str = "database store is unavailable";
const REGISTRY_UNAVAILABLE: &str = "database registry is unavailable";
enum Place {
    Personal,
    Owned(Arc<FoldDB>),
    Received(RegisteredShare),
}

enum OnReceived {
    Read,
    RefuseWrite,
    RefuseChanges,
}

pub(super) async fn read_in<T>(
    selector: Option<&str>,
    host: &Host,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    dispatch(selector, host, OnReceived::Read, &[], future).await
}

pub(super) async fn write_in<T>(
    selector: Option<&str>,
    host: &Host,
    mutations: &[OwnedMutation],
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    dispatch(selector, host, OnReceived::RefuseWrite, mutations, future).await
}

pub(super) async fn changes_in<T>(
    selector: Option<&str>,
    host: &Host,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    dispatch(selector, host, OnReceived::RefuseChanges, &[], future).await
}

async fn dispatch<T>(
    selector: Option<&str>,
    host: &Host,
    received: OnReceived,
    mutations: &[OwnedMutation],
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    match place(selector, host)? {
        Place::Personal => future.await,
        Place::Owned(store) => host.with_records(store, future).await,
        Place::Received(share) => match received {
            OnReceived::Read => read_received(host, &share, future).await,
            OnReceived::RefuseWrite => write_received(host, &share, mutations, future).await,
            OnReceived::RefuseChanges => Err(HostError::new(404, CHANGES_UNSERVED)),
        },
    }
}

fn place(selector: Option<&str>, host: &Host) -> Result<Place, HostError> {
    let Some(selector) = selector else {
        return Ok(Place::Personal);
    };
    if !valid_selector(selector) {
        return Err(bad("invalid database identifier"));
    }
    match host.owned.resolve(selector)? {
        ResolvedStore::Owner => Ok(Place::Personal),
        ResolvedStore::Owned(store) => Ok(Place::Owned(store)),
        ResolvedStore::Absent => received(host, selector),
    }
}

fn received(host: &Host, selector: &str) -> Result<Place, HostError> {
    match registered_share::resolve(&host.home, selector) {
        Ok(Some(share)) if share.signed_log => Ok(Place::Received(share)),
        Ok(_) => Err(HostError::new(404, NOT_REGISTERED)),
        Err(ShareCacheError::Invalid("database name")) => Err(bad("invalid database identifier")),
        Err(_) => Err(HostError::new(503, REGISTRY_UNAVAILABLE)),
    }
}

async fn write_received<T>(
    host: &Host,
    share: &RegisteredShare,
    mutations: &[OwnedMutation],
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    if !crate::log_sync::direct_write_allowed(host, share) {
        return Err(HostError::new(403, READ_ONLY));
    }
    let store = registered_share::receiver_store(host, &share.id)
        .await
        .map_err(|_| HostError::new(503, STORE_UNAVAILABLE))?;
    let result = host.with_records(Arc::clone(&store), future).await;
    let Ok(body) = result else {
        // The write may have moved the log. Record the count the list reads.
        crate::log_sync::note_receiver(&host.home, share.id.as_str());
        return result;
    };
    crate::log_sync::mirror_received_write(host, share, mutations).map_err(|error| {
        let reply = error.reply();
        tracing::warn!(
            target: "lastdbd::log_sync",
            status = reply.status,
            message = reply.message,
            "received write was not mirrored to the member stream"
        );
        HostError::new(reply.status, reply.message)
    })?;
    // Mirror enqueued the member stream entry; refresh publish.pending.
    crate::log_sync::note_receiver(&host.home, share.id.as_str());
    Ok(body)
}

async fn read_received<T>(
    host: &Host,
    share: &RegisteredShare,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    let store = registered_share::receiver_store(host, &share.id)
        .await
        .map_err(|_| HostError::new(503, STORE_UNAVAILABLE))?;
    crate::log_sync::note_receiver(&host.home, share.id.as_str());
    host.with_records(store, future).await
}
