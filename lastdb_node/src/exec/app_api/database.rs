//! Which store a `/v1` item names.
//!
//! An owned database and the personal database behave as before. A received
//! database is readable. A write to one is refused. `changes` does not open it.

use std::future::Future;
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;

use super::bad;
use crate::exec::reply::HostError;
use crate::host::owned::ResolvedStore;
use crate::host::Host;
use crate::lastdb_uds::uds_router::valid_selector;
use crate::registered_share::{self, RegisteredShare};
use crate::share_cache::ShareCacheError;

const NOT_REGISTERED: &str = "database is not registered on this node";
const READ_ONLY: &str = "a received database is read only";
const CHANGES_UNSERVED: &str = "a received database is not served by /v1/x/changes";
const NOT_DURABLE: &str = "database snapshot is not durable";
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
    dispatch(selector, host, OnReceived::Read, future).await
}

pub(super) async fn write_in<T>(
    selector: Option<&str>,
    host: &Host,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    dispatch(selector, host, OnReceived::RefuseWrite, future).await
}

pub(super) async fn changes_in<T>(
    selector: Option<&str>,
    host: &Host,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    dispatch(selector, host, OnReceived::RefuseChanges, future).await
}

async fn dispatch<T>(
    selector: Option<&str>,
    host: &Host,
    received: OnReceived,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    match place(selector, host)? {
        Place::Personal => future.await,
        Place::Owned(store) => host.with_records(store, future).await,
        Place::Received(share) => match received {
            OnReceived::Read => read_received(host, &share, future).await,
            OnReceived::RefuseWrite => Err(HostError::new(403, READ_ONLY)),
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

async fn read_received<T>(
    host: &Host,
    share: &RegisteredShare,
    future: impl Future<Output = Result<T, HostError>>,
) -> Result<T, HostError> {
    if !durable(host, share) {
        return Err(HostError::new(503, NOT_DURABLE));
    }
    let store = registered_share::receiver_store(host, &share.id)
        .await
        .map_err(|_| HostError::new(503, STORE_UNAVAILABLE))?;
    let result = host.with_records(store, future).await;
    if !durable(host, share) {
        return Err(HostError::new(503, NOT_DURABLE));
    }
    result
}

fn durable(host: &Host, share: &RegisteredShare) -> bool {
    crate::database_replay::prefix_durable(&host.home, share.id.as_str())
}

const SESSION_DATABASE: &str = "a database session addresses only its own database";

/// A database session addresses one database.
///
/// An item that omits `database` addresses that database: this fills the field
/// with the session id. An item that names the session database, by that id or
/// by a name that resolves to it, keeps the caller's string. An item that names
/// a different database is `permission_denied`. An unknown or invalid selector
/// is left as written, so the existing check answers it.
pub(super) fn bind_item(
    host: &Host,
    session: Option<&str>,
    database: &mut Option<String>,
) -> Result<(), HostError> {
    let Some(session) = session else {
        return Ok(());
    };
    let Some(selector) = database.clone() else {
        *database = Some(session.to_owned());
        return Ok(());
    };
    if selector == session {
        return Ok(());
    }
    match known_database(host, &selector)? {
        Some(id) if id != session => Err(HostError::new(403, SESSION_DATABASE)),
        _ => Ok(()),
    }
}

/// The id of a database this node can name. `None` leaves the existing check
/// in place. An owned store whose id cannot be named is refused: opening it
/// would skip the session boundary.
fn known_database(host: &Host, selector: &str) -> Result<Option<String>, HostError> {
    if !valid_selector(selector) {
        return Ok(None);
    }
    let resolved = match host.owned.resolve(selector) {
        Ok(resolved) => resolved,
        Err(_) => return Ok(None),
    };
    match resolved {
        ResolvedStore::Owner => Ok(host.owned.owner().ok().map(|entry| entry.id)),
        ResolvedStore::Owned(_) => owned_id(host, selector),
        ResolvedStore::Absent => Ok(received_id(host, selector)),
    }
}

fn owned_id(host: &Host, selector: &str) -> Result<Option<String>, HostError> {
    let entries = host
        .owned
        .owned_entries()
        .map_err(|_| HostError::new(503, REGISTRY_UNAVAILABLE))?;
    match entries
        .into_iter()
        .find(|entry| entry.id == selector || entry.name.as_deref() == Some(selector))
    {
        Some(entry) => Ok(Some(entry.id)),
        None => Err(HostError::new(503, REGISTRY_UNAVAILABLE)),
    }
}

fn received_id(host: &Host, selector: &str) -> Option<String> {
    match registered_share::resolve(&host.home, selector) {
        Ok(Some(share)) if share.signed_log => Some(share.id.as_str().to_owned()),
        _ => None,
    }
}
