//! Reads of the folded signed chain: grants, publisher heads, and relay URLs.
//!
//! Nothing here asks a side channel. Every answer is a view of the
//! `VerifiedHistory` this node holds for the database.

use mutation_log::{DatabaseId, NodeKey, VerifiedHistory};
use serde_json::{json, Value};

use super::chain_error::{require_enrolled, ChainError};
use crate::host::owned::{self, Entry};
use crate::host::Host;

/// The owned shareable database a selector names.
pub(super) fn owned_database(host: &Host, selector: &str) -> Result<Entry, ChainError> {
    owned::find_shareable_database(&host.home, selector).map_err(|message| match message.as_str() {
        "database registry is unavailable" => ChainError::Unconfirmed,
        "invalid database selector" => ChainError::Invalid("invalid database identifier"),
        "database is not registered on this node" => {
            ChainError::NotFound("database is not registered on this node")
        }
        _ => ChainError::Forbidden("database is not shareable"),
    })
}

/// Run `view` on the history of an owned database, or a received one held in memory.
fn with_history<R>(
    host: &Host,
    selector: &str,
    view: impl FnOnce(&str, &VerifiedHistory) -> Result<R, ChainError>,
) -> Result<R, ChainError> {
    match owned_database(host, selector) {
        Ok(entry) => {
            require_enrolled(host, &entry.id)?;
            host.histories
                .with_mut(&entry.id, |history| view(&entry.id, history))
                .map_err(|_| ChainError::Unconfirmed)?
        }
        Err(ChainError::NotFound(_)) => {
            let id = received_id(host, selector)?;
            require_enrolled(host, &id)?;
            host.histories
                .peek_with(&id, |history| view(&id, history))
                .unwrap_or(Err(ChainError::NotFound(
                    "no signed history is held for this database",
                )))
        }
        Err(error) => Err(error),
    }
}

fn received_id(host: &Host, selector: &str) -> Result<String, ChainError> {
    if DatabaseId::from_hex(selector).is_ok() {
        return Ok(selector.to_owned());
    }
    match crate::registered_share::resolve(&host.home, selector) {
        Ok(Some(share)) if share.signed_log => Ok(share.id.as_str().to_owned()),
        _ => Err(ChainError::NotFound(
            "database is not registered on this node",
        )),
    }
}

/// `{databaseId, members, any?, entryHash?, writerSeq?}` from the folded grants.
pub(crate) fn grants(host: &Host, selector: &str) -> Result<Value, ChainError> {
    with_history(host, selector, |id, history| {
        let members: Vec<Value> = history
            .grants()
            .members()
            .map(|(key, role)| json!({ "nodeKey": key.to_hex(), "role": role }))
            .collect();
        let mut out = json!({ "databaseId": id, "members": members });
        if let Some(role) = history.grants().any_role() {
            out["any"] = json!(role);
        }
        if let Some(tip) = history.latest_grant() {
            out["entryHash"] = json!(tip.entry_hash.to_hex());
            out["writerSeq"] = json!(tip.writer_seq);
        }
        Ok(out)
    })
}

/// `{databaseId, heads}`: the publisher-signed tips, for comparison only.
pub(crate) fn heads(host: &Host, selector: &str) -> Result<Value, ChainError> {
    with_history(host, selector, |id, history| {
        let heads: Vec<Value> = history
            .publisher_heads()
            .iter()
            .map(|head| {
                json!({
                    "deviceId": head.device_id.to_string(),
                    "writerSeq": head.writer_seq,
                    "entryHash": head.entry_hash.to_hex(),
                })
            })
            .collect();
        Ok(json!({ "databaseId": id, "heads": heads }))
    })
}

/// `{databaseId, urls}` from the latest publisher-signed relays entry.
pub(crate) fn relays(host: &Host, selector: &str) -> Result<Value, ChainError> {
    with_history(host, selector, |id, history| {
        Ok(json!({ "databaseId": id, "urls": history.relays() }))
    })
}

/// The first signed relay URL, also saved as this node's relay base URL.
pub(crate) fn bootstrap_relay(host: &Host, selector: &str) -> Result<Value, ChainError> {
    with_history(host, selector, |id, history| {
        let url = history
            .relays()
            .first()
            .cloned()
            .ok_or(ChainError::NotFound(
                "no signed relays entry is held for this database",
            ))?;
        super::store_relay_base_url(&host.home, &url).map_err(|_| ChainError::Unconfirmed)?;
        Ok(json!({ "databaseId": id, "url": url }))
    })
}

/// True when the folded grants of `database_id` give `node_key` an active role.
pub(crate) fn has_active_grant(host: &Host, database_id: &str, node_key: &str) -> bool {
    let Ok(node) = NodeKey::from_hex(node_key) else {
        return false;
    };
    host.histories
        .with_mut(database_id, |history| {
            history.grants().role_of(&node).is_some()
        })
        .unwrap_or(false)
}
