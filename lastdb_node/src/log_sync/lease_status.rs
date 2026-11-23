//! What a node says about write leases: the lease it holds, and the lease
//! length a publisher chose.

use mutation_log::{NodeKey, VerifiedHistory};
use serde_json::{json, Value};

use super::chain_error::ChainError;
use super::chain_read;
use super::lease_member;
use crate::host::owned::OwnedError;
use crate::host::Host;

/// The newest lease this history holds, as the status shows it. `covers` says
/// whether the lease names `node` as a writer. `directWrites` says whether the
/// node signs direct entries now, and `directWritesUntilMs` is the time, by its
/// clock, at which it stops (the lease's expiry less the margin). A node signs
/// direct entries only while its current role is `write`, so `covers` can be
/// true while `directWrites` is false.
pub(super) fn held(history: &VerifiedHistory, node: NodeKey, now_ms: u64) -> Option<Value> {
    let lease = history.latest_lease()?;
    let cover = lease_member::cover(history, node, now_ms);
    Some(json!({
        "entryHash": lease.entry_hash.to_hex(),
        "authoredAtMs": lease.authored_at_ms,
        "expiresAtMs": lease.expires_at_ms,
        "closesAtMs": lease.closes_at_ms,
        "covers": history.lease_covers(&lease.entry_hash, &node),
        "directWrites": cover.is_some(),
        "directWritesUntilMs": cover.map(|found| found.until_ms),
    }))
}

/// `{databaseId, role, leaseLengthMs?, lease}` for a database this node owns or
/// has received. `lease` is the newest lease held, or null. `leaseLengthMs` is
/// the publisher's setting and appears for an owned database that has one.
pub(crate) fn status(host: &Host, selector: &str) -> Result<Value, ChainError> {
    let node = NodeKey::from_bytes(host.keypair.public_key_bytes());
    let length = chain_read::owned_database(host, selector)
        .ok()
        .and_then(|entry| entry.lease_length_ms);
    chain_read::with_history(host, selector, |id, history| {
        let role = super::received::role_of(history, node);
        let mut out = json!({
            "databaseId": id,
            "role": role,
            "lease": held(history, node, fold_db::clock::unix_millis()),
        });
        if let Some(length) = length {
            out["leaseLengthMs"] = json!(length);
        }
        Ok(out)
    })
}

/// Set (`Some`) or clear (`None`) the lease length of an owned shareable
/// database. A cleared length stops new leases. A
/// lease already signed runs to its end.
pub(crate) fn set_length(
    host: &Host,
    selector: &str,
    length_ms: Option<u64>,
) -> Result<Value, ChainError> {
    let entry = chain_read::owned_database(host, selector)?;
    host.owned
        .set_lease_length(&host.home, &entry.id, length_ms)
        .map_err(|error| match error {
            OwnedError::InvalidLeaseLength => {
                ChainError::Invalid("the lease length must be between 10 seconds and 90 days")
            }
            OwnedError::NotOwned => ChainError::NotFound("database is not registered on this node"),
            _ => ChainError::Unconfirmed,
        })?;
    super::wake::request_wake(&host.home);
    Ok(json!({ "databaseId": entry.id, "leaseLengthMs": length_ms }))
}
