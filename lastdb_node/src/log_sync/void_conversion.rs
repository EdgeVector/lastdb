//! Sign a member's voided direct writes again as proposals.

use mutation_log::{decode_record_payload, entry_hash, SignedEntry, VerifiedHistory};
use serde_json::Value;

use super::chain_author::{self, AuthorIdentity};
use super::proposal::{
    author_proposal, envelope, hex_digest, load_ledger, store_ledger, ContributionError, Proposed,
    Submitted,
};
use crate::host::Host;
use crate::registered_share::RegisteredShare;

/// One voided direct write, as the proposal that carries the same change.
struct Conversion {
    mutation_id: String,
    namespace: String,
    record_key: String,
    value: Vec<u8>,
    digest: String,
}

/// Turn this node's voided direct writes into proposals on its own stream.
///
/// A member lowered from write to submit can have signed a direct write before
/// its node learned of the reduction. The publisher's cutoff voids that entry
/// on every node, so it never changes a record. Its change is not lost: the
/// same change is signed again as a proposal, which the publisher applies under
/// the proposal rules, or leaves submitted. The voided entry stays in the log.
/// A ledger entry keyed by the voided entry's hash makes the conversion happen
/// once, also across a restart. Returns how many proposals were signed.
pub(crate) fn convert_voided(
    host: &Host,
    share: &RegisteredShare,
) -> Result<usize, ContributionError> {
    let database_id = share.id.as_str();
    let identity = chain_author::identity_of(host).map_err(|_| ContributionError::Unavailable)?;
    let plans = super::histories_for(&host.home)
        .with_mut(database_id, |history| conversions(history, &identity))
        .map_err(|_| ContributionError::Unavailable)?;
    if plans.is_empty() {
        return Ok(0);
    }
    let mut ledger = load_ledger(&host.home, database_id)?;
    let mut converted = 0;
    for plan in plans {
        if ledger.contains_key(&plan.mutation_id) {
            continue;
        }
        let authored = author_proposal(
            host,
            database_id,
            plan.namespace,
            &plan.record_key,
            &plan.value,
        )?;
        ledger.insert(
            plan.mutation_id,
            Submitted {
                writer_seq: authored.writer_seq,
                entry_hash: authored.entry_hash,
                body_sha256: plan.digest,
            },
        );
        store_ledger(&host.home, database_id, &ledger)?;
        converted += 1;
    }
    Ok(converted)
}

fn conversions(history: &VerifiedHistory, identity: &AuthorIdentity) -> Vec<Conversion> {
    history
        .voided_direct_by(identity.node_key)
        .iter()
        .filter_map(|entry| conversion(history, entry))
        .collect()
}

fn conversion(history: &VerifiedHistory, entry: &SignedEntry) -> Option<Conversion> {
    let SignedEntry::Data { body, .. } = entry else {
        return None;
    };
    let decoded = decode_record_payload(&body.payload).ok()?;
    let hash = entry_hash(entry).ok()?;
    let winner = history
        .records()
        .get(&(body.namespace.clone(), decoded.record_key.clone()))
        .filter(|record| !record.value.is_empty());
    let (mutation_type, expected, record) = if decoded.value.is_empty() {
        let winner = winner?;
        (
            "delete",
            serde_json::json!({ "type": "entry", "entry": winner.entry.to_hex() }),
            None,
        )
    } else {
        let record = serde_json::from_slice::<Value>(&decoded.value)
            .ok()
            .filter(Value::is_object)?;
        match winner {
            None => (
                "create",
                serde_json::json!({ "type": "absent" }),
                Some(record),
            ),
            Some(winner) => (
                "replace",
                serde_json::json!({ "type": "entry", "entry": winner.entry.to_hex() }),
                Some(record),
            ),
        }
    };
    let proposed = Proposed {
        namespace: body.namespace.clone(),
        record_key: decoded.record_key,
        mutation_type: mutation_type.to_owned(),
        expected: Value::Null,
        record,
    };
    let value = envelope(&proposed, expected).ok()?;
    let mutation_id = uuid::Uuid::from_slice(&hash.as_bytes()[..16])
        .ok()?
        .to_string();
    Some(Conversion {
        mutation_id,
        namespace: proposed.namespace,
        record_key: proposed.record_key,
        value,
        digest: hex_digest(hash.to_hex().as_bytes()),
    })
}
