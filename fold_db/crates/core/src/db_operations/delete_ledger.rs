//! Durable ledger of atom hard-deletes.
//!
//! # Why this exists
//!
//! The atom counter deletes an atom body when its count reaches zero. The batch
//! that ends the last reference is a purge (`purge_record`, the compliance and
//! GDPR erasure verb), a delete, or another write. Before this module the purge
//! left no durable evidence: `purge_record` emitted a `tracing::info!` into a
//! ~15h log window. So the store could not answer a question about its own
//! contents: classifying 121 unresolved tips took three agent sessions and still
//! could not prove which run removed them, only which mechanism could have.
//!
//! For a compliance verb that is also an audit gap: no evidence a purge ran,
//! what it removed, or when.
//!
//! # The question this answers
//!
//! > This tip has no atom body. Was it deleted on purpose, or lost to a bug?
//!
//! A ledger read over the store's whole lifetime settles it. The **negative**
//! answer is the strong one: if no ledger row covers the window in which a body
//! disappeared, no delete path removed it, and the disappearance is a bug to
//! chase rather than a purge to accept.
//!
//! # Why the converge lane writes a row that deletes no body
//!
//! The delete-converge cutover moved live `Delete` off the inline path that
//! wrote a `delete` row and onto [`super::super::fold_db_core::purge`]'s
//! `converge_delete_tips`, which removes tips and defers byte reclaim to a
//! janitor. For 51 hours the ledger's newest row was pre-cutover and every
//! window it was asked about answered "no deletes" — including windows in which
//! the delete path had run thousands of times. The negative answer above is
//! only strong if silence means silence, so the converge lane writes its own
//! [`LEDGER_VERB_DELETE_CONVERGE`] row with `atoms_deleted: 0`. That
//! distinguishes the two states an empty window used to collapse:
//!
//! - **no row at all** — no delete path ran;
//! - **a converge row** — a delete ran and deliberately removed no body, so a
//!   body that vanished in that window still has no delete to account for it.
//!
//! # What is deliberately NOT recorded
//!
//! - **Atom content.** A purge ledger that retains what was purged defeats the
//!   erasure it audits.
//! - **Atom uuids.** Atoms are content-addressed over
//!   `(schema_name, content, source_file_name, metadata)`, so an atom uuid *is*
//!   a hash of the content. Retaining the uuids of purged atoms would leave a
//!   confirmation oracle: anyone who can guess an erased value can recompute
//!   the uuid and check it against the ledger. That is the same information
//!   leak the purge exists to close, so the ledger carries counts, not uuids.
//! - **The purged record key in plaintext.** For a GDPR erasure the key is
//!   typically the data subject's identifier. The ledger stores
//!   `key_fingerprint` — a SHA-256 over `{schema}\0{described key}` — so the
//!   audit question "did we honour the erasure request for X?" is answerable by
//!   computing X's fingerprint and looking it up, while the ledger alone does
//!   not enumerate the subjects whose data was erased.
//!
//! # Durability shape: write-ahead, then confirm
//!
//! Each batch writes its row **before** the destructive delete, with
//! `committed: false`, and re-puts the same key with `committed: true` once the
//! batch returns. Two small writes on a rare admin verb, in exchange for:
//!
//! - A crash or kill between the two writes cannot lose the evidence. A
//!   post-hoc-only ledger would.
//! - A row stuck at `committed: false` is itself diagnostic — a delete batch
//!   started and never confirmed, so its counts are what was *attempted*.
//!
//! # Retention
//!
//! **Ledger rows are never automatically removed.** They live under the `dellog:`
//! prefix, which no delete path touches:
//!
//! - The atom counter deletes only the body and the counter of an atom whose
//!   count reached zero (`atom:{uuid}\0b` and `atom:{uuid}\0c`). A ledger row is
//!   neither, and no counter counts it.
//! - `purge_record` deletes only `history:` / `atom:` / schema-index keys for the
//!   purged record.
//!
//! A self-erasing audit trail is worse than none, so this is enforced by
//! construction rather than left to convention. Rows are ~300 bytes and one row covers a
//! whole batch, not a key, so unbounded growth is not a space concern; the
//! class is reported by `lastdb db inventory` as `atom_delete_ledger` so it
//! stays visible if that ever stops being true. `delete` and `delete-converge`
//! are live-traffic verbs rather than administrative ones, but they are the
//! same lane at the same batch granularity: the primary accumulated 19,825
//! `delete` rows over a month on the inline path before the cutover.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

/// Bare key prefix for ledger rows. Rows sort in time order under a prefix scan
/// because the timestamp component is fixed-width zero-padded nanoseconds.
pub const ATOM_DELETE_LEDGER_PREFIX: &str = "dellog:";

/// Which path performed the delete.
pub const LEDGER_VERB_PURGE: &str = "purge";
pub const LEDGER_VERB_DELETE: &str = "delete";
/// Live `Delete` tip converge (`purge::converge_delete_tips`).
///
/// Deliberately NOT [`LEDGER_VERB_DELETE`]. Converge removes tip (`mk:`) rows
/// and their `tv:` chains and reclaims **no atom body** — byte reclaim is the
/// separate janitor pass (`design-lastdb-delete-converge-then-reclaim`). A
/// consumer that sums `atoms_deleted` over `delete` rows to answer "what
/// removed this body" must not be handed rows that never removed one, so the
/// converge lane gets its own verb rather than a `delete` row with a zero.
pub const LEDGER_VERB_DELETE_CONVERGE: &str = "delete-converge";

/// One append-only row per hard-delete batch.
///
/// Counts are what the batch was asked to remove, recorded write-ahead. On a row
/// with `committed: true` they are also what it removed; on a `committed: false`
/// row they are an upper bound, which is the safe direction for an audit trail —
/// it never under-reports a deletion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomDeleteLedgerEntry {
    /// Row format version. Bump when fields change meaning, not when adding.
    pub version: u32,
    /// [`LEDGER_VERB_PURGE`], [`LEDGER_VERB_DELETE`] or [`LEDGER_VERB_DELETE_CONVERGE`].
    pub verb: String,
    /// When the batch was planned (RFC3339). Matches the key's timestamp.
    pub at: String,
    /// Who asked. Free-form caller tag, e.g. `"mutation-pipeline"` or
    /// `"admin-api"`. Not authenticated — evidence of provenance, not identity.
    pub caller: String,
    /// True once the destructive batch returned successfully. A row left at
    /// `false` means the batch started and did not confirm.
    pub committed: bool,
    /// Purge only: the schema whose record was erased.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Purge only: SHA-256 hex over `{schema}\0{described key}`. See the
    /// module docs for why the key itself is not stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_fingerprint: Option<String>,
    /// `atom:` bodies removed.
    pub atoms_deleted: u64,
    /// `history:` rows removed (purge only).
    #[serde(default)]
    pub history_rows_deleted: u64,
    /// Purge only: Search-plane invalidations committed for the record(s).
    ///
    /// Historically this counted in-process `emb:` / `graveyard:emb:` rows.
    /// After native index removal, Mini no longer stores those rows; the value
    /// is the number of purged keys (one Search tombstone each) filled in at
    /// ledger commit. Meaningful only on a `committed: true` row.
    #[serde(default)]
    pub embedding_rows_deleted: u64,
    /// Total storage keys in the delete batch — bodies plus their locators,
    /// schema-index rows and (purge) history rows. The number that actually
    /// hit the store, as opposed to the logical atom count.
    #[serde(default)]
    pub storage_keys_deleted: u64,
    /// Delete-converge only: live `mk:` tip rows removed to make disk agree
    /// with the resident tombstone. Counted write-ahead, so on a
    /// `committed: false` row it is what the batch was asked to remove.
    #[serde(default)]
    pub tips_removed: u64,
    /// Delete-converge only: records in the batch that had durable residue to
    /// converge. Keys already converged, or never persisted, are dropped
    /// before the row is written and are not counted here.
    #[serde(default)]
    pub records_converged: u64,
    /// Purge only: how many of the hard-deleted atoms carried a file-blob
    /// reference (a `$lastdb_file` content pointer or `file_blob_ref` /
    /// `file_hash` metadata). A count, never the refs themselves — a blob_ref
    /// is a hash of the file's plaintext, so retaining it would be the same
    /// confirmation oracle the atoms-uuid rule closes (see module docs). A
    /// nonzero value tells compliance that sealed blob bytes may now be
    /// orphaned. The blob counter reclaims them when the dwell passes.
    #[serde(default)]
    pub file_pointer_atoms_purged: u64,
}

impl AtomDeleteLedgerEntry {
    /// All-zero row for `verb`, stamped now. Constructors below customize it;
    /// keeping the zeroing in one place is what lets a new counter field land
    /// as one struct field + one line here instead of an edit per verb.
    fn base(verb: &str, caller: &str) -> Self {
        Self {
            version: 1,
            verb: verb.to_string(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_string(),
            committed: false,
            schema: None,
            key_fingerprint: None,
            atoms_deleted: 0,
            history_rows_deleted: 0,
            embedding_rows_deleted: 0,
            storage_keys_deleted: 0,
            tips_removed: 0,
            records_converged: 0,
            file_pointer_atoms_purged: 0,
        }
    }

    /// A hard-erasure row (`delete` or `purge`), before the delete batch runs.
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        Self {
            schema: Some(schema.to_string()),
            key_fingerprint: Some(key_fingerprint(schema, described_key)),
            ..Self::base(verb, caller)
        }
    }

    /// A purge row, before the delete batch runs.
    pub fn purge(caller: &str, schema: &str, described_key: &str) -> Self {
        Self::erasure(LEDGER_VERB_PURGE, caller, schema, described_key)
    }

    /// A live-`Delete` tip-converge row, before the tip batch runs.
    ///
    /// Carries `schema` + `key_fingerprint` like the other erasure verbs, so
    /// the audit question "did a delete touch this record, in this window?" is
    /// answerable on the converge lane as it was on the inline one.
    pub fn delete_converge(caller: &str, schema: &str, described_key: &str) -> Self {
        Self::erasure(LEDGER_VERB_DELETE_CONVERGE, caller, schema, described_key)
    }
}

/// SHA-256 hex over `{schema}\0{described key}`.
///
/// The separator matters: without it `("ab", "c")` and `("a", "bc")` would
/// fingerprint identically, and a purge of one record would read as evidence of
/// purging another.
pub fn key_fingerprint(schema: &str, described_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(schema.as_bytes());
    hasher.update([0u8]);
    hasher.update(described_key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Handle to a written-ahead ledger row, so the caller can confirm it.
///
/// Deliberately not `Copy`/`Clone`-cheap-to-ignore: holding one is the reminder
/// that a batch which never calls [`AtomStore::commit_delete_ledger_row`] leaves
/// an unconfirmed row behind.
#[derive(Debug, Clone)]
pub struct DeleteLedgerHandle {
    key: String,
    entry: AtomDeleteLedgerEntry,
}

impl DeleteLedgerHandle {
    /// Full storage key of the row.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl AtomStore {
    /// Write a ledger row **before** its delete batch runs, returning a handle
    /// to confirm with once the batch succeeds.
    ///
    /// Failing to write the ledger fails the caller: a hard-delete that cannot
    /// be recorded is a hard-delete that must not happen. This is the one place
    /// the ledger is allowed to block a destructive verb, and it blocks it
    /// *before* anything is destroyed.
    pub async fn begin_delete_ledger_row(
        &self,
        storage_prefix: Option<&str>,
        entry: AtomDeleteLedgerEntry,
    ) -> Result<DeleteLedgerHandle, SchemaError> {
        // Nanoseconds keep two batches inside the same millisecond ordered; the
        // uuid suffix keeps them from colliding if the clock does not advance.
        let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let base = crate::kind_partition::anchored(
            "dellog",
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let key = build_storage_key(storage_prefix, &base);
        self.raw().put_item(&key, &entry).await.map_err(|e| {
            SchemaError::InvalidData(format!("delete ledger: failed to record intent: {e}"))
        })?;
        Ok(DeleteLedgerHandle { key, entry })
    }

    /// Re-put the row with final counts and `committed: true`.
    ///
    /// Unlike [`Self::begin_delete_ledger_row`] a failure here does **not** fail
    /// the caller: the delete already happened, and erroring out would report a
    /// destructive operation as not-done. The write-ahead row is already durable
    /// and its `committed: false` state is the honest record of this outcome, so
    /// this logs and moves on.
    pub async fn commit_delete_ledger_row(
        &self,
        handle: DeleteLedgerHandle,
        finalize: impl FnOnce(&mut AtomDeleteLedgerEntry),
    ) {
        let DeleteLedgerHandle { key, mut entry } = handle;
        finalize(&mut entry);
        entry.committed = true;
        if let Err(e) = self.raw().put_item(&key, &entry).await {
            tracing::warn!(
                ledger_key = %key,
                error = %e,
                "delete ledger: row stays uncommitted — the delete DID run; counts on this row are what was attempted"
            );
        }
    }

    /// Read ledger rows in time order (oldest first).
    ///
    /// `limit` of 0 means unbounded. This is a prefix scan by design: the ledger
    /// is an append-only time series read by administrative audit, not a
    /// key-addressed record, and the prefix bounds it to ledger rows alone.
    pub async fn list_atom_delete_ledger(
        &self,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AtomDeleteLedgerEntry>, SchemaError> {
        let prefix = build_storage_key(
            storage_prefix,
            &crate::kind_partition::anchored("dellog", ""),
        );
        let rows = self
            .raw()
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("delete ledger scan: {e}")))?;

        // `scan_prefix` ordering is backend-dependent, so sort on the key rather
        // than trusting it. The fixed-width timestamp makes byte order time
        // order.
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = rows;
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        let mut out = Vec::new();
        for (k, v) in rows {
            match serde_json::from_slice::<AtomDeleteLedgerEntry>(&v) {
                Ok(entry) => out.push(entry),
                Err(e) => {
                    // An unreadable audit row is worth knowing about, but must
                    // not blank the rest of the ledger.
                    tracing::warn!(
                        ledger_key = %String::from_utf8_lossy(&k),
                        error = %e,
                        "delete ledger: skipping unreadable row"
                    );
                }
            }
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}
