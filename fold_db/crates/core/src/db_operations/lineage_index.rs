//! Forward + reverse lineage indexes for derived molecules.
//!
//! PR 6 of `projects/molecule-provenance-dag`. Scaffolding only — no
//! production call sites. A future project (`view-compute-as-mutations`)
//! wires `LineageIndex::insert` into `MutationManager` when derived
//! molecules land on view schemas.
//!
//! # What this stores
//!
//! - **Forward** (`lineage_forward`): `derived_molecule_uuid -> Vec<MoleculeRef>`.
//!   "List all sources of Y."
//! - **Reverse** (`lineage_reverse`): `MoleculeRef::canonical_bytes() ->
//!   Vec<String>` of derived UUIDs. "List all derivatives of X." This is the
//!   redaction-case index.
//!
//! Both namespaces are skipped by store-level **capture**.
//! `crate::sync::policy` lists them in `CAPTURE_SKIP_NAMESPACES`, which is
//! about capture/snapshot only — both namespaces are still cloud-**backed
//! up** (encrypted since #1153; not on the at-rest-exemption list, so
//! `BACKUP_EXCLUDED_EXACT` doesn't touch them either). Rebuilding either
//! index is possible by replaying local molecule state — see
//! [`LineageIndex::verify_merkle_consistency`] for the consistency check used
//! during replay to confirm stored sources match the derived molecule's
//! `Provenance::Derived::sources_merkle_root`.
//!
//! # What this does not do
//!
//! - Does not construct `Provenance::Derived` molecules — that is PR of
//!   project 2.
//! - Does not run rebuild-from-replay end-to-end. Only the Merkle consistency
//!   helper is shipped here; the full replay loop is part of project 2.
//! - Is not atomic across the forward/reverse pair. `insert` and `remove`
//!   perform read-modify-write on the reverse entries. With no production
//!   callers in this PR, that is acceptable; the future project must add
//!   serialisation if concurrent writers touch the same derived UUID.

use crate::atom::{merkle::merkle_root, provenance::MoleculeRef};
use crate::error::FoldDbError;
use crate::hex::hex_lower;
use crate::schema::SchemaError;
use crate::storage::traits::KvStore;
use std::sync::Arc;

/// Two-way index from derived molecules to their source `MoleculeRef` set,
/// and back.
#[derive(Clone)]
pub struct LineageIndex {
    forward: Arc<dyn KvStore>,
    reverse: Arc<dyn KvStore>,
}

impl LineageIndex {
    pub(crate) fn new(forward: Arc<dyn KvStore>, reverse: Arc<dyn KvStore>) -> Self {
        Self { forward, reverse }
    }

    /// Flush both lineage-index namespaces to durable storage.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.forward.flush().await?;
        self.reverse.flush().await?;
        Ok(())
    }

    /// Record that `derived_uuid` was derived from `sources`.
    ///
    /// Writes the forward entry (`derived_uuid -> sources`) and, for every
    /// source, appends `derived_uuid` into the reverse entry keyed by the
    /// source's canonical bytes. Reverse entries are kept sorted and deduped
    /// so [`get_reverse`] returns a stable order regardless of insertion
    /// sequence.
    ///
    /// When the call overwrites an existing forward entry, any source that
    /// was in the previous set but not in `sources` has its reverse entry
    /// scrubbed of `derived_uuid` — otherwise the forward/reverse pair
    /// would disagree on whether `derived_uuid` was derived from that
    /// source.
    pub async fn insert(
        &self,
        derived_uuid: &str,
        sources: &[MoleculeRef],
    ) -> Result<(), FoldDbError> {
        let previous: Vec<MoleculeRef> = match self.forward.get(derived_uuid.as_bytes()).await? {
            Some(bytes) => match serde_json::from_slice(&bytes) {
                Ok(parsed) => parsed,
                Err(e) => {
                    // Ciphertext-era / empty forward tips must not veto the
                    // overwrite that heals them (same won't-undo as schema
                    // catalog skip-if-unchanged).
                    tracing::warn!(
                        derived_uuid,
                        error = %e,
                        "lineage_forward tip unreadable; treating prior sources as empty"
                    );
                    Vec::new()
                }
            },
            None => Vec::new(),
        };

        let forward_bytes = serde_json::to_vec(sources)?;
        self.forward
            .put(derived_uuid.as_bytes(), forward_bytes)
            .await?;

        for source in sources {
            let key = source.canonical_bytes();
            let mut derivatives = Self::read_reverse(&self.reverse, &key).await?;
            if !derivatives.iter().any(|d| d == derived_uuid) {
                derivatives.push(derived_uuid.to_string());
                derivatives.sort();
            }
            let bytes = serde_json::to_vec(&derivatives)?;
            self.reverse.put(&key, bytes).await?;
        }

        for stale in previous.iter().filter(|p| !sources.contains(p)) {
            let key = stale.canonical_bytes();
            let mut derivatives = Self::read_reverse(&self.reverse, &key).await?;
            derivatives.retain(|d| d != derived_uuid);
            if derivatives.is_empty() {
                self.reverse.delete(&key).await?;
            } else {
                let bytes = serde_json::to_vec(&derivatives)?;
                self.reverse.put(&key, bytes).await?;
            }
        }

        Ok(())
    }

    /// Look up the source `MoleculeRef`s for a derived molecule.
    pub async fn get_forward(
        &self,
        derived_uuid: &str,
    ) -> Result<Option<Vec<MoleculeRef>>, FoldDbError> {
        match self.forward.get(derived_uuid.as_bytes()).await? {
            Some(bytes) => match serde_json::from_slice(&bytes) {
                Ok(sources) => Ok(Some(sources)),
                Err(e) => {
                    tracing::warn!(
                        derived_uuid,
                        error = %e,
                        "lineage_forward tip unreadable; treating as missing \
                         (ciphertext-era / empty row)"
                    );
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// List every derived molecule that consumed `source_ref`. Returns an
    /// empty `Vec` when the source has no known derivatives. The returned
    /// list is sorted ascending — callers may rely on this ordering.
    pub async fn get_reverse(&self, source_ref: &MoleculeRef) -> Result<Vec<String>, FoldDbError> {
        let key = source_ref.canonical_bytes();
        Self::read_reverse(&self.reverse, &key).await
    }

    /// Drop every trace of `derived_uuid`. Removes the forward entry, then
    /// removes `derived_uuid` from every reverse entry its sources recorded.
    /// Reverse entries that become empty are deleted outright so scans stay
    /// clean.
    pub async fn remove(&self, derived_uuid: &str) -> Result<(), FoldDbError> {
        let sources: Vec<MoleculeRef> = match self.forward.get(derived_uuid.as_bytes()).await? {
            Some(bytes) => match serde_json::from_slice(&bytes) {
                Ok(parsed) => parsed,
                Err(e) => {
                    tracing::warn!(
                        derived_uuid,
                        error = %e,
                        "lineage_forward tip unreadable on remove; deleting forward key only"
                    );
                    self.forward.delete(derived_uuid.as_bytes()).await?;
                    return Ok(());
                }
            },
            None => return Ok(()),
        };

        self.forward.delete(derived_uuid.as_bytes()).await?;

        for source in sources {
            let key = source.canonical_bytes();
            let mut derivatives = Self::read_reverse(&self.reverse, &key).await?;
            derivatives.retain(|d| d != derived_uuid);
            if derivatives.is_empty() {
                self.reverse.delete(&key).await?;
            } else {
                let bytes = serde_json::to_vec(&derivatives)?;
                self.reverse.put(&key, bytes).await?;
            }
        }

        Ok(())
    }

    /// Check that `merkle_root(sources.canonical_bytes())` matches
    /// `expected_root_hex`. Used during rebuild-from-replay to confirm that
    /// the stored source list is consistent with the derived molecule's
    /// on-wire `Provenance::Derived::sources_merkle_root`. Any mismatch —
    /// reorder, added source, dropped source, mutated field — flips the
    /// return value to `false`.
    #[must_use]
    pub fn verify_merkle_consistency(sources: &[MoleculeRef], expected_root_hex: &str) -> bool {
        let leaves: Vec<Vec<u8>> = sources.iter().map(MoleculeRef::canonical_bytes).collect();
        let computed = merkle_root(&leaves);
        let computed_hex = hex_lower(computed);
        computed_hex == expected_root_hex
    }

    async fn read_reverse(
        reverse: &Arc<dyn KvStore>,
        key: &[u8],
    ) -> Result<Vec<String>, FoldDbError> {
        match reverse.get(key).await? {
            Some(bytes) => match serde_json::from_slice(&bytes) {
                Ok(parsed) => Ok(parsed),
                Err(e) => {
                    // Derived reverse plane: undecodable ciphertext/empty tips
                    // must not escalate into a mutation/request 400. Treat as
                    // empty so insert can overwrite/heal.
                    tracing::warn!(
                        error = %e,
                        "lineage_reverse tip unreadable; treating as empty \
                         (ciphertext-era / empty row)"
                    );
                    Ok(Vec::new())
                }
            },
            None => Ok(Vec::new()),
        }
    }
}
