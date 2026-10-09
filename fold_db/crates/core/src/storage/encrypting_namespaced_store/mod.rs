mod checkpoints;
mod legacy;
mod reseal;

use super::encrypting_store::EncryptingKvStore;
use super::error::{StorageError, StorageResult};
use super::reap_unsealed::{
    reap_checkpoint_key, ReapUnsealedCheckpoint, ReapUnsealedOptions, ReapUnsealedReport,
    REAP_CHECKPOINT_NAMESPACE,
};
use super::reseal_at_rest::{
    checkpoint_key, collection_is_allowed, legacy_checkpoint_key, ResealAtRestCheckpoint,
    ResealAtRestOptions, ResealAtRestReport, RESEAL_AT_REST_ALLOWLIST, RESEAL_CHECKPOINT_NAMESPACE,
    RESEAL_CHECKPOINT_VERSION, RESEAL_TARGET_FORMAT_VERSION,
};
use super::traits::{KvStore, NamespacedStore};
use crate::crypto::CryptoProvider;
use crate::resident::LogicalResidentSet;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Namespaces deliberately stored in plaintext — the encrypt-at-rest
/// **default-deny** allowlist (Gap G1 of `docs/security/at-rest-threat-model.md`,
/// §5.3). Every namespace opened through [`EncryptingNamespacedStore`] is
/// encrypted with AES-256-GCM at the value level *unless* it appears here, so
/// adding a new namespace defaults to encrypted and a value that should leak
/// in the clear must be justified, not the reverse. This inverts the prior
/// hand-curated `ENCRYPTED_NAMESPACES` allowlist (which silently left
/// `schemas`, `public_keys`, `node_id_schema_permissions`, `views`, lineage,
/// etc. in plaintext on disk).
///
/// Production plaintext allowlist (default-deny exceptions).
///
/// **Empty as of 2026-08-05.** The retired in-process embedding collection
/// `native_index` was the sole product exception (plaintext-by-policy for cold
/// boot cost, 2026-07-20). The product path that needed that exception is
/// gone; keeping a plaintext special case for a dead collection only preserved
/// a first-class decrypt migration and backup-policy coupling for no product
/// benefit. Residual cold-home `native_index` keys (if any) are not special-
/// cased here: dual-read `migration_mode` still serves legacy plaintext rows
/// when a namespace is default-encrypted, and one-shot maintain purge is the
/// path for deliberate cleanup — not a primary-boot decrypt walk.
///
/// Future exceptions need a comment naming the data and why disclosure is
/// acceptable. Only *values* are encrypted; keys stay plaintext (accepted leak,
/// threat-model §5.3) so prefix scans and key ordering are unaffected.
pub const PLAINTEXT_NAMESPACES: &[&str] = &[];

/// LastStore Layer-B plaintext policy — **atoms-only sealing** (fold #782).
///
/// The value-level `ENC:` seam stays only around secret content: atoms, blobs,
/// tips, change feed, sync capture. Boot-critical catalogs and derived indexes
/// listed below are written in the clear so a node can start and reason about
/// shape without decrypting personal atoms — decrypting them costs boot-time
/// AES on exactly the catalogs the startup path reads (schema load, lineage
/// walks).
///
/// **What this does *not* mean (corrected 2026-07-25).** An earlier version of
/// this comment justified the exemption as "LastStore's cabinet/frame layer owns
/// device-local at-rest protection." That premise does not hold in the deployed
/// configuration. Under `packaging=plain` — the default for new hash-group homes
/// and what the primary runs — there is **no LastStore data_key and no frame
/// AEAD** (see `open_primary_store` in
/// `fold_db_core::factory::local`). Segment records are `segfmt` framed but not
/// sealed, so these namespaces are readable with no key. Frame AEAD exists only
/// for legacy `frame_aead` (LSF1) homes.
///
/// So the accurate statement of the policy is: these namespaces are **not
/// encrypted at any LastDB layer**, and their device-local protection is the
/// host filesystem's (FileVault / LUKS / dm-crypt), not LastStore's.
///
/// Measured on the live primary 2026-07-25 (read-only scan classifying each
/// record body): `atoms` 12252 sealed / 0 plain; write-target `tips` and
/// leftover `field_tips` residue, plus `field_update_order_log`,
/// `sync_capture`, `change_feed`, `cas_blobs` all sealed; `schemas` 0 / 37,
/// `schema_index` 0 / 2734, `schema_states` 0 / 37, `idempotency` 0 / 1140,
/// `public_keys` 0 / 1.
/// The policy is working exactly as designed; only its stated reason was wrong.
///
/// **Scope of what travels — corrected 2026-08-03 (was wrong above).**
/// There are **two independent backup planes**, and only one of them encrypts:
///
/// - The **snapshot** plane (`Snapshot::seal()`, `sync/engine/backup.rs`)
///   encrypts the whole snapshot with the account E2E key before upload.
/// - The **LastStore chunk** plane (`storage::laststore::backup_uploader` +
///   `backup_manifest`) has no caller of `seal()`. It walks on-disk `.seg`
///   files and does a live-path file-backed PUT — verbatim bytes,
///   no encryption layer of its own.
///
/// So at-rest state **is** cloud state for every namespace the chunk plane
/// includes: a plaintext namespace here is plaintext in the object store too,
/// unless it is also listed in `BACKUP_EXCLUDED_EXACT`
/// (`storage::laststore::backup_manifest`). What travels in the clear is schema
/// shape and descriptive names, plus derived index/state metadata — never atom
/// content, which stays `ENC:`-sealed regardless of plane, and never file blob
/// bytes, which carry their own per-blob DEK.
///
/// Under the Trinity-only standard that is the **designed** outcome, not a
/// residual exposure: published catalogs are not secret, so they are allowed to
/// travel cleartext, and `lastdb-cloud-backup-ships-cleartext-schema-chunks-20260803`
/// is a description of the plane rather than an open incident. The coupling to
/// `BACKUP_EXCLUDED_EXACT` is still enforced by
/// `backup_role_matches_at_rest_encryption_exemption`, but it now demands a
/// *declaration* rather than exclusion: a namespace added here must be either
/// backup-excluded or named in `BACKUP_CLEARTEXT_CATALOG_NAMESPACES`, so the
/// two lists still cannot diverge silently and no Trinity surface can slip into
/// the cleartext set.
///
/// Note that this coupling answers **confidentiality** only — whether a
/// namespace may travel in the clear, not whether it travels at all. Choosing
/// "backup-excluded" here is a durability decision as well, and for a
/// source-of-truth collection it is the wrong one: excluding
/// `schema_states`/`schema_superseded_by` on secrecy grounds left restored
/// devices missing schema state until 2026-08-04. That half is enforced
/// separately by `sot_collections_are_backed_up_or_declared_exception`
/// against `mini_cutover::plane_roles::SOT_COLLECTIONS`.
///
/// **Per-namespace decision (2026-08-03, gap G1 first card).** The premise
/// above ("frame AEAD protects these") does not hold under `packaging=plain`,
/// so a 2026-08-03 pass moved `schemas`, `node_id_schema_permissions`,
/// `lineage_forward`, and `lineage_reverse` off this list onto the encrypted
/// set (#1153), reasoning that plaintext catalogs on an unsealed volume are a
/// confidentiality gap worth closing.
///
/// **Superseded the same day — Trinity-only standard (won't-undo, Tom
/// 2026-08-03).** That direction was reversed as a *product* decision, not a
/// measurement correction: **published schema definitions are not a secret.**
/// The entire product encryption story is four seals and nothing else —
/// HashKey blind (`blind_v1`), RangeKey OPE (`ope_v1`), atom content sealed
/// under the account E2E key (`ENC:`), and file blobs under a per-blob DEK
/// whose KDK lives in sealed atom access metadata. Catalog / schema / lineage /
/// permission namespaces are **deliberately plaintext by policy**, on disk and
/// in cloud backup alike, and agents must not re-file "encrypt schemas at rest"
/// work. Brain: `preference-lastdb-encryption-standard-trinity-only`,
/// `decision-2026-08-03-encryption-standard-trinity-only`.
///
/// So the per-entry rationale for this list is now uniform rather than a
/// per-namespace secrecy triage:
///
/// - **Catalog / schema / lineage / permissions** — `schemas`, `schema_index`,
///   `schema_states`, `schema_superseded_by`, `node_id_schema_permissions`,
///   `lineage_forward`, `lineage_reverse`: published or derived catalog shape.
///   Not secret by standard, and decrypting them costs boot-time AES on exactly
///   the catalogs the startup path reads.
///   (History: post-WASM ghosts `views` / `view_states` /
///   `transform_field_overrides` / `process_results` removed from this catalog.)
/// - **`public_keys`** — public by definition. **`idempotency`** — opaque UUIDs.
///
/// **Not listed:** the retired `native_index` collection (removed 2026-08-05
/// with the dead-system deletion train). It remains backup-excluded as
/// rebuildable residue when present, but is no longer a first-class
/// plaintext-by-policy product namespace and is not unwrapped on boot.
///
/// What is **not** on this list is the load-bearing part: atom content
/// (`main`), `atoms`, `tips`, `cas_blobs`, `metadata` and the boot decrypt
/// proof namespaces stay sealed under the standard above. Adding a Trinity
/// surface here is a product-crypto regression, not a policy tweak —
/// `factory::boot` pins the proof namespaces against this list, and
/// `backup_manifest` pins the cloud side.
///
/// Adding a namespace here needs a comment naming the data and why it is not a
/// Trinity surface, plus a matching entry in `backup_manifest` (either
/// backup-excluded or declared cleartext-shippable) — the coupling is enforced
/// by `backup_role_matches_at_rest_encryption_exemption`. Removing one is not a
/// doc change: see `DEFAULT_ENCRYPT_FLIPPED_NAMESPACES` and the strict-marker
/// clearing in `fold_db_core::factory::local::migrations` (#877) for the
/// migration a namespace must go through when it leaves or rejoins the
/// encrypted set.
///
/// Threat model: `docs/security/at-rest-threat-model.md` §4.5.
pub const LASTSTORE_PLAINTEXT_NAMESPACES: &[&str] = &[
    "schemas",
    // Database/schema -> universal instance references. Catalog metadata only;
    // never molecule/atom content.
    "db_catalog",
    "molecule_keys",
    "schema_states",
    "schema_superseded_by",
    "schema_index",
    "public_keys",
    "node_id_schema_permissions",
    "idempotency",
    "lineage_forward",
    "lineage_reverse",
];

/// Reserved namespace holding at-rest sentinels keyed by namespace name.
///
/// It once held the per-store strict-mode markers of Gap G1 §5.3, which let a
/// boot adopt strict without re-running an O(rows) clean scan. Those markers
/// and that scan are gone
/// (`decision-2026-09-14-drop-dual-read-unsealed-is-gone`): an un-enveloped row
/// in an encrypted namespace reads as absent, so there is nothing left to prove
/// at boot and nothing to record having proved. The name is kept because homes
/// on disk still carry the namespace.
///
/// What still lives here is the `plaintext-sweep:<ns>` sentinel set, which
/// gates the catalog unwrap sweeps. It holds only namespace-name provenance, so
/// the namespace is plaintext-by-policy and must stay that way: its
/// bootstrap-time read would otherwise recurse through the seam it configures.
pub const STRICT_MARKER_NAMESPACE: &str = "__at_rest_strict_markers";

/// Key prefix for plaintext-policy sweep completion sentinels stored in
/// [`STRICT_MARKER_NAMESPACE`].
const PLAINTEXT_SWEEP_MARKER_PREFIX: &str = "plaintext-sweep:";

/// Sentinel value written once a plaintext-policy namespace has been scanned
/// and all residual `ENC:` rows have been unwrapped.
const PLAINTEXT_SWEEP_MARKER_VALUE: &[u8] = b"1";

/// Namespaces that flipped from plaintext to default-encrypt when the
/// [`PLAINTEXT_NAMESPACES`] allowlist was inverted (Gap G1 of
/// `docs/security/at-rest-threat-model.md`, §5.3). A node upgraded across that
/// flip still holds **legacy plaintext rows** for these namespaces on disk;
/// `migration_mode` dual-read keeps them readable, but lazy rewrite-on-write
/// would never touch rows of records that are never rewritten, so they would
/// linger in plaintext indefinitely. The boot-time sweep
/// (`EncryptingNamespacedStore::encrypt_legacy_plaintext_namespace`, driven
/// from `fold_db_core::factory`) re-encrypts them once.
///
/// Deliberately **excludes** the namespaces that were already encrypted before
/// the flip and therefore hold no legacy plaintext: `main` and `metadata`
/// (the prior hand-curated allowlist). Also excludes the retired `native_index`
/// collection — residual rows (if any) are not re-encrypted on every boot; dual
/// read covers legacy plaintext and deliberate cleanup is a maintain path, not
/// a first-class product migration. Excluding `main` also bounds the sweep
/// cost — it is the large user-data namespace and is never rescanned here; the
/// namespaces below hold small-to-moderate schema/index/lineage metadata.
pub const DEFAULT_ENCRYPT_FLIPPED_NAMESPACES: &[&str] = &[
    "schemas",
    "schema_states",
    "schema_superseded_by",
    "node_id_schema_permissions",
    "public_keys",
    "idempotency",
    "lineage_forward",
    "lineage_reverse",
];

/// A decorator over any `NamespacedStore` that wraps returned `KvStore`
/// instances in `EncryptingKvStore` so their values are AES-256-GCM encrypted
/// at rest.
///
/// Encryption is **default-on**: every namespace is encrypted except those in
/// the [`PLAINTEXT_NAMESPACES`] allowlist, which are returned unwrapped.
pub struct EncryptingNamespacedStore {
    inner: Arc<dyn NamespacedStore>,
    crypto: Arc<dyn CryptoProvider>,
    /// Namespaces returned unwrapped (plaintext). Everything *not* in this set
    /// is encrypted — see [`PLAINTEXT_NAMESPACES`].
    plaintext_namespaces: HashSet<String>,
}

impl EncryptingNamespacedStore {
    /// Create a new encrypting namespaced store with the default
    /// (default-encrypt) policy: every namespace is encrypted except those in
    /// [`PLAINTEXT_NAMESPACES`].
    ///
    /// - `inner`: The underlying namespaced store (Sled, InMemory, etc.).
    /// - `crypto`: The crypto provider to use for encryption/decryption.
    pub fn new(inner: Arc<dyn NamespacedStore>, crypto: Arc<dyn CryptoProvider>) -> Self {
        let plaintext_namespaces = PLAINTEXT_NAMESPACES
            .iter()
            .copied()
            .map(str::to_string)
            .collect();
        Self {
            inner,
            crypto,
            plaintext_namespaces,
        }
    }

    /// Create with a custom plaintext allowlist (for testing the
    /// default-encrypt policy without depending on the production list).
    /// Namespaces in `plaintext_namespaces` are returned unwrapped; everything
    /// else is encrypted.
    pub fn with_plaintext_namespaces(
        inner: Arc<dyn NamespacedStore>,
        crypto: Arc<dyn CryptoProvider>,
        plaintext_namespaces: Vec<String>,
    ) -> Self {
        Self {
            inner,
            crypto,
            plaintext_namespaces: plaintext_namespaces.into_iter().collect(),
        }
    }

    /// Check if a namespace should be encrypted. Default-encrypt: a namespace
    /// is encrypted unless it is on the [`PLAINTEXT_NAMESPACES`] allowlist or is
    /// the reserved [`STRICT_MARKER_NAMESPACE`] (which holds only namespace-name
    /// provenance and must stay plaintext to avoid bootstrap recursion).
    pub(crate) fn should_encrypt(&self, namespace: &str) -> bool {
        namespace != STRICT_MARKER_NAMESPACE && !self.plaintext_namespaces.contains(namespace)
    }

    /// Public view of [`Self::should_encrypt`], for callers that must know
    /// whether scanning a namespace can prove anything about the at-rest key.
    ///
    /// The boot decrypt gate needs this: a plaintext-by-policy namespace holds
    /// no envelopes, so it reports zero undecryptable rows under *any* key and
    /// proves nothing. Asking the store — rather than re-deriving the policy
    /// from a second hardcoded list — keeps the two from drifting apart, which
    /// is exactly how the gate came to proof `schemas`/`schema_states` long
    /// after both became plaintext by policy.
    pub fn encrypts_namespace(&self, namespace: &str) -> bool {
        self.should_encrypt(namespace)
    }
}

#[async_trait]
impl NamespacedStore for EncryptingNamespacedStore {
    async fn restore_durability_barrier(&self) -> StorageResult<()> {
        self.inner.restore_durability_barrier().await
    }

    async fn flush_written_keys(&self, keys: &[laststore::ShardKey]) -> StorageResult<()> {
        self.inner.flush_written_keys(keys).await
    }

    fn raw_last_store(&self) -> Option<Arc<laststore::LastStore>> {
        self.inner.raw_last_store()
    }

    fn logical_resident_set(&self) -> Option<Arc<Mutex<LogicalResidentSet>>> {
        self.inner.logical_resident_set()
    }

    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        let inner_store = self.inner.open_namespace(name).await?;

        if self.should_encrypt(name) {
            // Once a namespace has been proven clean, the strict marker becomes
            // the authority for that namespace: missing `ENC:` is rejected
            // instead of dual-read as plaintext. Namespaces without a strict
            // marker still honor the store's global migration mode.
            // No dual-read arm: an un-enveloped value in an encrypted
            // namespace reads as absent, so there is nothing to decide here.
            let mut kv = EncryptingKvStore::new(name, inner_store, self.crypto.clone());
            if let Some(set) = self.inner.logical_resident_set() {
                kv = kv.with_logical(set);
            }
            Ok(Arc::new(kv))
        } else {
            // Non-sensitive namespaces: pass through without encryption
            Ok(inner_store)
        }
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.inner.delete_namespace(name).await
    }

    // The plane drain moves opaque value bytes between collections BELOW the
    // encryption seam (collection placement lives inside one encrypted
    // namespace), so ciphertext stays valid — the same reason the offline
    // `lastdb_local_maintain` drain works on real homes. Delegate, or the
    // production stack — which always wraps LastStore in this store — would
    // report "unsupported" on exactly the node the drain exists for.
    async fn drain_plane_residue(
        &self,
        options: crate::storage::laststore::PlaneResidueDrainOptions,
    ) -> StorageResult<crate::storage::laststore::PlaneResidueDrainReport> {
        self.inner.drain_plane_residue(options).await
    }

    async fn compact_collection(
        &self,
        options: crate::storage::laststore::CollectionCompactOptions,
    ) -> StorageResult<crate::storage::laststore::CollectionCompactReport> {
        self.inner.compact_collection(options).await
    }

    async fn compact_retired_groups(&self) -> StorageResult<()> {
        // The production stack always wraps LastStore in this store. A default
        // no-op here would drop the receipt pass on the node it exists for.
        self.inner.compact_retired_groups().await
    }

    fn report_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        self.inner.report_committed_successor_history()
    }

    // Directory-level; the seam sees no values. Ids are plaintext on every
    // packaging this verb can prove (it needs the id sidecar), so the
    // expected id passes through unchanged.
    fn drop_dead_hash_group(
        &self,
        options: crate::storage::laststore::DeadHashGroupDropOptions,
    ) -> StorageResult<crate::storage::laststore::DeadHashGroupDropReport> {
        self.inner.drop_dead_hash_group(options)
    }

    fn stamp_pending_committed_successor_history(
        &self,
    ) -> StorageResult<crate::storage::laststore::StampCommittedSuccessorHistoryReport> {
        self.inner.stamp_pending_committed_successor_history()
    }

    async fn reseal_at_rest(
        &self,
        options: ResealAtRestOptions,
    ) -> StorageResult<ResealAtRestReport> {
        self.reseal_at_rest_impl(options).await
    }

    async fn reap_unsealed(
        &self,
        options: ReapUnsealedOptions,
    ) -> StorageResult<ReapUnsealedReport> {
        self.reap_unsealed_impl(options).await
    }

    // Bytes on disk are bytes on disk: encryption changes the size of a record
    // but not which backend owns the directory. Delegate, or the production
    // stack — which always wraps LastStore in this store — would report `None`
    // and the pin-log bloat trigger would be blind on exactly the node that
    // carried 20.4 GiB of dead segment bytes behind one live record.
    fn collection_disk_bytes(&self, collection: &str) -> Option<u64> {
        self.inner.collection_disk_bytes(collection)
    }

    fn collection_disk_usage(
        &self,
        collection: &str,
    ) -> Option<crate::storage::traits::CollectionDiskUsage> {
        self.inner.collection_disk_usage(collection)
    }

    // Read cost is a property of the physical store, not of the encryption
    // seam. Delegate, or the production stack — which always wraps LastStore in
    // this store — would report `None` and the counters would stay invisible on
    // exactly the node they exist for.
    fn cold_shard_loads(&self) -> Option<u64> {
        self.inner.cold_shard_loads()
    }

    fn walk_ids_visited(&self) -> Option<u64> {
        self.inner.walk_ids_visited()
    }

    fn read_cost(&self) -> Option<crate::storage::traits::ReadCostStats> {
        self.inner.read_cost()
    }

    fn trim_warm_cache_for_pressure(&self) -> StorageResult<Option<u64>> {
        self.inner.trim_warm_cache_for_pressure()
    }

    fn warm_set_admission_stats(&self) -> Option<crate::storage::traits::WarmSetAdmissionStats> {
        self.inner.warm_set_admission_stats()
    }

    fn set_effective_warm_bytes(&self, bytes: u64) {
        self.inner.set_effective_warm_bytes(bytes);
    }

    fn set_warm_drain_hold(&self, hold: bool) {
        self.inner.set_warm_drain_hold(hold);
    }

    fn set_host_pressure_high(&self, high: bool) {
        self.inner.set_host_pressure_high(high);
    }

    fn evict_warm_set_to_bytes(
        &self,
        target: u64,
    ) -> StorageResult<Option<crate::storage::traits::WarmSetEvictionReport>> {
        self.inner.evict_warm_set_to_bytes(target)
    }
}
