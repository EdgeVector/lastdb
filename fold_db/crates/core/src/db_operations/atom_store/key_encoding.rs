//! Atom body key-encoding resolution for [`AtomStore`]: the override hooks,
//! the boot-time marker/gate, and the post-restore refresh.

use super::AtomStore;
use std::sync::atomic::Ordering;

impl AtomStore {
    /// Override the atom body key encoding (tests and the migration driver).
    ///
    /// Production resolves it at construction and refreshes it at the
    /// photograph-restore barrier. All clones change as one unit.
    #[must_use]
    pub(crate) fn with_atom_key_encoding(self, encoding: crate::atom::AtomKeyEncoding) -> Self {
        self.atom_keys_partition_prefixed
            .store(encoding.writes_partition_prefix(), Ordering::Release);
        self
    }

    /// Atom body key encoding for this store.
    #[must_use]
    pub(crate) fn atom_key_encoding(&self) -> crate::atom::AtomKeyEncoding {
        if self.atom_keys_partition_prefixed.load(Ordering::Acquire) {
            crate::atom::AtomKeyEncoding::PartitionPrefix
        } else {
            crate::atom::AtomKeyEncoding::Flat
        }
    }

    /// Re-read the durable atom-layout marker after a photograph restore.
    ///
    /// A bootstrap target starts empty, so construction resolves `flat` before
    /// the photograph installs its marker and partition-prefixed atom bodies.
    /// All clones share the atomic setting, which lets the serving store adopt
    /// the restored layout before it applies or reads the mutation-log tail.
    #[cfg_attr(not(feature = "cloud-sync"), allow(dead_code))]
    pub(crate) async fn refresh_boot_encoding_after_restore(
        &self,
    ) -> Result<(), crate::storage::StorageError> {
        self.clone().resolve_boot_encoding().await.map(|_| ())
    }

    /// Resolve the encoding this home is actually written under, stamp it, and
    /// refuse to serve a mismatch. Called at construction and after restore.
    ///
    /// Resolution order is `env override → home marker → Flat` (see
    /// [`crate::atom::atom_key_codec`]). Two things happen on top of it:
    ///
    /// - **Stamp.** Resolving to `PartitionPrefix` records that fact at
    ///   [`ATOM_KEY_ENCODING_MARKER_KEY`], so the *next* boot of this home does
    ///   not depend on an environment variable surviving. One idempotent point
    ///   write, only when the marker is missing or disagrees.
    /// - **Gate.** Resolving to `Flat` probes for prefixed keys and returns an
    ///   error if any exist, instead of serving short pages. The doc comment
    ///   this replaces had it backwards — "an unreadable home is a worse failure
    ///   than an unapplied optimization" is true for a *flat* home, but on a
    ///   migrated home falling back to `Flat` is what *creates* the unreadable
    ///   home. After `--remove-flat` there is no flat key left at all, so the
    ///   same boot would read the whole store as empty.
    ///
    /// A fresh or flat home pays one bounded prefix probe that matches nothing.
    ///
    /// **Known limit:** the probe covers the personal namespace (`atom:mk:`).
    /// Org-scoped bodies live under `{storage_prefix}:atom:mk:` and are not
    /// reachable by a single prefix scan, and the org prefixes are not known at
    /// this point in boot. The marker — which is store-wide — is the mechanism
    /// that covers them; the probe is the backstop for homes migrated before
    /// this marker existed, which is exactly the unprefixed case (the primary's
    /// rekey ran with `storage_prefix: None`).
    pub(crate) async fn resolve_boot_encoding(self) -> Result<Self, crate::storage::StorageError> {
        use crate::atom::atom_key_codec::ATOM_KEY_ENCODING_ALLOW_FLAT_ENV;

        let allow_flat = env_flag::var_truthy(ATOM_KEY_ENCODING_ALLOW_FLAT_ENV);
        self.resolve_boot_encoding_with(crate::atom::AtomKeyEncoding::from_env(), allow_flat)
            .await
    }

    /// [`Self::resolve_boot_encoding`] with the two environment reads lifted
    /// into parameters.
    ///
    /// The seam exists for the tests: `std::env::set_var` is process-global and
    /// these tests run in parallel with every other test in the crate, so a test
    /// that set `LASTDB_ATOM_KEY_ENCODING` to prove one boot's behaviour would
    /// silently change another test's store addressing. The decision under test
    /// is "given what the environment and the home each said, what does this
    /// boot do" — which is exactly this signature.
    pub(crate) async fn resolve_boot_encoding_with(
        self,
        env_override: Option<crate::atom::AtomKeyEncoding>,
        allow_flat_on_prefixed_home: bool,
    ) -> Result<Self, crate::storage::StorageError> {
        use crate::atom::{
            atom_key_codec::{
                AtomKeyEncodingMarker, ATOM_KEY_ENCODING_ALLOW_FLAT_ENV, ATOM_KEY_ENCODING_ENV,
                ATOM_KEY_ENCODING_MARKER_KEY, ATOM_PREFIX,
            },
            molecule_key_codec, AtomKeyEncoding,
        };

        let marker = self.load_atom_key_encoding_marker().await?;
        let (encoding, source) = match env_override {
            Some(encoding) => (encoding, ATOM_KEY_ENCODING_ENV),
            None => match marker {
                Some(encoding) => (encoding, ATOM_KEY_ENCODING_MARKER_KEY),
                None => (AtomKeyEncoding::default(), "default"),
            },
        };
        tracing::info!(
            ?encoding,
            source,
            marker = ?marker,
            "resolved atom body storage-key encoding"
        );

        if encoding.writes_partition_prefix() {
            if marker != Some(encoding) {
                let stamp = AtomKeyEncodingMarker {
                    version: 1,
                    encoding: encoding.as_marker_str().to_string(),
                    stamped_at_unix: crate::clock::unix_secs(),
                };
                self.main_store
                    .put_item(ATOM_KEY_ENCODING_MARKER_KEY, &stamp)
                    .await?;
                tracing::info!(
                    key = ATOM_KEY_ENCODING_MARKER_KEY,
                    encoding = encoding.as_marker_str(),
                    "stamped the atom key encoding in the home; this boot no longer \
                     depends on {ATOM_KEY_ENCODING_ENV} surviving"
                );
            }
        } else {
            // One bounded probe: does anything in this home carry a partition
            // prefix? Empty on a fresh or flat home, so it costs a prefix scan
            // that matches nothing.
            let probe = format!("{ATOM_PREFIX}{}", molecule_key_codec::MK_PREFIX);
            let prefixed = self
                .main_store
                .inner()
                .scan_prefix_paged(probe.as_bytes(), 1)
                .await?;
            if let Some((key, _)) = prefixed.first() {
                let sample = String::from_utf8_lossy(key).into_owned();
                if allow_flat_on_prefixed_home {
                    tracing::error!(
                        sample_key = %sample,
                        "{ATOM_KEY_ENCODING_ALLOW_FLAT_ENV} is set: serving a home that holds \
                         partition-prefixed atom bodies under the flat encoding. Reads will \
                         SILENTLY OMIT every prefixed-only body."
                    );
                } else {
                    return Err(crate::storage::StorageError::ConfigurationError(format!(
                        "atom key encoding mismatch: this home holds partition-prefixed atom \
                         bodies (e.g. {sample}) but the boot resolved to `flat` (source: \
                         {source}). Serving would silently omit every body that has no flat \
                         key. Set {ATOM_KEY_ENCODING_ENV}=partition_prefix (this boot will then \
                         stamp {ATOM_KEY_ENCODING_MARKER_KEY} so future boots do not need it), \
                         or set {ATOM_KEY_ENCODING_ALLOW_FLAT_ENV}=1 to accept a knowingly \
                         partial view."
                    )));
                }
            }
        }

        self.atom_keys_partition_prefixed
            .store(encoding.writes_partition_prefix(), Ordering::Release);
        self.hydrate_automatic_gc_atoms_generation().await?;
        Ok(self)
    }

    /// The encoding recorded in the home, if any. A marker that fails to decode
    /// is treated as absent (with a warn) rather than as `flat`: the boot gate
    /// below still catches a migrated home, so an unreadable marker degrades to
    /// "refuse", not to "serve short".
    async fn load_atom_key_encoding_marker(
        &self,
    ) -> Result<Option<crate::atom::AtomKeyEncoding>, crate::storage::StorageError> {
        use crate::atom::atom_key_codec::ATOM_KEY_ENCODING_MARKER_KEY;

        let raw = match self
            .main_store
            .get_item::<crate::atom::AtomKeyEncodingMarker>(ATOM_KEY_ENCODING_MARKER_KEY)
            .await
        {
            Ok(raw) => raw,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    key = ATOM_KEY_ENCODING_MARKER_KEY,
                    "atom key encoding marker did not decode; treating as absent"
                );
                None
            }
        };
        let Some(raw) = raw else { return Ok(None) };
        let parsed = crate::atom::AtomKeyEncoding::from_marker_str(&raw.encoding);
        if parsed.is_none() {
            tracing::warn!(
                encoding = %raw.encoding,
                key = ATOM_KEY_ENCODING_MARKER_KEY,
                "atom key encoding marker names an unknown encoding; treating as absent"
            );
        }
        Ok(parsed)
    }
}
