//! Atom **storage key** codec — the one place that builds and parses the key an
//! atom body is stored under.
//!
//! ## Why this module exists
//!
//! A schema read is two passes over two collections:
//!
//! 1. walk the tip plane (`tips`; `field_tips` dual-read was pruned after the
//!    2026-07-31 zero-hit soak) for the partition's per-key records, keyed
//!    `mk:{M}:{esc(hash)}\0{range}`;
//! 2. fetch each row's body from `atoms`.
//!
//! Pass 1 is physically bounded: the tip id carries a `\0`, so under
//! `HashGroupKey::PartitionPrefix` (the primary's layout since 2026-07-27)
//! LastStore places every row of a partition in the same
//! `hash_group_partition_fanout` groups. Pass 2 was not: the body key was
//! `atom:{uuid}`, which carries no `\0`, so `partition_of` returned the whole id
//! and placement fell back to full-key hashing — a partition's N bodies
//! scattered across *every* group of the largest collection in the store.
//! Measured in `vendor/laststore/tests/hash_group_atom_body_locality.rs`: a
//! 128-row partition over 256 groups touched 108 atom groups instead of 16, and
//! the scatter grows with partition size until it saturates the collection.
//!
//! [`AtomKeyEncoding::PartitionPrefix`] gives the body key the same partition
//! prefix its tip already carries, so both passes resolve the same bounded group
//! set.
//!
//! ## Identity is unchanged
//!
//! The atom UUID remains the whole identity: it is content-addressed, it is what
//! a tip stores (`AtomEntry::atom_uuid`), and it is what the API surfaces. Only
//! the *storage key* gains a locality prefix. Schema → Molecule → Atom → file
//! blob is untouched.
//!
//! ## Key shapes (BASE keys — callers add `{storage_prefix}:` via
//! [`crate::schema::types::field::build_storage_key`])
//!
//! | encoding | key |
//! |---|---|
//! | [`AtomKeyEncoding::Flat`] | `atom:{uuid}` |
//! | [`AtomKeyEncoding::PartitionPrefix`], partition known | `atom:mk:{M}:{esc(hash)}\0{uuid}` |
//! | [`AtomKeyEncoding::PartitionPrefix`], partition unknown | `atom:{uuid}` |
//!
//! Both shapes start with `atom:`, so `MAIN_KEY_PREFIX_COLLECTIONS` routing and
//! every `atom:` prefix scan keep working unchanged.
//!
//! The unknown-partition case is a **defined placement, not a panic**: atoms
//! reached without their owning slot (GC candidates, orphan sweeps, the uuid-only
//! `GET /api/atom/{uuid}` surface) resolve in the flat namespace rather than
//! forcing every caller to invent a partition.
//!
//! ## Status
//!
//! [`AtomKeyEncoding::Flat`] is the default for a **fresh** home. It is no
//! longer "the only encoding any shipped home uses": the primary has run under
//! `PartitionPrefix` since 2026-07-27, and its rekey finished with 949,863
//! bodies carrying **only** a prefixed key. `PartitionPrefix` is safe to enable
//! on a new empty home, and on a populated home only after
//! [`crate::db_operations::AtomStore::rekey_atoms_to_partition_prefix`] dual-writes
//! every tip-referenced body (locator + prefixed key). Live primary flip is
//! Tom-gated (`lastdb-safe-upgrade`). See brain
//! `design-lastdb-atom-key-partition-locality`.
//!
//! ## How a boot picks the encoding
//!
//! Config may *start* a migration; it must not be the only record that the
//! migration happened. The encoding a home is written under is therefore
//! persisted **in the home** at [`ATOM_KEY_ENCODING_MARKER_KEY`], and a boot
//! resolves it as:
//!
//! 1. [`ATOM_KEY_ENCODING_ENV`], when set — an explicit operator override;
//! 2. else the home marker;
//! 3. else [`AtomKeyEncoding::Flat`].
//!
//! Before this existed the env var was the *only* input, and dropping it from a
//! migrated home did not fail — it silently served short pages, because
//! [`crate::db_operations::AtomStore::get_atoms_located`] returns after step 1
//! under `Flat` and never reaches the flat fallback or the `aloc:` backstop. A
//! CoW clone of the primary booted without the var resolved 220 of 411 rows and
//! reported 191 unresolved; 8 of the 13 `lastdbd-primary` plist backups on disk
//! at the time carried no such var, including the one you would restore to roll
//! back the cutover. Brain:
//! `lastdb-atom-encoding-is-env-only-a-flat-boot-silently-hides-migrated-bodies`.
//!
//! A `Flat` boot against a home that still holds prefixed keys is refused
//! outright — see
//! [`crate::db_operations::AtomStore::resolve_boot_encoding`].

use crate::atom::molecule_key_codec;

/// Base prefix every atom body key starts with, under either encoding.
pub const ATOM_PREFIX: &str = "atom:";

/// Env var selecting the atom storage-key encoding.
pub const ATOM_KEY_ENCODING_ENV: &str = "LASTDB_ATOM_KEY_ENCODING";

/// Durable record, in the home, of the encoding this store's atom bodies are
/// written under. A sibling of [`crate::db_operations::ATOM_PARTITION_REKEY_CHECKPOINT_KEY`]
/// in the `main` namespace, stamped on every boot that resolves to
/// [`AtomKeyEncoding::PartitionPrefix`].
///
/// Deliberately store-wide rather than per-`storage_prefix`: the encoding is one
/// property of the [`crate::db_operations::AtomStore`], not of an org scope.
pub const ATOM_KEY_ENCODING_MARKER_KEY: &str = "amigr:atom_key_encoding_v1";

/// Escape hatch for the boot gate: set to `1`/`true` to serve a home that holds
/// prefixed keys under [`AtomKeyEncoding::Flat`] anyway.
///
/// Exists so a deliberate diagnostic (or a rollback that genuinely wants the
/// flat view) stays possible, while the *accidental* env-less boot — the case
/// that silently truncated reads — fails loudly. Serving short pages is wrong
/// however `Flat` was arrived at, so this override is required even when the
/// operator spelled `flat` out explicitly.
pub const ATOM_KEY_ENCODING_ALLOW_FLAT_ENV: &str = "LASTDB_ALLOW_FLAT_ON_PREFIXED_HOME";

/// Marker value stored at [`ATOM_KEY_ENCODING_MARKER_KEY`].
///
/// A struct rather than a bare string so the record can grow (and so a future
/// reader can tell a marker it does not understand from one that says `flat`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtomKeyEncodingMarker {
    pub version: u32,
    /// [`AtomKeyEncoding::as_marker_str`].
    pub encoding: String,
    /// Unix seconds when this home was first stamped with `encoding`.
    #[serde(default)]
    pub stamped_at_unix: u64,
}

/// How an atom body's storage key is built.
///
/// [`Default`] is [`Self::Flat`] — the shipped shape. Unlike
/// [`crate::atom::HashKeyEncoding`] / [`crate::atom::RangeKeyEncoding`], whose
/// defaults flipped to the encoded form for fresh installs, this one stays flat
/// until the migration exists: changing it rewrites where existing bodies live,
/// and a home that half-flipped would read `None` for every unmigrated row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AtomKeyEncoding {
    /// `atom:{uuid}` — no locality prefix. Every shipped home.
    #[default]
    Flat,
    /// `atom:{partition}{uuid}` — body co-located with its partition's tips.
    PartitionPrefix,
}

impl AtomKeyEncoding {
    /// Parse a `flat` | `partition_prefix` spelling (case-insensitive).
    #[must_use]
    pub fn from_marker_str(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("flat") {
            Some(Self::Flat)
        } else if s.eq_ignore_ascii_case("partition_prefix") {
            Some(Self::PartitionPrefix)
        } else {
            None
        }
    }

    /// The canonical spelling, as stored in [`ATOM_KEY_ENCODING_MARKER_KEY`] and
    /// accepted by [`ATOM_KEY_ENCODING_ENV`].
    #[must_use]
    pub const fn as_marker_str(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::PartitionPrefix => "partition_prefix",
        }
    }

    /// The explicit operator override from [`ATOM_KEY_ENCODING_ENV`], if any.
    ///
    /// `None` means "the environment said nothing" — the caller falls through to
    /// the home marker. An *unknown* value is also `None`, with a warn: a
    /// typo must not be read as a decision, and falling through to the marker
    /// keeps a migrated home readable where the old
    /// silently-coerce-to-`Flat` behaviour truncated it.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        match std::env::var(ATOM_KEY_ENCODING_ENV) {
            Ok(s) => Self::from_marker_str(&s).or_else(|| {
                tracing::warn!(
                    encoding = %s,
                    "unknown {ATOM_KEY_ENCODING_ENV}; falling through to the home marker"
                );
                None
            }),
            Err(_) => None,
        }
    }

    /// [`Self::from_env`] with the bare default, for call sites with no home to
    /// consult (test constructors, and the pre-resolution seed in
    /// [`crate::db_operations::AtomStore`], which
    /// [`crate::db_operations::AtomStore::resolve_boot_encoding`] then corrects
    /// against the home).
    #[must_use]
    pub fn from_env_or_default() -> Self {
        Self::from_env().unwrap_or_default()
    }

    /// Whether this encoding co-locates bodies with their partition's tips.
    #[must_use]
    pub const fn writes_partition_prefix(self) -> bool {
        matches!(self, Self::PartitionPrefix)
    }
}

/// The partition an atom body belongs to: the `mk:{M}:{esc(hash)}\0` prefix its
/// tip already lives under.
///
/// Constructed from **storage-form** segments (already blinded / OPE-encoded),
/// exactly like the free `molecule_key_codec::hash_range_*` builders — the
/// prefix must be byte-identical to the one the tip walk uses, or the body would
/// land in a different group than the tips it is supposed to accompany.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomPartition(String);

impl AtomPartition {
    /// The partition of the slot `(molecule_uuid, storage_hash)`.
    ///
    /// `storage_hash` is the storage-form hash segment. A range-keyed or Single
    /// field passes its empty hash, which still yields a well-formed
    /// `mk:{M}:\0` partition — every row of such a field then shares one
    /// partition, which is exactly the locality its tip walk already has.
    #[must_use]
    pub fn for_slot(molecule_uuid: &str, storage_hash: &str) -> Self {
        Self(molecule_key_codec::hash_range_scan_prefix_for_hash(
            molecule_uuid,
            storage_hash,
        ))
    }

    /// Molecule identity encoded in this partition prefix.
    #[must_use]
    pub fn molecule_uuid(&self) -> Option<&str> {
        let rest = self.0.strip_prefix(molecule_key_codec::MK_PREFIX)?;
        let (molecule_uuid, _) = rest.split_once(':')?;
        (!molecule_uuid.is_empty()).then_some(molecule_uuid)
    }

    /// The partition of the slot addressed by `(schema_name, field_name,
    /// storage_hash)`.
    ///
    /// The molecule uuid is `sha256(schema:field)`, so a writer that knows which
    /// field it is writing can name the partition without loading the molecule.
    #[must_use]
    pub fn for_field_slot(schema_name: &str, field_name: &str, storage_hash: &str) -> Self {
        Self::for_slot(
            &crate::atom::deterministic_molecule_uuid(schema_name, field_name),
            storage_hash,
        )
    }

    /// The partition of the per-key record stored at `record_key`.
    ///
    /// Takes the partition straight off the tip's **own** storage key —
    /// everything up to and including the first [`molecule_key_codec::PARTITION_SEP`],
    /// which is exactly what LastStore's `HashGroupKey::PartitionPrefix`
    /// placement reads. Prefer this over [`Self::for_slot`] wherever the record
    /// key is in scope: it cannot disagree with the tip, whereas recomputing
    /// from `(molecule_uuid, hash)` can if the caller holds the API-form hash
    /// rather than the storage-form one.
    ///
    /// `record_key` is a BASE key (`mk:…`, no `{storage_prefix}:`). Returns
    /// `None` for anything that is not a per-key record key — an unknown
    /// partition, which callers resolve in the flat namespace.
    #[must_use]
    pub fn from_record_key(record_key: &str) -> Option<Self> {
        if !record_key.starts_with(molecule_key_codec::MK_PREFIX) {
            return None;
        }
        let end = record_key.find(molecule_key_codec::PARTITION_SEP)?;
        Some(Self(record_key[..=end].to_string()))
    }

    /// Rebuild a partition from its stored string form (a locator value).
    ///
    /// Validates the shape rather than trusting it: a partition is
    /// `mk:{M}:{esc(hash)}\0` — `mk:`-prefixed, separator-terminated, and
    /// carrying exactly one separator (the hash segment is byte-stuffed, so a
    /// second one means the value is not a partition prefix). Anything else is
    /// `None`, which degrades to the flat key.
    #[must_use]
    pub fn from_prefix(prefix: &str) -> Option<Self> {
        let sep = molecule_key_codec::PARTITION_SEP;
        if !prefix.starts_with(molecule_key_codec::MK_PREFIX)
            || !prefix.ends_with(sep)
            || prefix.matches(sep).count() != 1
        {
            return None;
        }
        Some(Self(prefix.to_string()))
    }

    /// The prefix, separator included.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The unpartitioned body key `atom\0{uuid}` — kind-as-partition write form.
/// Dual-read still resolves the colon form `atom:{uuid}`.
#[must_use]
pub fn flat_key(atom_uuid: &str) -> String {
    crate::kind_partition::anchored("atom", atom_uuid)
}

/// Build the body key for `atom_uuid` under `encoding`.
///
/// `partition` is the owning slot when the caller knows it. Under
/// [`AtomKeyEncoding::Flat`] it is ignored; under
/// [`AtomKeyEncoding::PartitionPrefix`] `None` falls back to [`flat_key`].
#[must_use]
pub fn storage_key(
    encoding: AtomKeyEncoding,
    partition: Option<&AtomPartition>,
    atom_uuid: &str,
) -> String {
    match (encoding, partition) {
        (AtomKeyEncoding::PartitionPrefix, Some(p)) => {
            format!("{ATOM_PREFIX}{}{atom_uuid}", p.as_str())
        }
        _ => flat_key(atom_uuid),
    }
}

/// The atom UUID inside a body key, under either encoding.
///
/// Accepts a BASE key (`atom:…`). Returns `None` for a key that is not an atom
/// body key. The uuid is everything after the last partition separator, or after
/// `atom:` when the key carries no separator — which is why the separator must
/// never appear inside `esc(hash)` (the molecule codec byte-stuffs it) and never
/// inside a uuid (they are hex + dashes).
#[must_use]
pub fn uuid_of(base_key: &str) -> Option<&str> {
    Some(uuid_of_suffix(crate::kind_partition::rest_of(
        base_key, "atom",
    )?))
}

/// The partition embedded in a partition-prefixed atom body key.
///
/// This is the inverse needed by cloud replay: a locator row is derived from
/// the atom key itself, so it does not need to ride the mutation log. Flat atom
/// keys return `None` because they need no locator.
#[must_use]
pub fn partition_of(base_key: &str) -> Option<AtomPartition> {
    let suffix = crate::kind_partition::rest_of(base_key, "atom")?;
    let separator = molecule_key_codec::PARTITION_SEP;
    let end = suffix.rfind(separator)? + separator.len_utf8();
    AtomPartition::from_prefix(&suffix[..end])
}

/// [`uuid_of`] for a key whose `atom:` prefix has already been stripped.
///
/// Exists for scan loops that build their scan prefix with
/// [`crate::schema::types::field::build_storage_key`] and therefore hold
/// `{storage_prefix}:atom:` as one opaque string: they can `strip_prefix` that
/// and pass the remainder here, instead of re-concatenating `atom:` onto every
/// row just to satisfy [`uuid_of`]. On a store with millions of atoms that
/// allocation is the difference between a scan and a scan plus a heap churn.
///
/// Total, not fallible: a suffix with no separator *is* a flat uuid.
#[must_use]
pub fn uuid_of_suffix(suffix: &str) -> &str {
    match suffix.rsplit_once(molecule_key_codec::PARTITION_SEP) {
        Some((_, uuid)) => uuid,
        None => suffix,
    }
}
