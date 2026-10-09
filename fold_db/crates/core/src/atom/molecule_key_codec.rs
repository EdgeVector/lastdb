//! Per-key molecule storage key codec.
//!
//! Background. A field's index (a `MoleculeHash` / `MoleculeRange` /
//! `MoleculeHashRange`) used to live as one JSON blob under `ref:{molecule_uuid}`,
//! so every keyed read deserialized the whole field — O(field cardinality). The
//! per-key layout stores each key as its own Sled record so a point lookup is a
//! single `get` and a range/prefix scan is `O(matches)` over Sled's ordered
//! keyspace. This module owns the (only) place that builds and parses those keys.
//!
//! Key shapes (BASE keys — callers add the optional `{storage_prefix}:` prefix via
//! `build_storage_key`, exactly as for `ref:` today):
//!
//! - Per-key record (all field kinds): `mk:{M}:{esc(hash)}\0{range}`
//!   - Hash-only slots use empty range (`…\0`); range-only use empty hash (`…:\0{range}`);
//!     Single uses the empty `("", "")` slot.
//! - HashRange page index: `mhr:{M}\0{esc(range)}\0{esc(hash)}` (range-major;
//!   the first separator pins one molecule's whole index to one partition)
//! - Header:          `mh:{M}`                      (molecule-level metadata + "migrated" marker)
//! - Tip version:     `tv:{version_id}`             (archived tip in the per-slot chain)
//! - Tip backref:     `tvr:v2:{atom_uuid}\0{version_id}` (derived atom -> archived tip lookup)
//!
//! where `M` is the deterministic molecule uuid (`sha256(schema:field)`).
//!
//! ## Why this encoding (the correctness linchpin)
//!
//! For HashRange we need three things at once:
//! 1. **Exact point key** for `HashRangeKey{hash, range}` — a single `get`.
//! 2. **Unambiguous hash prefix** so `scan_prefix(mk:{M}:{esc(hash)}\0)` returns
//!    *only* that hash's ranges — `hash="a"` must NOT match `hash="ab"`.
//! 3. **Order-preserving range** so a range/prefix scan within a hash visits
//!    ranges in the same order as the old `BTreeMap<range, _>`.
//!
//! We get (2) by byte-stuffing the hash so it can never contain the `\0`
//! separator, then using a single raw `\0` as the hash↔range delimiter. We get
//! (3) by appending the range *raw* after the separator (Rust `str` ordering ==
//! UTF-8 byte ordering == Sled key ordering). `:` is unusable as a delimiter
//! because hash/range values routinely contain it (URLs, ISO timestamps) — the
//! same trap that made the old `split_once(':')` conflict-key path lossy.
//!
//! All inputs are `String`s (always valid UTF-8); the escape only ever inserts
//! single-byte (`< 0x80`) chars, so every key produced here is valid UTF-8 and
//! survives the store's `from_utf8_lossy` read path byte-for-byte.
//!
//! ## HashKey blinding + RangeKey OPE (v1)
//!
//! Free functions in this module take **storage-form** segments (already
//! plain / blinded / OPE-encoded). API plaintext HashKey/RangeKey must go
//! through [`MoleculeKeyCodec::storage_hash`] / [`MoleculeKeyCodec::storage_range`]
//! (or the `api_*` builders) first.
//!
//! - HashKey: brain `design-lastdb-hashkey-blind-v1`
//! - RangeKey: brain `design-lastdb-rangekey-ope-v1`

use crate::atom::molecule_uuid::molecule_uuid_read_candidates;
use crate::crypto::E2eKeys;
use crate::hex::hex_lower;

/// Character length of a [`HashKeyEncoding::BlindV1`] storage hash segment:
/// 16 HMAC bytes in base64url without padding.
const BLIND_HASH_KEY_TOKEN_LEN: usize = 22;

/// How API HashKeys are encoded into storage key segments.
///
/// **Product default (env unset): [`Self::BlindV1`]** so fresh Mini installs
/// write blinded keys from first boot (keys from identity / 24-word seed).
/// Explicit `plain` remains for tests and rare unencoded homes.
/// There is **no** dual-read / mid-flight `migrating_*` mode — one encoding,
/// one path.
///
/// [`Default`] for this enum stays [`Self::Plain`] so library unit tests that
/// use `MoleculeKeyCodec::default()` / `plain()` stay unencoded without env.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashKeyEncoding {
    /// Storage hash segment == API HashKey (tests / explicit opt-out).
    #[default]
    Plain,
    /// HMAC-blinded storage segment (`hk|v1` domain). Product default via env.
    BlindV1,
}

impl HashKeyEncoding {
    /// Parse `LASTDB_HASH_KEY_ENCODING` (`plain` | `blind_v1`).
    ///
    /// - **Unset** → [`Self::BlindV1`] (fresh install: encrypt keys from start)
    /// - `plain` → [`Self::Plain`]
    /// - `blind_v1` → [`Self::BlindV1`]
    /// - Deprecated `migrating_blind_v1` → [`Self::BlindV1`] with warn
    #[must_use]
    pub fn from_env_or_default() -> Self {
        match std::env::var("LASTDB_HASH_KEY_ENCODING") {
            Ok(s) if s.eq_ignore_ascii_case("plain") => Self::Plain,
            Ok(s) if s.eq_ignore_ascii_case("blind_v1") => Self::BlindV1,
            Ok(s) if s.eq_ignore_ascii_case("migrating_blind_v1") => {
                tracing::warn!(
                    "LASTDB_HASH_KEY_ENCODING=migrating_blind_v1 is removed; using blind_v1 (no dual-read)"
                );
                Self::BlindV1
            }
            Ok(s) => {
                tracing::warn!(
                    encoding = %s,
                    "unknown LASTDB_HASH_KEY_ENCODING; using blind_v1"
                );
                Self::BlindV1
            }
            Err(_) => Self::BlindV1,
        }
    }
}

/// How API RangeKeys are encoded into storage range segments.
///
/// **Product default (env unset): [`Self::OpeV1`]** so fresh Mini installs
/// write OPE ranges from first boot. Order leakage under OPE is accepted
/// (design-lastdb-rangekey-ope-v1). Per-byte OPE **is** byte-prefix preserving
/// (`ope(prefix)` is a string prefix of `ope(prefix||suffix)`), so RangePrefix
/// / HashRangePrefix work when scan *and* in-memory apply use storage-form
/// bounds (see `expand_filter_range_bounds_for_apply`).
///
/// Explicit `plain` remains for tests. No dual-read / `migrating_ope_v1`.
///
/// [`Default`] for this enum stays [`Self::Plain`] for library `plain()` codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RangeKeyEncoding {
    /// Storage range segment == API RangeKey (tests / explicit opt-out).
    #[default]
    Plain,
    /// Order-preserving opaque encoding (`rk|ope|v1`). Product default via env.
    OpeV1,
}

impl RangeKeyEncoding {
    /// Parse `LASTDB_RANGE_KEY_ENCODING` (`plain` | `ope_v1`).
    ///
    /// - **Unset** → [`Self::OpeV1`] (fresh install: encode ranges from start)
    /// - `plain` → [`Self::Plain`]
    /// - `ope_v1` → [`Self::OpeV1`]
    /// - Deprecated `migrating_ope_v1` → [`Self::OpeV1`] with warn
    #[must_use]
    pub fn from_env_or_default() -> Self {
        match std::env::var("LASTDB_RANGE_KEY_ENCODING") {
            Ok(s) if s.eq_ignore_ascii_case("plain") => Self::Plain,
            Ok(s) if s.eq_ignore_ascii_case("ope_v1") => Self::OpeV1,
            Ok(s) if s.eq_ignore_ascii_case("migrating_ope_v1") => {
                tracing::warn!(
                    "LASTDB_RANGE_KEY_ENCODING=migrating_ope_v1 is removed; using ope_v1 (no dual-read)"
                );
                Self::OpeV1
            }
            Ok(s) => {
                tracing::warn!(
                    encoding = %s,
                    "unknown LASTDB_RANGE_KEY_ENCODING; using ope_v1"
                );
                Self::OpeV1
            }
            Err(_) => Self::OpeV1,
        }
    }

    #[must_use]
    pub const fn writes_ope(self) -> bool {
        matches!(self, Self::OpeV1)
    }
}

/// Chokepoint: API HashKey/RangeKey → storage segments.
///
/// Free `hash_range_*` builders below take **storage-form** segments.
#[derive(Debug, Clone)]
pub struct MoleculeKeyCodec {
    hash_encoding: HashKeyEncoding,
    range_encoding: RangeKeyEncoding,
    /// Required when hash encoding is blind.
    index_key: Option<[u8; 32]>,
    /// Required when range encoding is OPE.
    ope_key: Option<[u8; 32]>,
    /// Previous node key, read-only during the per-molecule key transition.
    fallback_index_key: Option<[u8; 32]>,
    /// Previous node OPE key, read-only during the per-molecule key transition.
    fallback_ope_key: Option<[u8; 32]>,
}

impl Default for MoleculeKeyCodec {
    fn default() -> Self {
        Self {
            hash_encoding: HashKeyEncoding::Plain,
            range_encoding: RangeKeyEncoding::Plain,
            index_key: None,
            ope_key: None,
            fallback_index_key: None,
            fallback_ope_key: None,
        }
    }
}

impl MoleculeKeyCodec {
    /// HashKey-only constructor (range stays plain). Kept for call-site compatibility.
    #[must_use]
    pub fn new(encoding: HashKeyEncoding, index_key: Option<[u8; 32]>) -> Self {
        Self {
            hash_encoding: encoding,
            range_encoding: RangeKeyEncoding::Plain,
            index_key,
            ope_key: None,
            fallback_index_key: None,
            fallback_ope_key: None,
        }
    }

    /// Full constructor for HashKey blind + RangeKey OPE.
    #[must_use]
    pub fn with_encodings(
        hash_encoding: HashKeyEncoding,
        range_encoding: RangeKeyEncoding,
        index_key: Option<[u8; 32]>,
        ope_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            hash_encoding,
            range_encoding,
            index_key,
            ope_key,
            fallback_index_key: None,
            fallback_ope_key: None,
        }
    }

    /// Replace write keys and retain this codec's keys as read candidates.
    #[must_use]
    pub(crate) fn with_primary_keys_and_read_fallback(
        &self,
        index_key: [u8; 32],
        ope_key: [u8; 32],
    ) -> Self {
        Self {
            hash_encoding: self.hash_encoding,
            range_encoding: self.range_encoding,
            index_key: Some(index_key),
            ope_key: Some(ope_key),
            fallback_index_key: self.index_key,
            fallback_ope_key: self.ope_key,
        }
    }

    #[must_use]
    pub fn plain() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn encoding(&self) -> HashKeyEncoding {
        self.hash_encoding
    }

    #[must_use]
    pub fn range_encoding(&self) -> RangeKeyEncoding {
        self.range_encoding
    }

    /// Map API HashKey to storage-form hash segment.
    ///
    /// Empty API hash (range-only / Single slots) is never blinded.
    pub fn storage_hash(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        if api_hash.is_empty() {
            return Ok(String::new());
        }
        match self.hash_encoding {
            HashKeyEncoding::Plain => Ok(api_hash.to_string()),
            HashKeyEncoding::BlindV1 => {
                let key = self
                    .index_key
                    .as_ref()
                    .ok_or(MoleculeKeyCodecError::MissingIndexKey)?;
                Ok(E2eKeys::blind_hash_key(key, molecule_uuid, api_hash))
            }
        }
    }

    /// Map API RangeKey to storage-form range segment.
    ///
    /// Empty range is never OPE-encoded (hash-only / Single empty slot).
    pub fn storage_range(
        &self,
        molecule_uuid: &str,
        api_range: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        if api_range.is_empty() {
            return Ok(String::new());
        }
        match self.range_encoding {
            RangeKeyEncoding::Plain => Ok(api_range.to_string()),
            RangeKeyEncoding::OpeV1 => {
                let key = self
                    .ope_key
                    .as_ref()
                    .ok_or(MoleculeKeyCodecError::MissingOpeKey)?;
                Ok(E2eKeys::ope_encode_range(key, molecule_uuid, api_range))
            }
        }
    }

    /// Storage hash forms for a read, with the current key first.
    pub fn storage_hash_read_candidates(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<Vec<String>, MoleculeKeyCodecError> {
        let primary = self.storage_hash(molecule_uuid, api_hash)?;
        let mut candidates = vec![primary.clone()];
        if self.hash_encoding == HashKeyEncoding::BlindV1 {
            if let Some(key) = self.fallback_index_key.as_ref() {
                let fallback = E2eKeys::blind_hash_key(key, molecule_uuid, api_hash);
                if fallback != primary {
                    candidates.push(fallback);
                }
            }
        }
        Ok(candidates)
    }

    /// Storage range forms for a read, with the current key first.
    pub fn storage_range_read_candidates(
        &self,
        molecule_uuid: &str,
        api_range: &str,
    ) -> Result<Vec<String>, MoleculeKeyCodecError> {
        let primary = self.storage_range(molecule_uuid, api_range)?;
        let mut candidates = vec![primary.clone()];
        if self.range_encoding == RangeKeyEncoding::OpeV1 {
            if let Some(key) = self.fallback_ope_key.as_ref() {
                let fallback = E2eKeys::ope_encode_range(key, molecule_uuid, api_range);
                if fallback != primary {
                    candidates.push(fallback);
                }
            }
        }
        Ok(candidates)
    }

    /// `mk:{M}:{esc(storage_hash)}\0{storage_range}` from API hash + range.
    ///
    /// Write path: one key under the caller's `molecule_uuid` spelling.
    pub fn api_hash_range_record_key(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let h = self.storage_hash(molecule_uuid, api_hash)?;
        let r = self.storage_range(molecule_uuid, api_range)?;
        Ok(hash_range_record_key(molecule_uuid, &h, &r))
    }

    /// Point-read keys for one API slot: current molecule-UUID spelling first,
    /// then the hex/base64url twin when `M` is a 32-byte digest. Hash and
    /// range HMAC-bind `M`, so each candidate rebuilds both segments under
    /// the same spelling — encodings are never mixed inside one key.
    pub fn api_hash_range_record_keys_for_read(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<Vec<String>, MoleculeKeyCodecError> {
        let mut keys = Vec::new();
        for uid in molecule_uuid_read_candidates(molecule_uuid) {
            for h in self.storage_hash_read_candidates(&uid, api_hash)? {
                for r in self.storage_range_read_candidates(&uid, api_range)? {
                    let key = hash_range_record_key(&uid, &h, &r);
                    if !keys.iter().any(|k| k == &key) {
                        keys.push(key);
                    }
                }
            }
        }
        Ok(keys)
    }

    /// Prefix-scan keys for one API hash, dual-read across molecule-UUID
    /// encodings. Same HMAC-bind rule as
    /// [`Self::api_hash_range_record_keys_for_read`].
    pub fn api_hash_range_scan_prefixes_for_read(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<Vec<String>, MoleculeKeyCodecError> {
        let mut prefixes = Vec::new();
        for uid in molecule_uuid_read_candidates(molecule_uuid) {
            for h in self.storage_hash_read_candidates(&uid, api_hash)? {
                let prefix = hash_range_scan_prefix_for_hash(&uid, &h);
                if !prefixes.iter().any(|p| p == &prefix) {
                    prefixes.push(prefix);
                }
            }
        }
        Ok(prefixes)
    }

    /// `mk:{M}:` prefixes covering every per-key record of this molecule
    /// under every readable UUID spelling.
    #[must_use]
    pub fn molecule_record_prefixes_for_read(&self, molecule_uuid: &str) -> Vec<String> {
        molecule_uuid_read_candidates(molecule_uuid)
            .into_iter()
            .map(|uid| molecule_record_prefix(&uid))
            .collect()
    }

    /// `mk:{M}:{esc(storage_hash(api_hash))}\0` scan prefix from API hash.
    pub fn api_hash_range_scan_prefix_for_hash(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let h = self.storage_hash(molecule_uuid, api_hash)?;
        Ok(hash_range_scan_prefix_for_hash(molecule_uuid, &h))
    }

    /// `mhk:{M}:{esc(storage_hash(api_hash))}` from API hash.
    pub fn api_hash_key_lookup_key(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let h = self.storage_hash(molecule_uuid, api_hash)?;
        Ok(hash_key_lookup_key(molecule_uuid, &h))
    }

    /// True when `segment` has the **shape** this codec's [`Self::storage_hash`]
    /// produces for a non-empty API hash.
    ///
    /// Shape-only, and deliberately so: it exists to spot `mk:` hash segments
    /// left behind in an OLDER encoding, so that a caller can ask whether the
    /// current-encoding twin also exists. A blind token is HMAC-SHA256 → first
    /// 16 bytes → base64url no pad, which is exactly 22 characters over
    /// `[A-Za-z0-9_-]` (see [`E2eKeys::blind_hash_key`]).
    ///
    /// A plaintext key that happens to match that shape is reported as
    /// storage-form and simply never gets the twin test. That direction is the
    /// safe one: it leaves the row exactly as today's code treats it rather
    /// than dropping something on a guess.
    #[must_use]
    pub fn looks_like_storage_hash(&self, segment: &str) -> bool {
        match self.hash_encoding {
            // Plain writes the API hash verbatim, so every segment is current form.
            HashKeyEncoding::Plain => true,
            HashKeyEncoding::BlindV1 => {
                segment.len() == BLIND_HASH_KEY_TOKEN_LEN
                    && segment
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            }
        }
    }

    /// `mhr:{M}:{esc(storage_range)}\0{esc(storage_hash)}`.
    pub fn api_hash_range_page_index_key(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let h = self.storage_hash(molecule_uuid, api_hash)?;
        let r = self.storage_range(molecule_uuid, api_range)?;
        Ok(hash_range_page_index_key(molecule_uuid, &h, &r))
    }
}

/// Errors from [`MoleculeKeyCodec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoleculeKeyCodecError {
    /// Blind encoding requested but no E2E index key is configured.
    MissingIndexKey,
    /// OPE encoding requested but no E2E ope key is configured.
    MissingOpeKey,
}

impl std::fmt::Display for MoleculeKeyCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingIndexKey => write!(
                f,
                "hash_key_encoding requires E2E index_key (set encoding=plain or provide index key)"
            ),
            Self::MissingOpeKey => write!(
                f,
                "range_key_encoding requires E2E ope_key (set encoding=plain or provide ope key)"
            ),
        }
    }
}

impl std::error::Error for MoleculeKeyCodecError {}

/// Sled key prefix for a per-key molecule record.
pub const MK_PREFIX: &str = "mk:";
/// Sled key prefix for a molecule header record.
pub const MH_PREFIX: &str = "mh:";
/// Active molecule-generation pointer. The pointer is the only mutable row in
/// a generation activation; generation bodies are immutable.
pub const MOLECULE_GENERATION_POINTER_PREFIX: &str = "mgp:v1:";
/// Immutable molecule-generation body rows.
pub const MOLECULE_GENERATION_RECORD_PREFIX: &str = "mgr:v1:";
/// Sparse post-generation slot deletions.
pub const MOLECULE_GENERATION_DELETE_PREFIX: &str = "mgd:v1:";
/// Sled key prefix for one entry of a molecule's append-only `update_order`
/// log (HashRange only). Each entry is `mord:{M}:{seq}`; see [`order_entry_key`].
pub const MORD_PREFIX: &str = "mord:";
/// Sled key prefix for a molecule's append-only `update_order` count record
/// (HashRange only) — the number of persisted `mord:{M}:*` entries. See
/// [`order_count_key`].
pub const MOC_PREFIX: &str = "moc:";
/// Sled key prefix for the range-major secondary index over HashRange molecule
/// records. The authoritative record remains `mk:{M}:{esc(hash)}\0{range}`;
/// `mhr:{M}\0{esc(range)}\0{esc(hash)}` is a tiny derived marker used only to
/// page a whole HashRange field in `(range, hash)` order without scanning all
/// hash-major keys.
const MHR_PREFIX: &str = "mhr:";
/// Sled key prefix for the hash-major uniqueness marker over HashRange molecule
/// records. `mhk:{M}:{esc(hash)}` stores a derived fast-path record only when a
/// hash currently maps to one range; ambiguous hashes carry an ambiguous marker
/// and fall back to the authoritative `mk:` prefix scan.
const MHK_PREFIX: &str = "mhk:";
/// Sled key prefix for the completion marker that says the range-major
/// HashRange page index has been built for molecule `M`.
const MHI_PREFIX: &str = "mhi:";
/// Version tag for the completion marker, bumped whenever the page-index key
/// shape changes so an already-indexed home rebuilds instead of trusting a
/// marker that describes rows the live prefix can no longer see.
///
/// It sits *inside* [`MHI_PREFIX`] (`mhi:v2:{M}`) rather than replacing it
/// (`mhi2:{M}`) on purpose: `MAIN_KEY_PREFIX_COLLECTIONS` and the mini-cutover
/// classifier both route this row to `field_hashrange_complete` by matching the
/// literal `"mhi:"`, so a sibling prefix would quietly land the marker in the
/// default collection. Molecule uuids are 43-char base64url (or legacy 64-char
/// hex) and never collide with the `v2:` tag.
const MHI_VERSION_TAG: &str = "v2:";

/// Width of the zero-padded sequence in an `mord:{M}:{seq}` key. 16 decimal
/// digits hold up to ~10^16 entries — far beyond any realistic field
/// cardinality — and the fixed width makes the raw Sled key order identical to
/// the numeric insertion order (so a prefix scan reassembles `update_order` in
/// order without parsing the seq).
const ORDER_SEQ_WIDTH: usize = 16;

const SEP: char = '\u{0}'; // hash↔range separator (never appears in esc(hash))
const ESC: char = '\u{1}'; // escape byte for the hash segment

/// The separator, exported for keys built *outside* this module that must land
/// in the same LastStore partition as a molecule's per-key records.
///
/// LastStore's `HashGroupKey::PartitionPrefix` placement treats everything up to
/// and including the first occurrence of this byte as the partition. That is why
/// the hash segment is byte-stuffed: it makes "the first separator" an
/// unambiguous boundary rather than a guess. [`crate::atom::atom_key_codec`]
/// relies on both properties to give an atom body the same partition as its tip.
pub const PARTITION_SEP: char = SEP;

/// Byte-stuff a hash so it contains no `\0` (and no bare `\u{1}`), making the
/// `\0` separator and the escape unambiguous. `\0 -> \x01\x01`, `\x01 -> \x01\x02`.
fn escape_segment(segment: &str) -> String {
    // Fast path: the overwhelmingly common case has neither control byte.
    if !segment.contains(SEP) && !segment.contains(ESC) {
        return segment.to_string();
    }
    let mut out = String::with_capacity(segment.len() + 2);
    for c in segment.chars() {
        match c {
            SEP => {
                out.push(ESC);
                out.push('\u{1}');
            }
            ESC => {
                out.push(ESC);
                out.push('\u{2}');
            }
            other => out.push(other),
        }
    }
    out
}

/// Inverse of [`escape_segment`]. Returns `None` on a malformed escape sequence.
fn unescape_segment(escaped: &str) -> Option<String> {
    if !escaped.contains(ESC) {
        return Some(escaped.to_string());
    }
    let mut out = String::with_capacity(escaped.len());
    let mut chars = escaped.chars();
    while let Some(c) = chars.next() {
        if c == ESC {
            match chars.next() {
                Some('\u{1}') => out.push(SEP),
                Some('\u{2}') => out.push(ESC),
                _ => return None,
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

/// `mk:{M}:` — prefix covering every per-key record of molecule `M`
/// (full-field materialize / iterate / purge).
#[must_use]
pub fn molecule_record_prefix(molecule_uuid: &str) -> String {
    format!("{MK_PREFIX}{molecule_uuid}:")
}

/// `mh:{M}` — molecule header record key.
#[must_use]
pub fn header_key(molecule_uuid: &str) -> String {
    format!("{MH_PREFIX}{molecule_uuid}")
}

/// `mgp:v1:{M}` — the selected immutable base generation for one molecule.
#[must_use]
pub fn molecule_generation_pointer_key(molecule_uuid: &str) -> String {
    format!("{MOLECULE_GENERATION_POINTER_PREFIX}{molecule_uuid}")
}

/// `mgr:v1:{M}:{generation}:` — every immutable slot in one generation.
#[must_use]
pub fn molecule_generation_record_prefix(molecule_uuid: &str, generation: &str) -> String {
    format!("{MOLECULE_GENERATION_RECORD_PREFIX}{molecule_uuid}:{generation}:")
}

/// One generation slot. The suffix matches `mk:{M}:` byte-for-byte, so range
/// order stays identical between the selected base and the live change rows.
#[must_use]
pub fn molecule_generation_record_key(
    molecule_uuid: &str,
    generation: &str,
    hash: &str,
    range: &str,
) -> String {
    format!(
        "{}{}{SEP}{range}",
        molecule_generation_record_prefix(molecule_uuid, generation),
        escape_segment(hash)
    )
}

/// `mgd:v1:{M}:` — sparse deletions after any generation cut.
#[must_use]
pub fn molecule_generation_delete_prefix(molecule_uuid: &str) -> String {
    format!("{MOLECULE_GENERATION_DELETE_PREFIX}{molecule_uuid}:")
}

/// One sparse delete marker. Its suffix matches the corresponding `mk:` key.
#[must_use]
pub fn molecule_generation_delete_key(molecule_uuid: &str, hash: &str, range: &str) -> String {
    format!(
        "{}{}{SEP}{range}",
        molecule_generation_delete_prefix(molecule_uuid),
        escape_segment(hash)
    )
}

/// Convert an `mk:` key or bound into the sparse-delete plane.
#[must_use]
pub fn molecule_generation_delete_bound_for_record_bound(
    molecule_uuid: &str,
    record_bound: &str,
) -> Option<String> {
    let suffix = record_bound.strip_prefix(&molecule_record_prefix(molecule_uuid))?;
    Some(format!(
        "{}{suffix}",
        molecule_generation_delete_prefix(molecule_uuid)
    ))
}

/// Decode a sparse-delete row back to its ordinary `mk:` key.
#[must_use]
pub fn molecule_record_key_from_generation_delete_key(
    molecule_uuid: &str,
    delete_key: &str,
) -> Option<String> {
    let suffix = delete_key.strip_prefix(&molecule_generation_delete_prefix(molecule_uuid))?;
    Some(format!("{}{suffix}", molecule_record_prefix(molecule_uuid)))
}

/// Convert one `mk:` key or prefix into the same bound in a generation.
#[must_use]
pub fn molecule_generation_bound_for_record_bound(
    molecule_uuid: &str,
    generation: &str,
    record_bound: &str,
) -> Option<String> {
    let suffix = record_bound.strip_prefix(&molecule_record_prefix(molecule_uuid))?;
    Some(format!(
        "{}{suffix}",
        molecule_generation_record_prefix(molecule_uuid, generation)
    ))
}

/// Decode a generation row back to the ordinary `mk:` key used by callers.
#[must_use]
pub fn molecule_record_key_from_generation_key(
    molecule_uuid: &str,
    generation: &str,
    generation_key: &str,
) -> Option<String> {
    let suffix = generation_key.strip_prefix(&molecule_generation_record_prefix(
        molecule_uuid,
        generation,
    ))?;
    Some(format!("{}{suffix}", molecule_record_prefix(molecule_uuid)))
}

mod tip_keys;
pub use tip_keys::*;
mod atom_ref_keys;
pub use atom_ref_keys::*;
mod order_keys;
pub use order_keys::*;
