use super::*;

/// `mord:{M}:{seq}` — one entry of the append-only `update_order` log.
///
/// `update_order` is **append-only**: a write that touches K keys pushes K new
/// entries to the tail; entries `[0..L)` already on disk are never reordered or
/// removed. Storing each entry as its own record (seq = its index in the
/// vector, zero-padded to [`ORDER_SEQ_WIDTH`]) lets a write persist just the new
/// tail — O(K) puts instead of an O(N) full-Vec rewrite. The fixed-width seq
/// makes the raw Sled key order match insertion order, so a prefix scan over
/// [`order_log_prefix`] reassembles the vector in order.
#[must_use]
pub fn order_entry_key(molecule_uuid: &str, seq: usize) -> String {
    let width = ORDER_SEQ_WIDTH;
    crate::kind_partition::anchored("mord", &format!("{molecule_uuid}:{seq:0width$}"))
}

/// Settled retention window for `mord:` / `moc:` (Tom, 2026-08-26): keep 30
/// days, and drop every entry for a molecule with zero live `mk:` rows.
pub const ORDER_LOG_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;

/// Settled retention window for superseded versions of **live** records
/// (`tv:` rows + tip-chain history). Tom, 2026-08-26:
/// `decision-2026-08-26-retention-windows-order-log-30d-versions-7d-fleet-ttl`.
///
/// Deleted-record semantics are untouched: no timer, no retention window.
pub const SUPERSEDED_VERSION_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// `mord:{M}\0{nanos}:{writer}:{offset}` — one sparse append-log entry.
///
/// The partition separator pins one molecule's sparse log to its own LastStore
/// partition, so reassembly is a targeted prefix read rather than a collection
/// sweep. `nanos + writer + offset` is unique without reading or updating a
/// shared counter; a failed batch therefore leaves no hole that a reader can
/// mistake for end-of-log.
#[must_use]
pub fn sparse_order_entry_key(
    molecule_uuid: &str,
    nanos: u64,
    writer: &str,
    offset: usize,
) -> String {
    format!("{MORD_PREFIX}{molecule_uuid}{SEP}{nanos:020}:{writer}:{offset:016}")
}

/// Nanos stamped in a sparse `mord:` key, when the key matches the layout.
///
/// Accepts an optional storage-prefix in front of `mord:`. Dense
/// `mord:{M}:{seq}` keys return `None`.
#[must_use]
pub fn sparse_order_entry_nanos(storage_key: &str) -> Option<u64> {
    let mord = storage_key.find(MORD_PREFIX)?;
    let rest = &storage_key[mord + MORD_PREFIX.len()..];
    let (_molecule, stamped) = rest.split_once(SEP)?;
    let nanos = stamped.get(..20)?;
    if !nanos.as_bytes().iter().all(u8::is_ascii_digit) {
        return None;
    }
    nanos.parse().ok()
}

/// `mord:{M}\0` — partition-pinned prefix for sparse append-log entries.
#[must_use]
pub fn sparse_order_log_prefix(molecule_uuid: &str) -> String {
    format!("{MORD_PREFIX}{molecule_uuid}{SEP}")
}

/// `mord:{M}:` — prefix covering every append-log entry of molecule `M` (full
/// `update_order` reassembly on the `SampleN` / no-filter materialize path).
#[must_use]
pub fn order_log_prefix(molecule_uuid: &str) -> String {
    crate::kind_partition::anchored("mord", &format!("{molecule_uuid}:"))
}

/// `moc:{M}` — molecule `update_order` append-log count record (the number of
/// persisted `mord:{M}:*` entries).
///
/// Read O(1) by the write path so it can append only `update_order[count..]`
/// (the new tail) without scanning the whole log. Written in the same atomic
/// batch as the new entries so the count and the log never diverge.
#[must_use]
pub fn order_count_key(molecule_uuid: &str) -> String {
    crate::kind_partition::anchored("moc", molecule_uuid)
}

/// `mhr:{M}\0` — prefix covering the range-major page-index marker rows for one
/// HashRange molecule.
///
/// The separator sits directly after `{M}` **on purpose**. LastStore's
/// `HashGroupKey::PartitionPrefix` placement partitions on everything up to and
/// including the first separator, so this shape makes one molecule's entire
/// page index a single partition: a molecule-wide walk resolves that
/// partition's `fanout` groups instead of enumerating every group in the
/// collection (1024 on the primary).
///
/// That is the right trade *for this index specifically*. `mhr:` exists to be
/// walked molecule-wide in `(range, hash)` order — it is never read one range
/// at a time — so co-locating it turns its only access pattern from a full
/// sweep into a pinned read. Contrast `mk:`, which stays partitioned per hash
/// because its hot shape is a single-hash read and co-locating a whole molecule
/// there would build one oversized group per field.
#[must_use]
pub fn hash_range_page_index_prefix(molecule_uuid: &str) -> String {
    format!("{MHR_PREFIX}{molecule_uuid}{SEP}")
}

/// `mhr:{M}:` — the pre-partition-pinned page-index prefix.
///
/// Rows under it are dead once the index has been rebuilt under
/// [`hash_range_page_index_prefix`], but a home written before that change
/// still holds them and nothing else would ever reap them: the live prefix no
/// longer matches. Rebuild and delete sweep both prefixes so the migration
/// reclaims the old rows rather than leaking them.
#[must_use]
pub fn legacy_hash_range_page_index_prefix(molecule_uuid: &str) -> String {
    format!("{MHR_PREFIX}{molecule_uuid}:")
}

/// `mhk:{M}:` — prefix covering hash-key uniqueness markers for one molecule.
#[must_use]
pub fn hash_key_lookup_prefix(molecule_uuid: &str) -> String {
    format!("{MHK_PREFIX}{molecule_uuid}:")
}

/// `mhk:{M}:{esc(hash)}` — derived marker for fast `HashKey(hash)` reads.
#[must_use]
pub fn hash_key_lookup_key(molecule_uuid: &str, hash: &str) -> String {
    format!("{MHK_PREFIX}{molecule_uuid}:{}", escape_segment(hash))
}

/// Every molecule-scoped key prefix, longest-first so `mhr:`/`mhk:` are tested
/// before the shorter `mh:` they share two characters with.
const MOLECULE_KEY_PREFIXES: &[&str] = &[
    MORD_PREFIX,
    MHR_PREFIX,
    MHK_PREFIX,
    MOC_PREFIX,
    MK_PREFIX,
    MH_PREFIX,
];

/// Which part of a molecule's storage counter one written row feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoleculeCounterRow {
    /// A current `mk:` tip record (`tip_index_bytes`).
    Tip,
    /// A header, order-log, order-count, page-index, or hash-key lookup row
    /// (`molecule_metadata_bytes`).
    Structure,
}

/// Classify a storage key for the keep-small molecule counter, and read its
/// molecule uuid.
///
/// This is the write-path twin of the row set the bootstrap measurer reads
/// (`measure_molecule_counter`: tips, `mh:`, `moc`, the dense and sparse
/// `mord` logs, `mhr:` and `mhk:`). Both must agree or the two paths report
/// different `schema_structure_bytes` for the same molecule. They did not:
/// the write path matched the substrings `"mord:"` and `"moc:"`, which miss
/// the kind-anchored write forms `mord\0{M}:{seq}` and `moc\0{M}`
/// ([`crate::kind_partition::anchored`]). On a one-record Hash fixture that
/// dropped 26 bytes per molecule (a 25-byte order entry and a 1-byte count),
/// 1056 metered against 1108 measured
/// (papercut-keep-small-incremental-vs-bootstrap-measure-drift-52-bytes-20260921).
///
/// Accepts both the anchored and the flat form, with or without a
/// `{storage_prefix}:` head. Returns `None` for rows the counter does not
/// meter (atoms, `mhi:` completion markers, `tv`, `aref`, generation rows).
#[must_use]
pub fn molecule_counter_row(key: &str) -> Option<(MoleculeCounterRow, &str)> {
    // `kind_partition::anchored("mord" | "moc", ..)` heads; the test below
    // pins them to the real builders.
    const ANCHORED_MORD: &str = "mord\u{0}";
    const ANCHORED_MOC: &str = "moc\u{0}";
    let candidates: [(&str, MoleculeCounterRow); 8] = [
        (MORD_PREFIX, MoleculeCounterRow::Structure),
        (ANCHORED_MORD, MoleculeCounterRow::Structure),
        (MHR_PREFIX, MoleculeCounterRow::Structure),
        (MHK_PREFIX, MoleculeCounterRow::Structure),
        (MOC_PREFIX, MoleculeCounterRow::Structure),
        (ANCHORED_MOC, MoleculeCounterRow::Structure),
        (MK_PREFIX, MoleculeCounterRow::Tip),
        (MH_PREFIX, MoleculeCounterRow::Structure),
    ];
    let (start, prefix, class) = candidates
        .iter()
        .filter_map(|(prefix, class)| key.find(prefix).map(|at| (at, *prefix, *class)))
        .min_by_key(|(at, _, _)| *at)?;
    let rest = &key[start + prefix.len()..];
    let end = rest.find([':', SEP]).unwrap_or(rest.len());
    let uuid = &rest[..end];
    if uuid.is_empty() {
        None
    } else {
        Some((class, uuid))
    }
}

/// Read the molecule uuid out of a molecule-scoped storage key.
///
/// Accepts keys with or without a `{storage_prefix}:` head, because the
/// keep-small write path meters the final storage key rather than the base
/// key. Returns `None` for a key that names no molecule (an atom row, a
/// metadata row), which is the caller's signal to leave those bytes
/// unattributed rather than guess an owner.
#[must_use]
pub fn molecule_uuid_from_storage_key(key: &str) -> Option<&str> {
    let (start, prefix) = MOLECULE_KEY_PREFIXES
        .iter()
        .filter_map(|prefix| key.find(prefix).map(|at| (at, *prefix)))
        .min_by_key(|(at, _)| *at)?;
    let rest = &key[start + prefix.len()..];
    let end = rest.find([':', SEP]).unwrap_or(rest.len());
    let uuid = &rest[..end];
    if uuid.is_empty() {
        None
    } else {
        Some(uuid)
    }
}

/// `mhr:{M}\0{esc(range)}\0{esc(hash)}` — range-major secondary index marker
/// for one HashRange record. The escaped segments preserve raw string ordering
/// for the control bytes this codec reserves, so lexicographic key order matches
/// the HashRange `Page` order: `(range, hash)`.
///
/// Two separators, two different jobs: the first ends the partition (see
/// [`hash_range_page_index_prefix`]), the second delimits range from hash.
/// `esc(range)` can never contain a raw separator, so the split stays
/// unambiguous.
#[must_use]
pub fn hash_range_page_index_key(molecule_uuid: &str, hash: &str, range: &str) -> String {
    format!(
        "{MHR_PREFIX}{molecule_uuid}{SEP}{}{SEP}{}",
        escape_segment(range),
        escape_segment(hash)
    )
}

/// `mhi:v2:{M}` — completion marker for the HashRange range-major page index.
///
/// Version-tagged alongside the partition-pinned key shape. The marker is what
/// suppresses the lazy rebuild, so leaving it unversioned would tell every
/// already-indexed home that its index is complete while every read scanned the
/// new prefix and found nothing — an empty page returned as a correct answer.
/// Bumping it makes each molecule rebuild once, on first page read, and that
/// rebuild is also what reaps the legacy rows.
#[must_use]
pub fn hash_range_page_index_complete_key(molecule_uuid: &str) -> String {
    format!("{MHI_PREFIX}{MHI_VERSION_TAG}{molecule_uuid}")
}

/// `mhi:{M}` — the pre-partition-pinned (untagged) completion marker, swept by
/// rebuild and delete so the version bump does not strand a row per molecule.
#[must_use]
pub fn legacy_hash_range_page_index_complete_key(molecule_uuid: &str) -> String {
    format!("{MHI_PREFIX}{molecule_uuid}")
}

/// `mk:{M}:{esc(hash)}\0{range}` — unified per-key record key for every field kind.
#[must_use]
pub fn hash_range_record_key(molecule_uuid: &str, hash: &str, range: &str) -> String {
    format!(
        "{MK_PREFIX}{molecule_uuid}:{}{SEP}{range}",
        escape_segment(hash)
    )
}

/// `mk:{M}:{esc(hash)}\0` — prefix selecting exactly one hash's ranges in a
/// HashRange field (`HashKey` scan). Unambiguous: `hash="a"` won't match `"ab"`.
#[must_use]
pub fn hash_range_scan_prefix_for_hash(molecule_uuid: &str, hash: &str) -> String {
    format!("{MK_PREFIX}{molecule_uuid}:{}{SEP}", escape_segment(hash))
}

/// Parse the part of a unified record key that follows `mk:{M}:` (the
/// `esc(hash)\0range` suffix) back into `(hash, range)`. Use this when the
/// caller has already stripped the full — possibly `{storage_prefix}:`-prefixed —
/// record prefix from a scan. `None` if the escape is malformed or the suffix
/// is not unified (no `\0` delimiter).
#[must_use]
pub fn decode_hash_range_suffix(suffix: &str) -> Option<(String, String)> {
    // esc(hash) contains no SEP, so the first SEP is the delimiter; the range
    // (which may itself contain SEP) is everything after it.
    let (esc, range) = suffix.split_once(SEP)?;
    let hash = unescape_segment(esc)?;
    Some((hash, range.to_string()))
}

/// Parse the suffix after `mhr:{M}:` back into `(hash, range)`.
#[must_use]
fn decode_hash_range_page_index_suffix(suffix: &str) -> Option<(String, String)> {
    let (esc_range, esc_hash) = suffix.split_once(SEP)?;
    let range = unescape_segment(esc_range)?;
    let hash = unescape_segment(esc_hash)?;
    Some((hash, range))
}

/// Parse a stored HashRange page-index marker key back into `(hash, range)`.
#[must_use]
pub fn decode_hash_range_page_index(
    stored_key: &str,
    molecule_uuid: &str,
) -> Option<(String, String)> {
    let suffix = stored_key.strip_prefix(&hash_range_page_index_prefix(molecule_uuid))?;
    decode_hash_range_page_index_suffix(suffix)
}

/// Parse a stored HashRange record key back into `(hash, range)`. `None` if the
/// prefix doesn't match or the escape is malformed.
#[must_use]
pub fn decode_hash_range(stored_key: &str, molecule_uuid: &str) -> Option<(String, String)> {
    let suffix = stored_key.strip_prefix(&molecule_record_prefix(molecule_uuid))?;
    decode_hash_range_suffix(suffix)
}

/// Parse a stored HashRange record key when the caller does not know which
/// molecule-UUID spelling is on the key (write-form vs legacy hex).
#[must_use]
pub fn decode_hash_range_any(stored_key: &str) -> Option<(String, String)> {
    let molecule_uuid = molecule_uuid_from_storage_key(stored_key)?;
    decode_hash_range(stored_key, molecule_uuid)
}
