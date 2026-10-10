/// Shared atom plane: `atom\0…` and an org-prefixed `*:atom\0…`.
///
/// `atom:uuid` (no NUL) is not this plane. A cold load must rebuild the
/// index; body bytes are not what this check protects.
pub(super) fn is_atom_plane_id(id: &str) -> bool {
    let partition = partition_of(id);
    let bare = partition.strip_suffix('\0').unwrap_or(partition);
    bare == "atom" || bare.ends_with(":atom")
}

/// Byte separating a key's partition prefix from its in-partition suffix.
///
/// Callers that want [`HashGroupKey::PartitionPrefix`] locality encode keys as
/// `{partition}\0{suffix}`. LastDB's molecule codec already does: its per-key
/// record is `mk:{molecule}:{esc(hash)}\0{range}`, and it byte-stuffs the hash
/// so `esc(hash)` can never contain a NUL — which is what makes "the first NUL"
/// an unambiguous partition boundary rather than a guess.
pub(super) const PARTITION_SEP: char = '\0';

/// Whether `LASTDB_TRACE_SWEEP` asked for unprunable-walk reporting.
///
/// Read once and cached: the check sits on every prefix and range walk, so a
/// per-call `env::var` (which locks and allocates) would itself distort the
/// cost being measured.
pub(super) fn full_sweep_trace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("LASTDB_TRACE_SWEEP")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false)
    })
}

/// The partition prefix of `id`: everything up to and **including** the first
/// [`PARTITION_SEP`], or all of `id` when it contains none.
///
/// Including the separator is what keeps partitions from colliding: without it,
/// `a\0x` and `ab\0x` would both reduce to a prefix that the other's scan
/// prefix also matches.
pub(super) fn partition_of(id: &str) -> &str {
    match id.find(PARTITION_SEP) {
        Some(index) => &id[..index + PARTITION_SEP.len_utf8()],
        None => id,
    }
}

/// The exclusive upper bound of the ids beginning with `partition`: the ids in
/// that partition are exactly the half-open interval `[partition, end)`.
///
/// `partition` here always ends with [`PARTITION_SEP`] (`0x00`), so the bound is
/// that trailing byte incremented to `0x01`. Any id `partition + suffix` sorts
/// below it (they agree up to the separator, where `0x00 < 0x01`), and the bound
/// itself is not prefixed by `partition` — which is what makes the interval
/// exact rather than merely safe.
pub(super) fn partition_end(partition: &str) -> String {
    debug_assert!(partition.ends_with(PARTITION_SEP));
    let head = &partition[..partition.len() - PARTITION_SEP.len_utf8()];
    let mut end = String::with_capacity(partition.len());
    end.push_str(head);
    end.push('\u{1}');
    end
}
