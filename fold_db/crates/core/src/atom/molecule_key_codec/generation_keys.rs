use super::*;

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
