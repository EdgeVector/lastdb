use super::*;

/// Prefix for the internal atom reverse-reference plane.
///
/// The atom component precedes [`PARTITION_SEP`], so a lookup for one
/// candidate atom stays inside one LastStore hash group. The range component
/// contains the edge type, molecule, disk key, and version identity.
pub const ATOM_REF_EDGE_PREFIX: &str = "aref:v1:e:";

/// Durable cursor and phase for the two-pass reverse-reference rebuild.
pub const ATOM_REF_REINDEX_CHECKPOINT_KEY: &str = "aref:v1:m:reindex";

/// Global marker. Per-molecule manifests remain the authoritative cutover
/// gate; this marker lets audit tools reject an incomplete whole-home result.
pub const ATOM_REF_COMPLETE_KEY: &str = "aref:v1:m:complete";

/// Durable cursor and phase for the compact v2 two-pass rebuild.
pub const ATOM_REF_V2_REINDEX_CHECKPOINT_KEY: &str = "aref:v2:m:reindex";

/// Global compact-plane completion marker for one storage prefix.
pub const ATOM_REF_V2_COMPLETE_KEY: &str = "aref:v2:m:complete";

/// Marker that every known molecule passed compact mutation-history audit.
pub const ATOM_REF_V2_HISTORY_COMPLETE_KEY: &str = "aref:v2:m:history-complete";

fn hex_key_component(value: &str) -> String {
    hex_lower(value)
}

/// Prefix covering every reverse edge for one atom UUID.
#[must_use]
pub fn atom_ref_edge_prefix(atom_uuid: &str) -> String {
    format!(
        "{ATOM_REF_EDGE_PREFIX}{}{PARTITION_SEP}",
        hex_key_component(atom_uuid)
    )
}

/// One reverse edge key.
#[must_use]
pub fn atom_ref_edge_key(
    atom_uuid: &str,
    edge_type: &str,
    molecule_uuid: &str,
    disk_hash: &str,
    disk_range: &str,
    version_id: &str,
) -> String {
    format!(
        "{}{}:{}:{}:{}:{}",
        atom_ref_edge_prefix(atom_uuid),
        hex_key_component(edge_type),
        hex_key_component(molecule_uuid),
        hex_key_component(disk_hash),
        hex_key_component(disk_range),
        hex_key_component(version_id),
    )
}

/// Per-molecule reverse-reference completeness manifest.
#[must_use]
pub fn atom_ref_molecule_manifest_key(molecule_uuid: &str) -> String {
    crate::kind_partition::anchored(
        "aref",
        &format!("v1:m:molecule:{}", hex_key_component(molecule_uuid)),
    )
}

/// Durable cursor for one molecule's bounded mutation-history edge upgrade.
#[must_use]
pub fn atom_ref_history_upgrade_key(molecule_uuid: &str) -> String {
    crate::kind_partition::anchored(
        "aref",
        &format!("v1:m:history:{}", hex_key_component(molecule_uuid)),
    )
}

/// Per-molecule compact reverse-reference completeness manifest.
#[must_use]
pub fn atom_ref_v2_molecule_manifest_key(molecule_uuid: &str) -> String {
    format!("aref:v2:m:molecule:{}", hex_key_component(molecule_uuid))
}

/// Durable cursor for one molecule's compact mutation-history edge upgrade.
#[must_use]
pub fn atom_ref_v2_history_upgrade_key(molecule_uuid: &str) -> String {
    format!("aref:v2:m:history:{}", hex_key_component(molecule_uuid))
}
