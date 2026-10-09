use super::common::FieldCommon;
use crate::db_operations::{DbOperations, MoleculeData};
use crate::schema::types::SchemaError;

/// Load the per-key molecule for exactly `storage_prefix` / storage prefix.
///
/// No bare-key dual-read: a storage prefix (`from:{sender}`, historical org
/// hash, etc.) is an isolation boundary. Falling back to personal keys would
/// leak personal data into share/org reads. After the org-crypto strip there
/// is no set-org-hash pre-tag migration path left to dual-read for.
async fn load_per_key_exact(
    db_ops: &DbOperations,
    molecule_uuid: &str,
    storage_prefix: Option<&str>,
) -> Result<Option<MoleculeData>, SchemaError> {
    db_ops
        .atoms()
        .load_molecule_per_key(molecule_uuid, storage_prefix)
        .await
}

/// Refresh a unified [`super::FieldVariant`]'s molecule slot from storage.
///
/// Per-key layout (`mk:` / `mh:`) is the only source of truth. Legacy `ref:`
/// blobs are not read; sync migrates them on receive when a peer still ships
/// them.
pub(crate) async fn refresh_field_molecule_from_db(
    inner: &mut FieldCommon,
    molecule_slot: &mut Option<MoleculeData>,
    kind: super::variant::FieldKind,
    db_ops: &DbOperations,
) -> Result<(), SchemaError> {
    let Some(molecule_uuid) = inner.molecule_uuid().cloned() else {
        return Ok(());
    };
    let storage_prefix = inner.storage_prefix().map(ToString::to_string);
    let storage_prefix_ref = storage_prefix.as_deref();

    if kind.is_per_key() {
        if let Some(data) = load_per_key_exact(db_ops, &molecule_uuid, storage_prefix_ref).await? {
            // Normalize 1-D slot orientation for Hash-only / Range-only fields.
            let data = data.retyped_to_slot(kind.retype_slot());
            if !kind.matches_data(&data) {
                return Err(SchemaError::InvalidData(format!(
                    "refresh_field_molecule_from_db: per-key molecule {molecule_uuid} kind mismatch with field kind {kind:?}"
                )));
            }
            *molecule_slot = Some(data);
        }
    }

    Ok(())
}
