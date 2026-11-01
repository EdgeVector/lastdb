use super::*;
use crate::db_operations::atom_store::MoleculeRangeForm;

/// Hydrate + filter into [`KeyedAtomMatch`] tuples **without** fetching
/// atom bodies.
///
/// Used by the co-key multi-field list plan: the primary field establishes
/// the page of keys, secondary fields contribute only UUID lookups for
/// those keys, then a single cross-field atom batch materializes content.
pub async fn collect_matches(
    field: &mut FieldVariant,
    db_ops: &Arc<DbOperations>,
    filter: Option<HashRangeFilter>,
    include_tombstones: bool,
) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
    field.require_supported_read(filter.as_ref())?;
    let profile = read_range_profile(field, db_ops).await?;
    // A field with no molecule and no active comparison context has no range type.
    let filter = match profile {
        Some(profile) => {
            crate::schema::types::key_comparison::normalize::canonical_filter(profile, filter)?
        }
        None => filter,
    };
    crate::schema::types::key_comparison::KeyComparisonContext::run_for_molecule(
        profile,
        captured_field_read(
            db_ops,
            collect_matches_captured(field, db_ops, filter, include_tombstones).boxed(),
        ),
    )
    .await
}

async fn collect_matches_captured(
    field: &mut FieldVariant,
    db_ops: &Arc<DbOperations>,
    filter: Option<HashRangeFilter>,
    include_tombstones: bool,
) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
    if let Some(filter) = filter.as_ref() {
        if let Some(matches) = crate::storage::disk_pack::logical_paths::core_filter::matches(
            db_ops.namespaced_store().disk_pack_runtime(),
            field.common().storage_prefix(),
            field.common().molecule_uuid().map(String::as_str),
            field.kind,
            filter,
            include_tombstones,
        )? {
            return Ok(matches);
        }
    }
    let effective_filter = filter.unwrap_or(HashRangeFilter::Page {
        offset: 0,
        limit: DEFAULT_UNFILTERED_PAGE_LIMIT,
    });

    let range_form = refresh_for_read(field, db_ops, &effective_filter, include_tombstones).await?;

    // In-memory apply uses string compare on molecule range slots. Typed
    // range slots stay storage-form (see load_filtered_molecule), so API
    // range bounds must be rewritten to the stored payload first —
    // otherwise HashRangePrefix drops every match after a correct storage
    // scan.
    let results = match field.common().molecule_uuid().map(String::as_str) {
        Some(mol_uuid) => {
            let codec = db_ops.atoms().key_codec_for_molecule(mol_uuid)?;
            let storage_filter =
                crate::schema::types::field::filter_utils::filter_range_bounds_for_apply(
                    &effective_filter,
                    &codec,
                    mol_uuid,
                )
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            field.apply_filter(Some(storage_filter))
        }
        None => field.apply_filter(Some(effective_filter)),
    };
    let source_codec = if range_form == MoleculeRangeForm::Storage && !results.matches.is_empty() {
        let molecule = field.common().molecule_uuid().ok_or_else(|| {
            SchemaError::InvalidData("stored field range has no molecule identity".into())
        })?;
        Some(db_ops.atoms().key_codec_for_molecule(molecule)?)
    } else {
        None
    };

    let mut matches_with_meta: Vec<KeyedAtomMatch> = results
        .matches
        .into_iter()
        .map(|(storage_key, atom_uuid)| {
            // Metadata uses the transient molecule's own coordinates. Exact
            // API ranges retain their previously selected physical receipt.
            let (key_meta, writer_pubkey) = field.entry_meta_at(&storage_key);
            let kv = match source_codec.as_ref() {
                Some(codec) => api_key_value_from_storage(storage_key.clone(), codec)?,
                None => storage_key.clone(),
            };
            note_loaded_tip(field, db_ops, &storage_key, &kv)?;
            Ok((kv, atom_uuid, key_meta, writer_pubkey))
        })
        .collect::<Result<_, SchemaError>>()?;

    if !include_tombstones {
        matches_with_meta
            .retain(|(_, _, key_meta, _)| !key_meta.as_ref().is_some_and(|m| m.tombstoned));
    }

    Ok(matches_with_meta)
}
