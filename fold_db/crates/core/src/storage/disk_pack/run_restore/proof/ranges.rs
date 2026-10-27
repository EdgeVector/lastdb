//! Typed schema/profile admission against the same held encrypted backup root.

use super::{budget, invalid, map, read_budget, rows::Rows, RunRead, StorageResult};
use crate::db_operations::molecule_key_store::{
    node_bundle_storage_keys, verify_frozen_typed_profile, NODE_KEY_ROW_MAX_BYTES,
};
use crate::schema::Schema;
use schema_types::molecule_range_profile::MoleculeRangeProfile;

pub(super) fn verify(
    main: &RunRead,
    account_key: &[u8; 32],
    version: u16,
    key_heads: bool,
) -> StorageResult<()> {
    if version == 1 {
        return Ok(());
    }
    if version != 2 {
        return Err(invalid("run backup range profile version"));
    }
    let mut read = super::rows::read(main, key_heads)?;
    let mut rows = Rows::new(main, read.as_mut(), "schemas")?;
    while let Some((_, bytes)) = rows.next()? {
        let opened = crate::crypto::is_sealed_at_rest(&bytes)
            .then(|| {
                crate::crypto::at_rest::open_at_rest_bounded(
                    account_key,
                    &bytes,
                    budget::ATOM_BODY_MAX_BYTES,
                )
                .map(zeroize::Zeroizing::new)
            })
            .transpose()
            .map_err(|_| invalid("run backup schema envelope cannot be opened"))?;
        let plain = opened.as_deref().map_or(bytes.as_slice(), Vec::as_slice);
        let mut schema: Schema = serde_json::from_slice(plain)
            .map_err(|_| invalid("run backup schema record is unreadable"))?;
        schema
            .populate_runtime_fields()
            .map_err(|_| invalid("run backup schema fields are invalid"))?;
        schema
            .validate_key_field_types()
            .map_err(|_| invalid("run backup key fields are invalid"))?;
        let range = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        let profile = MoleculeRangeProfile::for_field_type(range.as_ref())
            .map_err(|_| invalid("run backup range field has no typed profile"))?;
        let mut molecules = schema
            .runtime_fields
            .values()
            .map(|field| {
                field
                    .common()
                    .molecule_uuid
                    .as_deref()
                    .ok_or_else(|| invalid("run backup field molecule is absent"))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        molecules.sort_unstable();
        molecules.dedup();
        for chunk in molecules.chunks(128) {
            batch(main, account_key, profile, chunk)?;
        }
    }
    Ok(())
}

fn batch(
    main: &RunRead,
    account_key: &[u8; 32],
    profile: MoleculeRangeProfile,
    molecules: &[&str],
) -> StorageResult<()> {
    let mut coordinates = Vec::with_capacity(molecules.len() * 2);
    for molecule in molecules {
        coordinates.extend(
            node_bundle_storage_keys(molecule)?.map(|key| ("molecule_keys".to_owned(), key)),
        );
    }
    let mut read = main
        .fork_bounded(read_budget(
            budget::ATOM_INVENTORY_READ_BYTES,
            coordinates.len() as u64,
        ))
        .map_err(map)?;
    let values = read
        .get_many_bounded(&coordinates, NODE_KEY_ROW_MAX_BYTES)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    if values.len() != coordinates.len() {
        return Err(invalid("run backup molecule contract batch is incomplete"));
    }
    for (molecule, rows) in molecules.iter().zip(values.chunks_exact(2)) {
        verify_frozen_typed_profile(
            molecule,
            profile,
            account_key,
            rows[0].as_deref(),
            rows[1].as_deref(),
        )?;
    }
    Ok(())
}
