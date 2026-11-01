//! Typed schema/profile admission against the same held encrypted backup root.

use super::{budget, invalid, map, read_budget, rows::Rows, RunRead, StorageResult};
use crate::schema::Schema;
use crate::storage::field_key_store::{
    node_bundle_storage_keys, verify_frozen_typed_profile, NODE_KEY_ROW_MAX_BYTES,
};
use schema_types::field_range_profile::FieldRangeProfile;

pub(super) fn verify(
    main: &RunRead,
    account_key: &[u8; 32],
    version: u16,
    key_heads: bool,
) -> StorageResult<()> {
    // This release restores only format 7 homes.
    if version != crate::storage::disk_pack::run_admission::RECORD_PROFILE {
        return Err(invalid("run backup range profile version"));
    }
    let mut read = super::rows::read(main, key_heads)?;
    let mut rows = Rows::new(main, read.as_mut(), "schemas")?;
    while let Some((_, bytes)) = rows.next()? {
        let mut schema: Schema = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("run backup schema record is unreadable"))?;
        schema
            .populate_runtime_fields()
            .map_err(|_| invalid("run backup schema fields are invalid"))?;
        schema
            .validate_key_field_types()
            .map_err(|_| invalid("run backup key fields are invalid"))?;
        schema
            .validate_alternate_keys()
            .map_err(|_| invalid("run backup alternate declarations are invalid"))?;
        let range = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| schema.key_field_type(path));
        let profile = FieldRangeProfile::for_field_type(range.as_ref())
            .map_err(|_| invalid("run backup range field has no typed profile"))?;
        let mut field_keys = schema
            .runtime_fields
            .values()
            .map(|field| {
                field
                    .common()
                    .field_key_id
                    .as_deref()
                    .ok_or_else(|| invalid("run backup field key is absent"))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        field_keys.sort_unstable();
        field_keys.dedup();
        for chunk in field_keys.chunks(128) {
            batch(main, account_key, profile, chunk)?;
        }
        // A format 7 home adds the record space of each schema.
        for (field_key, alternate_profile) in
            crate::storage::field_key_store::schema_field_key_profiles(&schema, true)?
        {
            if !field_keys.contains(&field_key.as_str()) {
                batch(main, account_key, alternate_profile, &[field_key.as_str()])?;
            }
        }
    }
    Ok(())
}

fn batch(
    main: &RunRead,
    account_key: &[u8; 32],
    profile: FieldRangeProfile,
    field_keys: &[&str],
) -> StorageResult<()> {
    let mut coordinates = Vec::with_capacity(field_keys.len() * 2);
    for field_key in field_keys {
        coordinates.extend(
            node_bundle_storage_keys(field_key)?.map(|key| ("molecule_keys".to_owned(), key)),
        );
    }
    let mut read = super::fork_mapped(
        main,
        read_budget(
            budget::RECORD_INVENTORY_READ_BYTES,
            coordinates.len() as u64,
        ),
    )?;
    let values = read
        .get_many_bounded(&coordinates, NODE_KEY_ROW_MAX_BYTES)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    if values.len() != coordinates.len() {
        return Err(invalid("run backup field key contract batch is incomplete"));
    }
    for (field_key, rows) in field_keys.iter().zip(values.chunks_exact(2)) {
        verify_frozen_typed_profile(
            field_key,
            profile,
            account_key,
            rows[0].as_deref(),
            rows[1].as_deref(),
        )?;
    }
    Ok(())
}
