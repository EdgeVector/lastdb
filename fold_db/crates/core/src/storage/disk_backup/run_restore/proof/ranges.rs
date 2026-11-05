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
    if version != crate::storage::disk_pack::run_admission::RECORD_PROFILE {
        return Err(invalid("run backup range profile version"));
    }
    let mut read = super::rows::read(main, key_heads)?;
    let mut rows = Rows::new(
        main,
        read.as_mut(),
        crate::storage::format::SCHEMAS_NAMESPACE,
    )?;
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
        schema
            .require_record_keys()
            .map_err(|_| invalid("run backup schema has no explicit record keys"))?
            .primary
            .range_profile()
            .map_err(|_| invalid("run backup record key has no typed profile"))?;
        let mut grouped: Vec<(FieldRangeProfile, Vec<String>)> = Vec::new();
        for (field_key, profile) in
            crate::storage::field_key_store::schema_field_key_profiles(&schema)?
        {
            if let Some((_, keys)) = grouped.iter_mut().find(|(found, _)| *found == profile) {
                keys.push(field_key);
            } else {
                grouped.push((profile, vec![field_key]));
            }
        }
        for (profile, field_keys) in grouped {
            let refs: Vec<&str> = field_keys.iter().map(String::as_str).collect();
            for chunk in refs.chunks(128) {
                batch(main, account_key, profile, chunk)?;
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
            node_bundle_storage_keys(field_key)?
                .map(|key| (crate::storage::format::FIELD_KEYS_NAMESPACE.to_owned(), key)),
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
