//! Reconstruct the filtered native state through the ordinary staged writer.

use crate::byte_cloud_restore::{RecoveryKeys, RestoreError, Result};
use crate::shared_restore::recovery::SnapshotSchema;
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::{
    factory::{create_fold_db_with_options, FoldDbOptions},
    mutation_manager::{portable_checkpoint::PortableCheckpoint, portable_replay::ReplayCandidate},
};
use fold_db::portable_mutation::{MutationOperation, Position, StreamId};
use fold_db::schema::Schema;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::FoldDB;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_LIMIT: u64 = lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;

pub(super) async fn open(root: &Path, keys: &RecoveryKeys) -> Result<Arc<FoldDB>> {
    crate::byte_cloud_restore::local::directory(root)?;
    let e2e = E2eKeys::from_ed25519_seed(keys.account_seed())
        .map_err(|_| RestoreError::Invalid("filtered checkpoint account key"))?;
    let signer = Ed25519KeyPair::from_secret_key(keys.account_seed())
        .map_err(|_| RestoreError::Invalid("filtered checkpoint account signer"))?;
    create_fold_db_with_options(
        &DatabaseConfig::local(root),
        &e2e,
        Arc::new(signer),
        FoldDbOptions {
            offline_maintenance: true,
            search_outbox_inbox: None,
        },
    )
    .await
    .map_err(|_| RestoreError::Invalid("filtered native home open"))
}

pub(super) async fn copy(
    db: &FoldDB,
    source: &mut PortableCheckpoint,
    schemas: &[SnapshotSchema],
    expires: u64,
) -> Result<()> {
    require_time(expires)?;
    let install = schemas
        .iter()
        .map(|binding| {
            let mut wire = binding
                .definition
                .verify(binding.definition.id())
                .map_err(|_| RestoreError::Invalid("filtered checkpoint schema"))?
                .clone();
            wire.name.clone_from(&binding.local_schema);
            Ok(Schema::from(wire))
        })
        .collect::<Result<Vec<_>>>()?;
    db.schema_manager()
        .load_schema_batch(install)
        .await
        .map_err(|_| RestoreError::Invalid("install filtered checkpoint schemas"))?;
    let definitions: BTreeMap<_, _> = schemas
        .iter()
        .map(|schema| (schema.local_schema.as_str(), &schema.definition))
        .collect();
    let writer = db.mutation_manager();
    while let Some(mut page) = source
        .next_page()
        .await
        .map_err(|_| RestoreError::Invalid("read native checkpoint page"))?
    {
        require_time(expires)?;
        page.sort_unstable_by(|left, right| right.entry().stamp.cmp(&left.entry().stamp));
        let mut batch = Vec::new();
        let mut files = BTreeMap::new();
        let mut total = 0u64;
        for row in &page {
            let entry = row.entry();
            if let MutationOperation::ReplaceRecord { data, .. } = &entry.operation {
                if let Some(marker) = &data.file {
                    if !batch.is_empty() && total.saturating_add(marker.size_bytes) > FILE_LIMIT / 2
                    {
                        apply(writer, &mut batch, &mut files).await?;
                        total = 0;
                    }
                    let sealed = source
                        .file(row)
                        .await
                        .map_err(|_| RestoreError::Invalid("read filtered checkpoint file"))?;
                    total = total.saturating_add(sealed.len() as u64);
                    if total > FILE_LIMIT {
                        return Err(RestoreError::Limit("filtered checkpoint file input"));
                    }
                    if files.insert(marker.version.clone(), sealed).is_some() {
                        return Err(RestoreError::Invalid(
                            "filtered checkpoint repeats a file version",
                        ));
                    }
                }
            }
            let definition = definitions
                .get(row.local_schema())
                .ok_or(RestoreError::Invalid(
                    "filtered checkpoint schema is missing",
                ))?;
            batch.push(ReplayCandidate {
                entry,
                definition,
                local_schema: row.local_schema(),
            });
        }
        apply(writer, &mut batch, &mut files).await?;
    }
    let fresh = StreamId::try_from(uuid::Uuid::new_v4().to_string())
        .map_err(|_| RestoreError::Invalid("filtered checkpoint stream identifier"))?;
    writer
        .finish_portable_restore(
            source.progress(),
            source.delete_reclaimed_through_ms(),
            &fresh,
        )
        .await
        .map_err(|_| RestoreError::Invalid("persist filtered checkpoint cut"))?;
    Ok(())
}

async fn apply(
    writer: &fold_db::fold_db_core::MutationManager,
    batch: &mut Vec<ReplayCandidate<'_>>,
    files: &mut BTreeMap<Position, Vec<u8>>,
) -> Result<()> {
    if !batch.is_empty() {
        writer
            .apply_portable_candidates(batch, std::mem::take(files))
            .await
            .map_err(|_| RestoreError::Invalid("commit filtered checkpoint records"))?;
        batch.clear();
    }
    Ok(())
}

pub(super) fn require_time(expires: u64) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RestoreError::Invalid("filtered checkpoint clock"))?;
    if now.as_millis() >= u128::from(expires) {
        return Err(RestoreError::Limit("filtered checkpoint deadline"));
    }
    Ok(())
}
