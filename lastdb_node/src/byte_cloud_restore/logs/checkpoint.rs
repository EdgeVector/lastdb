use super::super::{download, inventory::Inventory, native, runs::coverage::Coverage};
use super::*;
use crate::byte_cloud_log::MutationLogPage;
use cloud_backup_protocol::v2::access::ReadSelector;
use fold_db::storage::disk_pack::byte_backup::CODEC_ENVELOPE_MAX_BYTES;

pub(super) async fn download(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    root: &MutationLogRoot,
    mut inventory: Inventory,
) -> Result<()> {
    let mut coverage = Coverage::new_pages(&root.pages, session, &mut inventory)?;
    let mut files = 0u64;
    let mut bytes = 0u64;
    for batch in root
        .pages
        .chunks(cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS)
    {
        let selectors = batch
            .iter()
            .map(|page| ReadSelector::Receipt {
                receipt_id: page.receipt_id.clone(),
            })
            .collect();
        let grants = download::grants(session, client, selectors).await?;
        for (expected, grant) in batch.iter().zip(grants) {
            grant.validate_for_receipt(inventory.receipt(&expected.receipt_id)?)?;
            inventory.use_receipt(&expected.receipt_id)?;
            let path = download::fetch(session, client, &grant, true).await?;
            let page = MutationLogPage::decode(
                &crate::native_cloud_restore::bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
                keys.content_key(),
                &session.accepted().begin,
            )
            .map_err(|_| RestoreError::Invalid("mutation file page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("mutation file page context"))?;
            if page.page_index != expected.page_index {
                return Err(RestoreError::Invalid("mutation page order"));
            }
            coverage.page_with(&page.files, &mut inventory, |file| {
                root.validate_file(file)
                    .map_err(|_| RestoreError::Invalid("mutation native file identity"))
            })?;
            for file in &page.files {
                files = files
                    .checked_add(1)
                    .ok_or(RestoreError::Limit("mutation file total"))?;
                bytes = bytes
                    .checked_add(file.native_bytes.get())
                    .ok_or(RestoreError::Limit("mutation byte total"))?;
            }
            if files > root.native_file_count || bytes > root.native_bytes {
                return Err(RestoreError::Invalid("mutation file totals"));
            }
            native::restore_at(
                session,
                client,
                &mut inventory,
                &page.files,
                &session.root.join("replay-files"),
            )
            .await?;
        }
    }
    if files != root.native_file_count || bytes != root.native_bytes {
        return Err(RestoreError::Invalid("complete mutation file totals"));
    }
    coverage.finish()?;
    inventory.finish()
}

pub(super) fn release(session: &RestoreSession, root: &MutationLogRoot) -> Result<()> {
    for entry in &root.entries {
        for part in &entry.parts {
            let name = fold_db::storage::disk_pack::mutation_log::log_path(
                &root.anchor.epoch,
                entry.sequence,
                part.role,
                &part.run_id,
            );
            let path = session.root.join("replay-files").join(&name);
            match std::fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_file() && meta.len() == part.bytes => {
                    std::fs::remove_file(&path)?;
                }
                Ok(_) => return Err(RestoreError::Invalid("mutation source release identity")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            session
                .remove_spool_file(&session.root.join("entries").join(native::path_key(&name)))?;
        }
    }
    std::fs::File::open(session.root.join("replay-files"))?.sync_all()?;
    super::cache::clear(session)
}
