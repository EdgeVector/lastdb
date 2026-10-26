//! Restore the exact paired disk V2 files before any normal writer opens them.
mod coverage;
mod install;

use super::{download, inventory::Inventory, native};
use super::{ByteBackupClient, NativeInventory, RecoveryKeys, RestoreError, RestoreReport};
use super::{RestoreSession, Result, VerifiedNativeContext};
use cloud_backup_protocol::v2::access::ReadSelector;
use fold_db::storage::disk_pack::byte_backup::{ClientPage, ClientRoot, CODEC_ENVELOPE_MAX_BYTES};

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    root: &ClientRoot,
    mut inventory: Inventory,
) -> Result<RestoreReport> {
    let mut coverage = coverage::Coverage::new(root, session, &mut inventory)?;
    let mut total = NativeInventory {
        files: 0,
        bytes: 0,
        dependencies: 0,
    };
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
            let page = ClientPage::decode(
                &crate::native_cloud_restore::bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
                keys.content_key(),
                &session.accepted().begin,
            )
            .map_err(|_| RestoreError::Invalid("run client page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("run page context"))?;
            if page.page_index != expected.page_index || !page.external_dependencies.is_empty() {
                return Err(RestoreError::Invalid("run page identity or dependencies"));
            }
            coverage.page(&page.native_files, &mut inventory)?;
            add_totals(&mut total, &page, root)?;
            native::restore_at(
                session,
                client,
                &mut inventory,
                &page.native_files,
                &session.root.join("home/data"),
            )
            .await?;
        }
    }
    if total.files != root.native_file_count || total.bytes != root.native_bytes {
        return Err(RestoreError::Invalid("complete run native totals"));
    }
    coverage.finish()?;
    inventory.finish()?;
    drop(coverage);
    drop(inventory);
    let context = VerifiedNativeContext {
        root: &session.root,
        destination: &session.selection.destination,
        session_id: session.selection.session_id,
        commit_sha256: &session.accepted().commit_sha256,
        native_root: &root.native_root,
        receipt_name: "byte-cloud-restore-receipt.json",
        receipt_version: 2,
    };
    install::finish(session, keys, root, &context, total)
}

fn add_totals(total: &mut NativeInventory, page: &ClientPage, root: &ClientRoot) -> Result<()> {
    total.files = total
        .files
        .checked_add(page.native_files.len() as u64)
        .ok_or(RestoreError::Limit("run native file count"))?;
    for file in &page.native_files {
        total.bytes = total
            .bytes
            .checked_add(file.native_bytes.get())
            .ok_or(RestoreError::Limit("run native byte count"))?;
    }
    if total.files > root.native_file_count || total.bytes > root.native_bytes {
        return Err(RestoreError::Invalid("run native totals"));
    }
    Ok(())
}
