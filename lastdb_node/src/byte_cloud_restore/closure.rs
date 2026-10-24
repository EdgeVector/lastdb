use super::{dependencies, download, inventory::Inventory, local, native};
use super::{
    ByteBackupClient, NativeInventory, RecoveryKeys, RestoreError, RestoreReport, RestoreSession,
    Result, VerifiedNativeContext,
};
use cloud_backup_protocol::v2::access::ReadSelector;
use fold_db::storage::disk_pack::byte_backup::{
    Bootstrap, ClientPage, ClientRoot, CODEC_ENVELOPE_MAX_BYTES,
};

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
) -> Result<RestoreReport> {
    let accepted = session.accepted();
    let bootstrap = Bootstrap::from_descriptor(accepted.descriptor()).map_err(codec_error)?;
    bootstrap
        .recovery
        .validate_for(&accepted.job().stream.scope)?;
    keys.validate_account(&bootstrap.recovery.account_public_key)?;
    let mut inventory = Inventory::load(session, client).await?;
    let grant = download::one(
        session,
        client,
        ReadSelector::Receipt {
            receipt_id: accepted.root_receipt_id.clone(),
        },
    )
    .await?;
    grant.validate_for_receipt(inventory.receipt(&accepted.root_receipt_id)?)?;
    if grant.bytes.get() > CODEC_ENVELOPE_MAX_BYTES {
        return Err(RestoreError::Limit("client root metadata"));
    }
    let path = download::fetch(session, client, &grant, true).await?;
    let root = ClientRoot::decode(
        &crate::native_cloud_restore::bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
        keys.content_key(),
        &accepted.begin,
    )
    .map_err(codec_error)?;
    let context = VerifiedNativeContext {
        root: &session.root,
        destination: &session.selection.destination,
        session_id: session.selection.session_id,
        commit_sha256: &accepted.commit_sha256,
        native_root: &root.native_root,
        receipt_name: "byte-cloud-restore-receipt.json",
        receipt_version: 2,
    };
    if let Some(report) = context.published()? {
        return Ok(report);
    }
    dependencies::prepare(&context, &root.recovery.account_public_key)?;
    let totals = restore_pages(session, client, keys, &root, &mut inventory).await?;
    inventory.finish()?;
    drop(inventory);
    let atoms = context.verify_records(keys.content_key(), totals.dependencies)?;
    context.verify_metadata(keys.content_key())?;
    context.finish(keys, totals, atoms, |path| {
        local::read_json(
            &session.root.join("entries").join(native::path_key(path)),
            256 * 1024,
        )
    })
}

async fn restore_pages(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
    root: &ClientRoot,
    inventory: &mut Inventory,
) -> Result<NativeInventory> {
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
            let receipt = inventory.receipt(&expected.receipt_id)?;
            if receipt.object.sha256 != expected.sha256 || receipt.object.bytes != expected.bytes {
                return Err(RestoreError::Invalid("client page receipt"));
            }
            grant.validate_for_receipt(receipt)?;
            inventory.use_receipt(&expected.receipt_id)?;
            let path = download::fetch(session, client, &grant, true).await?;
            let page = ClientPage::decode(
                &crate::native_cloud_restore::bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
                keys.content_key(),
                &session.accepted().begin,
            )
            .map_err(codec_error)?;
            root.validate_page(&page).map_err(codec_error)?;
            if page.page_index != expected.page_index {
                return Err(RestoreError::Invalid("client page index"));
            }
            dependencies::require_native_coverage(&page.external_dependencies)?;
            add_totals(&mut total, &page, root)?;
            native::restore_page(session, client, inventory, &page.native_files).await?;
        }
    }
    if total.files != root.native_file_count
        || total.bytes != root.native_bytes
        || total.dependencies != root.external_dependency_count
    {
        return Err(RestoreError::Invalid("complete client native totals"));
    }
    Ok(total)
}

fn add_totals(total: &mut NativeInventory, page: &ClientPage, root: &ClientRoot) -> Result<()> {
    total.files = total
        .files
        .checked_add(page.native_files.len() as u64)
        .ok_or(RestoreError::Limit("native files"))?;
    for entry in &page.native_files {
        total.bytes = total
            .bytes
            .checked_add(entry.native_bytes.get())
            .ok_or(RestoreError::Limit("native bytes"))?;
    }
    total.dependencies = total
        .dependencies
        .checked_add(page.external_dependencies.len() as u64)
        .ok_or(RestoreError::Limit("file dependency count"))?;
    let coverage = lastdb_storage_policy::backup_decode::FILE_COVERAGE_MEMORY_BYTES
        / lastdb_storage_policy::backup_decode::FILE_COVERAGE_IDENTITY_BYTES;
    if total.files > root.native_file_count
        || total.bytes > root.native_bytes
        || total.dependencies > root.external_dependency_count
        || total.dependencies > coverage as u64
    {
        return Err(RestoreError::Invalid(
            "client totals or file coverage admission",
        ));
    }
    Ok(())
}

fn codec_error(_: fold_db::storage::StorageError) -> RestoreError {
    RestoreError::Invalid("client native metadata")
}
