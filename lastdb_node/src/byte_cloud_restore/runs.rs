//! Restore the exact paired disk V2 files, verify extent coverage, and open run restore proof.
use super::{
    bounded_bytes, download,
    inventory::{digest_bytes, Inventory},
    local, native, recipient, verify_file, ByteBackupReader, NativeFileProof, RecoveryDecryption,
    RestoreError, RestoreReport, RestoreSession, Result,
};
use cloud_backup_protocol::v2::access::ReadSelector;
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::Digest;
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::{
        run_backup_destination, ClientPage, ClientPageReference, ClientRoot,
        CODEC_ENVELOPE_MAX_BYTES,
    },
    run_restore,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    identity_seed: &[u8; 32],
    root: &ClientRoot,
    inventory: Inventory,
) -> Result<RestoreReport> {
    let totals = stage(session, client, decryption, root, inventory).await?;
    let proof = verify_stage(session, decryption, root)?;
    recipient::finish_run(session, &root.native_root, identity_seed, totals, &proof)
}

pub(super) async fn stage(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    root: &ClientRoot,
    inventory: Inventory,
) -> Result<(u64, u64)> {
    let mut files = 0u64;
    let mut bytes = 0u64;
    let data = session.root.join("home/data");
    stage_pages(
        session,
        client,
        &root.pages,
        inventory,
        &data,
        |index, raw| {
            let page = ClientPage::decode(raw, decryption.content_key(), &session.accepted().begin)
                .map_err(|_| RestoreError::Invalid("run client page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("run page context"))?;
            if page.page_index != index {
                return Err(RestoreError::Invalid("run page identity"));
            }
            add_totals(&mut files, &mut bytes, &page, root)?;
            page.native_files
                .into_iter()
                .map(|file| {
                    let dest = run_backup_destination(file.path.as_str())
                        .map_err(|_| RestoreError::Invalid("run native path"))?;
                    Ok((file, dest))
                })
                .collect()
        },
    )
    .await?;
    if (files, bytes) != (root.native_file_count, root.native_bytes) {
        return Err(RestoreError::Invalid("complete run native totals"));
    }
    Ok((files, bytes))
}

pub(super) async fn stage_pages(
    session: &RestoreSession,
    client: &ByteBackupReader,
    pages: &[ClientPageReference],
    mut inventory: Inventory,
    target: &Path,
    mut decode: impl FnMut(u32, &[u8]) -> Result<Vec<(ClientNativeFile, Destination)>>,
) -> Result<()> {
    let mut coverage = Coverage::new_pages(pages, session, &mut inventory)?;
    for batch in pages.chunks(cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS) {
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
            let typed = decode(
                expected.page_index,
                &bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
            )?;
            coverage.add_page(&typed, &mut inventory)?;
            let files: Vec<_> = typed.into_iter().map(|(file, _)| file).collect();
            native::restore_at(session, client, &mut inventory, &files, target).await?;
        }
    }
    coverage.finish()?;
    inventory.finish()
}

pub(super) fn verify_stage(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    root: &ClientRoot,
) -> Result<run_restore::RunRestoreGuard> {
    let data = session.root.join("home/data");
    local::prepare_native_ephemera(&data)?;
    run_restore::install_empty_cas_home(&data, decryption.content_key())
        .map_err(RestoreError::native_proof)?;
    let proof = run_restore::verify(
        &data,
        &root.run,
        &root.native_root,
        decryption.content_key(),
    )
    .map_err(RestoreError::native_proof)?;
    for file in proof.files() {
        let entry: NativeFileProof = local::read_json(
            &session
                .root
                .join("entries")
                .join(native::path_key(&file.path)),
            256 * 1024,
        )?;
        if entry.path != file.path || entry.bytes != file.bytes {
            return Err(RestoreError::Invalid("run exact native inventory"));
        }
        verify_file(&data.join(&file.path), &entry.sha256, file.bytes)?;
    }
    Ok(proof)
}

fn add_totals(
    files: &mut u64,
    bytes: &mut u64,
    page: &ClientPage,
    root: &ClientRoot,
) -> Result<()> {
    *files = files
        .checked_add(page.native_files.len() as u64)
        .ok_or(RestoreError::Limit("run native file count"))?;
    for file in &page.native_files {
        *bytes = bytes
            .checked_add(file.native_bytes.get())
            .ok_or(RestoreError::Limit("run native byte count"))?;
    }
    if *files > root.native_file_count || *bytes > root.native_bytes {
        return Err(RestoreError::Invalid("run native totals"));
    }
    Ok(())
}

struct Coverage {
    metadata: BTreeSet<[u8; 32]>,
    data: BTreeMap<[u8; 32], Object>,
}
struct Object {
    bytes: u64,
    ranges: BTreeMap<u64, u64>,
}

impl Coverage {
    fn new_pages(
        pages: &[ClientPageReference],
        session: &RestoreSession,
        inventory: &mut Inventory,
    ) -> Result<Self> {
        let mut value = Self {
            metadata: BTreeSet::new(),
            data: BTreeMap::new(),
        };
        value.metadata(&session.accepted().root_receipt_id, inventory)?;
        for page in pages {
            let receipt = inventory.receipt(&page.receipt_id)?;
            if receipt.object.sha256 != page.sha256 || receipt.object.bytes != page.bytes {
                return Err(RestoreError::Invalid("run page receipt"));
            }
            value.metadata(&page.receipt_id, inventory)?;
        }
        Ok(value)
    }

    fn metadata(&mut self, id: &Digest, inventory: &mut Inventory) -> Result<()> {
        if inventory.receipt(id)?.object.destination != Destination::R2 {
            return Err(RestoreError::Invalid("run metadata destination"));
        }
        inventory.charge(fold_db::storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
        if !self.metadata.insert(digest_bytes(id)?) {
            return Err(RestoreError::Invalid("duplicate run metadata receipt"));
        }
        Ok(())
    }

    fn add_page(
        &mut self,
        files: &[(ClientNativeFile, Destination)],
        inventory: &mut Inventory,
    ) -> Result<()> {
        for (file, destination) in files {
            for extent in &file.extents {
                let receipt = inventory.receipt(&extent.receipt_id)?;
                let id = digest_bytes(&extent.receipt_id)?;
                if self.metadata.contains(&id) || receipt.object.destination != *destination {
                    return Err(RestoreError::Invalid("run native receipt destination"));
                }
                let bytes = receipt.object.bytes.get();
                let object = match self.data.entry(id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        inventory.charge(
                            fold_db::storage_policy::backup_decode::IDENTITY_MEMORY_BYTES,
                        )?;
                        entry.insert(Object {
                            bytes,
                            ranges: BTreeMap::new(),
                        })
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                };
                let start = extent.offset.get();
                let length = extent.length.get();
                if object
                    .ranges
                    .get(&start)
                    .is_some_and(|end| Some(*end) == start.checked_add(length))
                {
                    continue;
                }
                inventory.charge(fold_db::storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
                object.add(start, length)?;
            }
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        for object in self.data.values() {
            let mut cursor = 0;
            for (&start, &end) in &object.ranges {
                if start != cursor {
                    return Err(RestoreError::Invalid("run receipt coverage gap"));
                }
                cursor = end;
            }
            if cursor != object.bytes {
                return Err(RestoreError::Invalid("run complete receipt coverage"));
            }
        }
        Ok(())
    }
}

impl Object {
    fn add(&mut self, start: u64, length: u64) -> Result<()> {
        let end = start
            .checked_add(length)
            .ok_or(RestoreError::Invalid("run extent overflow"))?;
        if length == 0
            || end > self.bytes
            || self
                .ranges
                .range(..=start)
                .next_back()
                .is_some_and(|(_, &last)| last > start)
            || self
                .ranges
                .range(start..)
                .next()
                .is_some_and(|(&next, _)| next < end)
        {
            return Err(RestoreError::Invalid(
                "run receipt extent overlap or bounds",
            ));
        }
        self.ranges.insert(start, end);
        Ok(())
    }
}
