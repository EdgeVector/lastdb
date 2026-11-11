//! Restore the exact paired disk V2 files, verify extent coverage, and open run restore proof.
use super::{
    bounded_bytes, download, inventory::Inventory, keys::RecoveryDecryption, local, native,
    verify_file, ByteBackupReader, NativeFileProof, NativeInventory, RestoreError, RestoreReport,
    RestoreSession, Result, VerifiedNativeContext,
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

pub(super) mod coverage {
    pub(in crate::byte_cloud_restore) use crate::byte_cloud_restore::runs::Coverage;
}

pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    identity_seed: &[u8; 32],
    root: &ClientRoot,
    inventory: Inventory,
) -> Result<RestoreReport> {
    let total = stage(session, client, decryption, root, inventory).await?;
    let context = VerifiedNativeContext {
        root: &session.root,
        destination: &session.selection.destination,
        session_id: session.selection.session_id,
        commit_sha256: &session.accepted().commit_sha256,
        native_root: &root.native_root,
        receipt_name: "byte-cloud-restore-receipt.json",
        receipt_version: 2,
    };
    let proof = verify_stage(session, decryption, root)?;
    context.finish_run(identity_seed, total, &proof)
}

pub(super) async fn stage(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    root: &ClientRoot,
    mut inventory: Inventory,
) -> Result<NativeInventory> {
    let mut coverage = Coverage::new(root, session, &mut inventory)?;
    let mut total = NativeInventory { files: 0, bytes: 0 };
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
                &bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
                decryption.content_key(),
                &session.accepted().begin,
            )
            .map_err(|_| RestoreError::Invalid("run client page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("run page context"))?;
            if page.page_index != expected.page_index {
                return Err(RestoreError::Invalid("run page identity"));
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
    Ok(total)
}

pub(super) fn verify_stage(
    session: &RestoreSession,
    decryption: &RecoveryDecryption,
    root: &ClientRoot,
) -> Result<run_restore::RunRestoreGuard> {
    let data = session.root.join("home/data");
    local::prepare_native_ephemera(&data)?;
    if root.run.cas.is_none() {
        run_restore::install_empty_cas_home(&data, decryption.content_key())
            .map_err(RestoreError::native_proof)?;
    }
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

pub(in crate::byte_cloud_restore) struct Coverage {
    metadata: BTreeSet<[u8; 32]>,
    data: BTreeMap<[u8; 32], Object>,
}
struct Object {
    bytes: u64,
    ranges: BTreeMap<u64, u64>,
}

impl Coverage {
    pub(in crate::byte_cloud_restore) fn new(
        root: &ClientRoot,
        session: &RestoreSession,
        inventory: &mut Inventory,
    ) -> Result<Self> {
        Self::new_pages(&root.pages, session, inventory)
    }

    pub(in crate::byte_cloud_restore) fn new_pages(
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
        if !self.metadata.insert(digest(id)?) {
            return Err(RestoreError::Invalid("duplicate run metadata receipt"));
        }
        Ok(())
    }

    pub(in crate::byte_cloud_restore) fn page(
        &mut self,
        files: &[ClientNativeFile],
        inventory: &mut Inventory,
    ) -> Result<()> {
        self.page_with(files, inventory, |file| {
            run_backup_destination(file.path.as_str())
                .map_err(|_| RestoreError::Invalid("run native path"))
        })
    }

    pub(in crate::byte_cloud_restore) fn page_with(
        &mut self,
        files: &[ClientNativeFile],
        inventory: &mut Inventory,
        destination: impl Fn(&ClientNativeFile) -> Result<Destination>,
    ) -> Result<()> {
        for file in files {
            let destination = destination(file)?;
            for extent in &file.extents {
                let receipt = inventory.receipt(&extent.receipt_id)?;
                let id = digest(&extent.receipt_id)?;
                if self.metadata.contains(&id) || receipt.object.destination != destination {
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

    pub(in crate::byte_cloud_restore) fn finish(&self) -> Result<()> {
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

fn digest(value: &Digest) -> Result<[u8; 32]> {
    hex::decode(value.as_str())
        .map_err(|_| RestoreError::Invalid("run receipt digest"))?
        .try_into()
        .map_err(|_| RestoreError::Invalid("run receipt digest"))
}
