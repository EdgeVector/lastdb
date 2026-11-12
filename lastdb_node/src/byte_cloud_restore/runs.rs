//! Verified object inventory, extent coverage, and run restore proof.
use super::{
    bounded_bytes, download, local, verify_file, ByteBackupReader, NativeFileProof,
    RecoveryDecryption, RestoreError, RestoreSession, Result,
};
use cloud_backup_protocol::limits::DECODED_VALUES_MAX_BYTES;
use cloud_backup_protocol::v2::{
    access::ReadSelector,
    inventory::{InventoryIndex, InventoryPage, InventoryVerifier},
    limits::{INVENTORY_JSON_MAX_BYTES, MAX_BATCH_ITEMS},
    transfer::ObjectReceipt,
    Destination,
};
use cloud_backup_protocol::{bytes_sha256, Digest};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::{
        run_backup_destination, ClientPage, ClientPageReference, ClientRoot,
        CODEC_ENVELOPE_MAX_BYTES,
    },
    run_restore,
};
use fold_db::storage_policy::backup_decode::{
    IDENTITY_MEMORY_BYTES, JSON_PAGE_RESERVE_BYTES, RECEIPT_MEMORY_BYTES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Default)]
pub(super) struct Inventory {
    pub receipts: BTreeMap<[u8; 32], ObjectReceipt>,
    used: BTreeSet<[u8; 32]>,
    paths: BTreeSet<[u8; 32]>,
    charged: u64,
}

impl Inventory {
    pub async fn load(session: &RestoreSession, client: &ByteBackupReader) -> Result<Self> {
        let accepted = session.accepted();
        let sha256 = accepted.inventory_root.sha256.clone();
        let grant = download::one(session, client, ReadSelector::InventoryIndex { sha256 }).await?;
        let path = download::fetch(session, client, &grant, true).await?;
        let index: InventoryIndex = local::read_json(&path, INVENTORY_JSON_MAX_BYTES)?;
        if index.scope != accepted.job().stream.scope || index.root()? != accepted.inventory_root {
            return Err(RestoreError::Invalid("inventory root"));
        }
        let mut verifier = InventoryVerifier::new(&index, accepted.root_receipt_id.clone())?;
        let mut inv = Self::default();
        for batch in index.pages.chunks(MAX_BATCH_ITEMS) {
            let sels = batch
                .iter()
                .map(|p| ReadSelector::InventoryPage {
                    sha256: p.sha256.clone(),
                })
                .collect();
            let grants = download::grants(session, client, sels).await?;
            for (desc, grant) in batch.iter().zip(grants) {
                if (grant.sha256.as_str(), grant.bytes) != (desc.sha256.as_str(), desc.bytes) {
                    return Err(RestoreError::Invalid("inventory page grant"));
                }
                let path = download::fetch(session, client, &grant, true).await?;
                let page: InventoryPage = local::read_json(&path, INVENTORY_JSON_MAX_BYTES)?;
                verifier.push(&page)?;
                for receipt in page.objects {
                    inv.charge(RECEIPT_MEMORY_BYTES)?;
                    if inv
                        .receipts
                        .insert(digest_bytes(&receipt.receipt_id)?, receipt)
                        .is_some()
                    {
                        return Err(RestoreError::Invalid("duplicate inventory receipt"));
                    }
                }
            }
        }
        accepted.validate_inventory(&verifier.finish()?)?;
        inv.use_receipt(&accepted.root_receipt_id).map(|()| inv)
    }

    pub fn receipt(&self, id: &Digest) -> Result<&ObjectReceipt> {
        self.receipts
            .get(&digest_bytes(id)?)
            .ok_or(RestoreError::Invalid("receipt inventory membership"))
    }

    pub fn use_receipt(&mut self, id: &Digest) -> Result<()> {
        let key = digest_bytes(id)?;
        if !self.receipts.contains_key(&key) {
            return Err(RestoreError::Invalid("receipt inventory membership"));
        }
        if !self.used.contains(&key) {
            self.charge(IDENTITY_MEMORY_BYTES)?;
            self.used.insert(key);
        }
        Ok(())
    }

    pub fn path(&mut self, identity: &[u8]) -> Result<()> {
        self.charge(IDENTITY_MEMORY_BYTES)?;
        self.paths
            .insert(digest_bytes(&bytes_sha256(identity))?)
            .then_some(())
            .ok_or(RestoreError::Invalid("duplicate native path or dependency"))
    }

    pub fn finish(&self) -> Result<()> {
        (self.used.len() == self.receipts.len())
            .then_some(())
            .ok_or(RestoreError::Invalid("complete used receipt set"))
    }

    pub(super) fn charge(&mut self, bytes: u64) -> Result<()> {
        self.charged = self
            .charged
            .checked_add(bytes)
            .filter(|&n| n <= DECODED_VALUES_MAX_BYTES - JSON_PAGE_RESERVE_BYTES)
            .ok_or(RestoreError::Limit("inventory memory"))?;
        Ok(())
    }
}

pub(super) fn digest_bytes(id: &Digest) -> Result<[u8; 32]> {
    hex::decode(id.as_str())
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or(RestoreError::Invalid("digest"))
}

pub(super) async fn stage_and_verify(
    session: &RestoreSession,
    client: &ByteBackupReader,
    decryption: &RecoveryDecryption,
    root: &ClientRoot,
    inventory: Inventory,
) -> Result<run_restore::RunRestoreGuard> {
    let (mut files, mut bytes) = (0u64, 0u64);
    let (data, key) = (session.root.join("home/data"), &decryption.content);
    stage_pages(
        session,
        client,
        &root.pages,
        inventory,
        &data,
        |index, raw| {
            let page = ClientPage::decode(raw, key, &session.accepted().begin)
                .map_err(|_| RestoreError::Invalid("run client page"))?;
            root.validate_page(&page)
                .map_err(|_| RestoreError::Invalid("run page context"))?;
            if page.page_index != index {
                return Err(RestoreError::Invalid("run page identity"));
            }
            files = files
                .checked_add(page.native_files.len() as u64)
                .ok_or(RestoreError::Limit("run native file count"))?;
            for f in &page.native_files {
                bytes = bytes
                    .checked_add(f.native_bytes.get())
                    .ok_or(RestoreError::Limit("run native byte count"))?;
            }
            if files > root.native_file_count || bytes > root.native_bytes {
                return Err(RestoreError::Invalid("run native totals"));
            }
            page.native_files
                .into_iter()
                .map(|f| {
                    let d = run_backup_destination(f.path.as_str())
                        .map_err(|_| RestoreError::Invalid("run native path"))?;
                    Ok((f, d))
                })
                .collect()
        },
    )
    .await?;
    let totals = (files, bytes);
    if totals != (root.native_file_count, root.native_bytes) {
        return Err(RestoreError::Invalid("complete run native totals"));
    }
    local::prepare_native_ephemera(&data)?;
    run_restore::install_empty_cas_home(&data, key).map_err(RestoreError::native_proof)?;
    let proof = run_restore::verify(&data, &root.run, &root.native_root, key)
        .map_err(RestoreError::native_proof)?;
    proof
        .validate_context(&data, &root.native_root)
        .map_err(RestoreError::native_proof)?;
    for file in proof.files() {
        let path = session
            .root
            .join("entries")
            .join(local::path_key(&file.path));
        let entry: NativeFileProof = local::read_json(&path, 256 * 1024)?;
        if entry.path != file.path || entry.bytes != file.bytes {
            return Err(RestoreError::Invalid("run exact native inventory"));
        }
        verify_file(&data.join(&file.path), &entry.sha256, file.bytes)?;
    }
    if proof.totals().map_err(RestoreError::native_proof)? != totals {
        return Err(RestoreError::Invalid("verified run inventory"));
    }
    Ok(proof)
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
    for batch in pages.chunks(MAX_BATCH_ITEMS) {
        let ids: Vec<_> = batch.iter().map(|p| p.receipt_id.clone()).collect();
        let grants = download::receipt_grants(session, client, &inventory, &ids).await?;
        for (expected, grant) in batch.iter().zip(grants) {
            inventory.use_receipt(&expected.receipt_id)?;
            let path = download::fetch(session, client, &grant, true).await?;
            let typed = decode(
                expected.page_index,
                &bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?,
            )?;
            coverage.add_page(&typed, &mut inventory)?;
            let files: Vec<_> = typed.into_iter().map(|(f, _)| f).collect();
            local::restore_at(session, client, &mut inventory, &files, target).await?;
        }
    }
    coverage.finish()?;
    inventory.finish()
}

#[derive(Default)]
struct Coverage {
    metadata: BTreeSet<[u8; 32]>,
    data: BTreeMap<[u8; 32], (u64, BTreeMap<u64, u64>)>,
}

impl Coverage {
    fn new_pages(
        pages: &[ClientPageReference],
        session: &RestoreSession,
        inventory: &mut Inventory,
    ) -> Result<Self> {
        let mut value = Self::default();
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
        inventory.charge(IDENTITY_MEMORY_BYTES)?;
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
            for ext in &file.extents {
                let (receipt, id) = (
                    inventory.receipt(&ext.receipt_id)?,
                    digest_bytes(&ext.receipt_id)?,
                );
                if self.metadata.contains(&id) || receipt.object.destination != *destination {
                    return Err(RestoreError::Invalid("run native receipt destination"));
                }
                let bytes = receipt.object.bytes.get();
                let (obj_bytes, ranges) = match self.data.entry(id) {
                    std::collections::btree_map::Entry::Vacant(e) => {
                        inventory.charge(IDENTITY_MEMORY_BYTES)?;
                        e.insert((bytes, BTreeMap::new()))
                    }
                    std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                };
                let (start, length) = (ext.offset.get(), ext.length.get());
                let end = start
                    .checked_add(length)
                    .ok_or(RestoreError::Invalid("run extent overflow"))?;
                if ranges.get(&start) == Some(&end) {
                    continue;
                }
                inventory.charge(IDENTITY_MEMORY_BYTES)?;
                let overlaps = ranges
                    .range(..=start)
                    .next_back()
                    .is_some_and(|(_, &l)| l > start)
                    || ranges.range(start..).next().is_some_and(|(&n, _)| n < end);
                if length == 0 || end > *obj_bytes || overlaps {
                    return Err(RestoreError::Invalid(
                        "run receipt extent overlap or bounds",
                    ));
                }
                ranges.insert(start, end);
            }
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        for (bytes, ranges) in self.data.values() {
            let mut cursor = 0;
            for (&start, &end) in ranges {
                if start != cursor {
                    return Err(RestoreError::Invalid("run receipt coverage gap"));
                }
                cursor = end;
            }
            if cursor != *bytes {
                return Err(RestoreError::Invalid("run complete receipt coverage"));
            }
        }
        Ok(())
    }
}
