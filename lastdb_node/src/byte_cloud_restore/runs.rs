//! Verified object inventory, extent coverage, and native file installation.
use super::{
    check_capacity, ensure_directory, fetch_bytes, fetch_object, grants, open_private,
    prepare_native_ephemera, sync_dir, verify_file, ByteBackupReader, GuestSecrets,
    RestoreError as E, RestoreSession, Result,
};
use cloud_backup_protocol::limits::DECODED_VALUES_MAX_BYTES;
use cloud_backup_protocol::v2::{
    access::{ReadGrant, ReadSelector},
    inventory::{InventoryIndex, InventoryPage, InventoryVerifier},
    limits::{INVENTORY_JSON_MAX_BYTES, MAX_BATCH_ITEMS},
    transfer::ObjectReceipt,
    Destination,
};
use cloud_backup_protocol::{bytes_sha256, Digest};
use fold_db::cloud_backup_plan::ClientNativeFile;
use fold_db::storage::disk_pack::{
    byte_backup::{ClientPage, ClientPageReference, ClientRoot, CODEC_ENVELOPE_MAX_BYTES},
    run_restore,
};
use fold_db::storage_policy::backup_decode::{
    IDENTITY_MEMORY_BYTES, JSON_PAGE_RESERVE_BYTES, RECEIPT_MEMORY_BYTES,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

type CoverageMap = BTreeMap<[u8; 32], (u64, BTreeMap<u64, u64>)>;

#[derive(Default)]
pub(super) struct Inventory {
    pub receipts: BTreeMap<[u8; 32], (ObjectReceipt, u8)>,
    paths: BTreeSet<[u8; 32]>,
    charged: u64,
}

impl Inventory {
    pub async fn load(session: &RestoreSession, client: &ByteBackupReader) -> Result<Self> {
        let accepted = &session.accepted;
        let sel = ReadSelector::InventoryIndex {
            sha256: accepted.inventory_root.sha256.clone(),
        };
        let grant = grants(session, client, vec![sel])
            .await?
            .pop()
            .ok_or(E::Invalid("object grant count"))?;
        let index: InventoryIndex = fetch_json(client, &grant).await?;
        if index.scope != accepted.job().stream.scope || index.root()? != accepted.inventory_root {
            return Err(E::Invalid("inventory root"));
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
            for (desc, grant) in batch.iter().zip(grants(session, client, sels).await?) {
                if (grant.sha256.as_str(), grant.bytes) != (desc.sha256.as_str(), desc.bytes) {
                    return Err(E::Invalid("inventory page grant"));
                }
                let page: InventoryPage = fetch_json(client, &grant).await?;
                verifier.push(&page)?;
                for r in page.objects {
                    inv.charge(RECEIPT_MEMORY_BYTES)?;
                    if inv
                        .receipts
                        .insert(digest_bytes(&r.receipt_id)?, (r, 0))
                        .is_some()
                    {
                        return Err(E::Invalid("duplicate inventory receipt"));
                    }
                }
            }
        }
        accepted.validate_inventory(&verifier.finish()?)?;
        inv.mark(&accepted.root_receipt_id, true)?;
        Ok(inv)
    }

    pub fn receipt(&self, id: &Digest) -> Result<&ObjectReceipt> {
        self.receipts
            .get(&digest_bytes(id)?)
            .map(|(r, _)| r)
            .ok_or(E::Invalid("receipt inventory membership"))
    }

    fn mark(&mut self, id: &Digest, meta: bool) -> Result<([u8; 32], &ObjectReceipt)> {
        let key = digest_bytes(id)?;
        let (r, s) = self
            .receipts
            .get_mut(&key)
            .ok_or(E::Invalid("receipt inventory membership"))?;
        if r.object.destination != Destination::R2 || (meta && *s != 0) || (!meta && *s == 1) {
            return Err(E::Invalid("run receipt destination or kind"));
        }
        *s = if meta { 1 } else { 2 };
        Ok((key, r))
    }

    pub fn path(&mut self, identity: &[u8]) -> Result<()> {
        self.charge(IDENTITY_MEMORY_BYTES)?;
        self.paths
            .insert(digest_bytes(&bytes_sha256(identity))?)
            .then_some(())
            .ok_or(E::Invalid("duplicate native path or dependency"))
    }

    pub fn finish(&self) -> Result<()> {
        self.receipts
            .values()
            .all(|(_, s)| *s != 0)
            .then_some(())
            .ok_or(E::Invalid("complete used receipt set"))
    }

    pub fn charge(&mut self, bytes: u64) -> Result<()> {
        self.charged = self
            .charged
            .checked_add(bytes)
            .filter(|&n| n <= DECODED_VALUES_MAX_BYTES - JSON_PAGE_RESERVE_BYTES)
            .ok_or(E::Limit("inventory memory"))?;
        Ok(())
    }
}

async fn fetch_json<T: serde::de::DeserializeOwned>(
    client: &ByteBackupReader,
    grant: &ReadGrant,
) -> Result<T> {
    serde_json::from_slice(&fetch_bytes(client, grant, INVENTORY_JSON_MAX_BYTES).await?)
        .map_err(|_| E::Invalid("metadata JSON"))
}

async fn receipt_grants(
    session: &RestoreSession,
    client: &ByteBackupReader,
    inventory: &Inventory,
    ids: &[Digest],
) -> Result<Vec<ReadGrant>> {
    let sels = ids
        .iter()
        .cloned()
        .map(|receipt_id| ReadSelector::Receipt { receipt_id })
        .collect();
    let batch = grants(session, client, sels).await?;
    for (id, grant) in ids.iter().zip(&batch) {
        grant.validate_for_receipt(inventory.receipt(id)?)?;
    }
    Ok(batch)
}

pub(super) fn snapshot_closure(
    session: &RestoreSession,
    secrets: &GuestSecrets,
    bytes: &[u8],
) -> Result<ClientRoot> {
    let root = ClientRoot::decode(bytes, &secrets.key, &session.accepted.begin)
        .map_err(|_| E::Invalid("client native metadata"))?;
    (root.recovery == secrets.recovery)
        .then_some(root)
        .ok_or(E::Invalid("recovery public key"))
}

pub(super) async fn load_closure(
    session: &RestoreSession,
    client: &ByteBackupReader,
) -> Result<(Inventory, Vec<u8>)> {
    let id = &session.accepted.root_receipt_id;
    let inventory = Inventory::load(session, client).await?;
    let grant = receipt_grants(session, client, &inventory, std::slice::from_ref(id))
        .await?
        .pop()
        .ok_or(E::Invalid("object grant count"))?;
    Ok((
        inventory,
        fetch_bytes(client, &grant, CODEC_ENVELOPE_MAX_BYTES).await?,
    ))
}

fn digest_bytes(id: &Digest) -> Result<[u8; 32]> {
    hex::decode(id.as_str())
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or(E::Invalid("digest"))
}

pub(super) async fn stage_and_verify(
    session: &RestoreSession,
    client: &ByteBackupReader,
    secrets: &GuestSecrets,
    root: &ClientRoot,
    inventory: &mut Inventory,
) -> Result<run_restore::RunNodeGuard> {
    let (data, key) = (session.root.join("data"), &secrets.key);
    stage_pages(
        session,
        client,
        &root.pages,
        (root.native_file_count, root.native_bytes),
        inventory,
        &data,
        |index, raw| {
            let page = ClientPage::decode(raw, key, &session.accepted.begin)
                .map_err(|_| E::Invalid("run client page"))?;
            root.validate_page(&page, index)
                .map(|()| page.native_files)
                .map_err(|_| E::Invalid("run page context"))
        },
    )
    .await?;
    prepare_native_ephemera(&data, key)?;
    run_restore::verify(&data, &root.run, &root.native_root, key).map_err(E::native_proof)
}

pub(super) async fn stage_pages(
    session: &RestoreSession,
    client: &ByteBackupReader,
    pages: &[ClientPageReference],
    totals: (u64, u64),
    inventory: &mut Inventory,
    target: &Path,
    mut decode: impl FnMut(u32, &[u8]) -> Result<Vec<ClientNativeFile>>,
) -> Result<()> {
    for page in pages {
        let r = &inventory.mark(&page.receipt_id, true)?.1.object;
        if (&r.sha256, r.bytes) != (&page.sha256, page.bytes) {
            return Err(E::Invalid("run page receipt"));
        }
    }
    let (mut data, mut files_n, mut bytes_n) = (CoverageMap::new(), 0u64, 0u64);
    for batch in pages.chunks(MAX_BATCH_ITEMS) {
        let ids: Vec<_> = batch.iter().map(|p| p.receipt_id.clone()).collect();
        let gbatch = receipt_grants(session, client, inventory, &ids).await?;
        for (exp, grant) in batch.iter().zip(gbatch) {
            let raw = fetch_bytes(client, &grant, CODEC_ENVELOPE_MAX_BYTES).await?;
            let files = decode(exp.page_index, &raw)?;
            files_n = files_n
                .checked_add(files.len() as u64)
                .ok_or(E::Limit("native file count"))?;
            for f in &files {
                bytes_n = bytes_n
                    .checked_add(f.native_bytes.get())
                    .ok_or(E::Limit("native byte count"))?;
            }
            if files_n > totals.0 || bytes_n > totals.1 {
                return Err(E::Invalid("native file totals"));
            }
            add_coverage(&mut data, &files, inventory)?;
            restore_at(session, client, inventory, &files, target).await?;
        }
    }
    if (files_n, bytes_n) != totals {
        return Err(E::Invalid("complete native totals"));
    }
    check_coverage(&data)?;
    inventory.finish()
}

async fn restore_at(
    session: &RestoreSession,
    client: &ByteBackupReader,
    inventory: &mut Inventory,
    entries: &[ClientNativeFile],
    root: &Path,
) -> Result<()> {
    let mut ids = BTreeSet::new();
    for entry in entries {
        let rel = entry.path.as_str();
        inventory.path(format!("native:{rel}").as_bytes())?;
        ids.extend(entry.extents.iter().map(|e| e.receipt_id.clone()));
        if let Some((dir, _)) = rel.rsplit_once('/') {
            let mut parent = root.to_path_buf();
            for part in dir.split('/') {
                parent.push(part);
                ensure_directory(&parent)?;
            }
        }
        check_capacity(&session.root, entry.native_bytes.get())?;
        open_private(&root.join(rel), true, false)?;
    }
    let ids: Vec<_> = ids.into_iter().collect();
    for batch in ids.chunks(MAX_BATCH_ITEMS) {
        let gbatch = receipt_grants(session, client, inventory, batch).await?;
        for (id, grant) in batch.iter().zip(gbatch) {
            let object = fetch_object(session, client, &grant).await?;
            let copied = copy_object(session, &object, id, entries, root);
            let _ = fs::remove_file(&object);
            copied?;
        }
    }
    for entry in entries {
        let path = root.join(entry.path.as_str());
        verify_file(&path, &entry.native_sha256, entry.native_bytes.get())?;
        sync_dir(path.parent().ok_or(E::Invalid("native parent"))?)?;
    }
    Ok(())
}

fn copy_object(
    session: &RestoreSession,
    object: &Path,
    id: &Digest,
    entries: &[ClientNativeFile],
    root: &Path,
) -> Result<()> {
    let mut input = File::open(object)?;
    for entry in entries {
        let mut offset = 0u64;
        for ext in &entry.extents {
            let len = ext.length.get();
            if ext.receipt_id == *id {
                check_capacity(&session.root, len)?;
                let mut out = open_private(&root.join(entry.path.as_str()), false, false)?;
                out.seek(SeekFrom::Start(offset))?;
                input.seek(SeekFrom::Start(ext.offset.get()))?;
                if io::copy(&mut (&mut input).take(len), &mut out)? != len {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
                }
                out.sync_all()?;
            }
            offset += len;
        }
    }
    Ok(())
}

fn add_coverage(
    data: &mut CoverageMap,
    files: &[ClientNativeFile],
    inventory: &mut Inventory,
) -> Result<()> {
    for ext in files.iter().flat_map(|f| &f.extents) {
        let (id, bytes) = {
            let (id, r) = inventory.mark(&ext.receipt_id, false)?;
            (id, r.object.bytes.get())
        };
        if let std::collections::btree_map::Entry::Vacant(v) = data.entry(id) {
            inventory.charge(IDENTITY_MEMORY_BYTES)?;
            v.insert((bytes, BTreeMap::new()));
        }
        let (obj_bytes, ranges) = data.get_mut(&id).expect("inserted");
        let (start, len) = (ext.offset.get(), ext.length.get());
        let end = start
            .checked_add(len)
            .ok_or(E::Invalid("run extent overflow"))?;
        if ranges.get(&start) == Some(&end) {
            continue;
        }
        inventory.charge(IDENTITY_MEMORY_BYTES)?;
        let overlaps = ranges
            .range(..=start)
            .next_back()
            .is_some_and(|(_, &l)| l > start)
            || ranges.range(start..).next().is_some_and(|(&n, _)| n < end);
        if len == 0 || end > *obj_bytes || overlaps {
            return Err(E::Invalid("run receipt extent overlap or bounds"));
        }
        ranges.insert(start, end);
    }
    Ok(())
}

fn check_coverage(data: &CoverageMap) -> Result<()> {
    for (bytes, ranges) in data.values() {
        let mut cursor = 0;
        for (&start, &end) in ranges {
            if start != cursor {
                return Err(E::Invalid("run receipt coverage gap"));
            }
            cursor = end;
        }
        if cursor != *bytes {
            return Err(E::Invalid("run complete receipt coverage"));
        }
    }
    Ok(())
}
