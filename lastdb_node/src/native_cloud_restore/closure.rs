use super::{
    download, keys::RecoveryKeys, session, RestoreError, RestoreSession, RestoreSource, Result,
};
use cloud_backup_protocol::access::{ReadGrantRequest, ReadSelector};
use cloud_backup_protocol::limits::*;
use cloud_backup_protocol::references::{ReferenceIndex, ReferencePage};
use cloud_backup_protocol::transfer::ObjectReceipt;
use cloud_backup_protocol::{
    Digest, ManifestPage, NativeBackupManifest, NativeFileEntry, ObjectKind, Version,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Clone, Copy)]
pub(super) struct Inventory {
    pub files: u64,
    pub bytes: u64,
}
struct Closure {
    receipts: BTreeMap<[u8; 32], ObjectReceipt>,
    used: BTreeSet<[u8; 32]>,
    paths: BTreeSet<[u8; 32]>,
    charged: u64,
}
impl Closure {
    fn charge(&mut self, bytes: u64) -> Result<()> {
        self.charged = self
            .charged
            .checked_add(bytes)
            .ok_or(RestoreError::Limit("inventory memory"))?;
        // Leave four MiB within the decoded budget for the active metadata page.
        if self.charged > DECODED_VALUES_MAX_BYTES - 4 * 1024 * 1024 {
            return Err(RestoreError::Limit("inventory memory"));
        }
        Ok(())
    }
    fn use_receipt(&mut self, id: &Digest) -> Result<()> {
        let raw = raw_digest(id)?;
        if !self.used.contains(&raw) {
            self.charge(192)?;
            self.used.insert(raw);
        }
        Ok(())
    }
}
fn raw_digest(id: &Digest) -> Result<[u8; 32]> {
    hex::decode(id.as_str())
        .map_err(|_| RestoreError::Invalid("digest"))?
        .try_into()
        .map_err(|_| RestoreError::Invalid("digest"))
}

pub(super) async fn restore(
    session: &RestoreSession,
    source: &dyn RestoreSource,
    keys: &RecoveryKeys,
) -> Result<Inventory> {
    let accepted = session.accepted();
    let request = ReadGrantRequest {
        version: Version::V1,
        job: accepted.job.clone(),
        objects: vec![ReadSelector::Receipt {
            receipt_id: accepted.encrypted_manifest_receipt_id.clone(),
        }],
    };
    let response = source.grants(&request).await?;
    let grant = response.manifest_index_grant_for(&request, accepted)?;
    let path = download::fetch(session, source, grant, true).await?;
    let manifest: NativeBackupManifest = decrypt_json(&path, &keys.content)?;
    manifest.validate_for_accepted(accepted)?;
    if manifest.external_dependency_count.get() != 0 {
        return Err(RestoreError::Unsupported("file dependencies need B2"));
    }
    let mut closure = load_references(session, source).await?;
    let mut total = Inventory { files: 0, bytes: 0 };
    for expected_batch in manifest.pages.chunks(4) {
        let selectors = expected_batch
            .iter()
            .map(|page| ReadSelector::Receipt {
                receipt_id: page.receipt_id.clone(),
            })
            .collect();
        let grants = download::grants(session, source, selectors).await?;
        for (expected, grant) in expected_batch.iter().zip(grants) {
            let receipt = closure
                .receipts
                .get(&raw_digest(&expected.receipt_id)?)
                .ok_or(RestoreError::Invalid("manifest page membership"))?
                .clone();
            manifest.validate_page_receipt(expected.page_index, &receipt)?;
            closure.use_receipt(&receipt.receipt_id)?;
            grant.validate_for_receipt(&receipt)?;
            let path = download::fetch(session, source, &grant, true).await?;
            let page: ManifestPage = decrypt_json(&path, &keys.content)?;
            manifest.validate_page(&page)?;
            if page.context.page_index != expected.page_index {
                return Err(RestoreError::Invalid("manifest page index"));
            }
            if !page.external_dependencies.is_empty() {
                return Err(RestoreError::Unsupported("file dependencies need B2"));
            }
            restore_page(session, source, &mut closure, &page).await?;
            for entry in &page.native_files {
                total.files = total
                    .files
                    .checked_add(1)
                    .ok_or(RestoreError::Limit("native files"))?;
                total.bytes = total
                    .bytes
                    .checked_add(entry.native_bytes.get())
                    .ok_or(RestoreError::Limit("native bytes"))?;
            }
            if total.files > manifest.native_file_count as u64
                || total.bytes > manifest.native_bytes.get()
            {
                return Err(RestoreError::Invalid("native totals"));
            }
        }
    }
    if total.files != manifest.native_file_count as u64
        || total.bytes != manifest.native_bytes.get()
        || closure.used.len() != closure.receipts.len()
    {
        return Err(RestoreError::Invalid("complete manifest closure"));
    }
    Ok(total)
}

async fn load_references(session: &RestoreSession, source: &dyn RestoreSource) -> Result<Closure> {
    let expected = &session.accepted().reference_root;
    let grant = download::grant(
        session,
        source,
        ReadSelector::ReferenceIndex {
            sha256: expected.sha256.clone(),
        },
    )
    .await?;
    if grant.sha256 != expected.sha256 || grant.bytes.get() > MAX_REFERENCE_PAGE_BYTES {
        return Err(RestoreError::Invalid("reference index grant"));
    }
    let path = download::fetch(session, source, &grant, true).await?;
    let index: ReferenceIndex = session::read_json(&path, MAX_REFERENCE_PAGE_BYTES)?;
    if index.scope != session.accepted().job.stream.scope || index.root()? != *expected {
        return Err(RestoreError::Invalid("reference root"));
    }
    let mut closure = Closure {
        receipts: BTreeMap::new(),
        used: BTreeSet::new(),
        paths: BTreeSet::new(),
        charged: 0,
    };
    for descriptors in index.pages.chunks(4) {
        let selectors = descriptors
            .iter()
            .map(|page| ReadSelector::ReferencePage {
                sha256: page.sha256.clone(),
            })
            .collect();
        let grants = download::grants(session, source, selectors).await?;
        for (descriptor, grant) in descriptors.iter().zip(grants) {
            if grant.sha256 != descriptor.sha256 || grant.bytes != descriptor.bytes {
                return Err(RestoreError::Invalid("reference page grant"));
            }
            let path = download::fetch(session, source, &grant, true).await?;
            let page: ReferencePage = session::read_json(&path, MAX_REFERENCE_PAGE_BYTES)?;
            index.validate_page(&page)?;
            for receipt in page.objects {
                // Fixed scalars plus owned map key and conservative BTree node charge.
                closure.charge(512)?;
                let key = raw_digest(&receipt.receipt_id)?;
                // Normalize string capacities to bounded canonical scalar lengths.
                if closure.receipts.insert(key, receipt.clone()).is_some() {
                    return Err(RestoreError::Invalid("duplicate receipt"));
                }
            }
        }
    }
    Ok(closure)
}

async fn restore_page(
    session: &RestoreSession,
    source: &dyn RestoreSource,
    closure: &mut Closure,
    page: &ManifestPage,
) -> Result<()> {
    let mut objects = BTreeSet::new();
    for entry in &page.native_files {
        let path_id = raw_digest(&cloud_backup_protocol::bytes_sha256(
            entry.path.as_str().as_bytes(),
        ))?;
        closure.charge(192)?;
        if !closure.paths.insert(path_id) {
            return Err(RestoreError::Invalid("duplicate native path"));
        }
        let receipt = closure
            .receipts
            .get(&raw_digest(&entry.extent.receipt_id)?)
            .ok_or(RestoreError::Invalid("file receipt"))?;
        let end = entry
            .extent
            .offset
            .get()
            .checked_add(entry.extent.length.get())
            .ok_or(RestoreError::Invalid("file extent"))?;
        if end > receipt.object.bytes.get()
            || !matches!(
                receipt.object.kind,
                ObjectKind::NativeFile | ObjectKind::MetadataPack
            )
            || (receipt.object.kind == ObjectKind::NativeFile
                && (entry.extent.offset.get() != 0 || entry.extent.length != receipt.object.bytes))
        {
            return Err(RestoreError::Invalid("file extent kind or bounds"));
        }
        closure.use_receipt(&entry.extent.receipt_id)?;
        if session.physical().join(entry.path.as_str()).try_exists()? {
            // Resume verifies existing exact bytes before it skips network work.
            install_file(session, Path::new(""), entry)?;
        } else {
            objects.insert(entry.extent.receipt_id.clone());
        }
    }
    let objects: Vec<_> = objects.into_iter().collect();
    for ids in objects.chunks(4) {
        let request = ReadGrantRequest {
            version: Version::V1,
            job: session.accepted().job.clone(),
            objects: ids
                .iter()
                .map(|receipt_id| ReadSelector::Receipt {
                    receipt_id: receipt_id.clone(),
                })
                .collect(),
        };
        let response = source.grants(&request).await?;
        response.validate_for(&request)?;
        for (id, item) in ids.iter().zip(response.items) {
            let cloud_backup_protocol::access::ReadGrantItemResult::Ok { grant, .. } = item else {
                return Err(RestoreError::Source("native object grant"));
            };
            grant.validate_for_receipt(&closure.receipts[&raw_digest(id)?])?;
            let path = download::fetch(session, source, &grant, false).await?;
            for entry in page
                .native_files
                .iter()
                .filter(|entry| &entry.extent.receipt_id == id)
            {
                install_file(session, &path, entry)?;
            }
        }
    }
    Ok(())
}

fn install_file(session: &RestoreSession, object: &Path, entry: &NativeFileEntry) -> Result<()> {
    let path = session.physical().join(entry.path.as_str());
    let mut parent = session.physical();
    let components: Vec<_> = entry.path.as_str().split('/').collect();
    for component in &components[..components.len() - 1] {
        parent.push(component);
        match fs::create_dir(&parent) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(&parent)?.is_dir() {
            return Err(RestoreError::Invalid("native parent"));
        }
    }
    if path.try_exists()? {
        download::verify_file(&path, &entry.native_sha256, entry.native_bytes.get())?;
    } else {
        download::check_capacity(&session.root, entry.native_bytes.get())?;
        let pending = session.root.join("native.pending");
        if pending.try_exists()? && !fs::symlink_metadata(&pending)?.is_file() {
            return Err(RestoreError::Invalid("native temporary"));
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&pending)?;
        let mut input = File::open(object)?;
        input.seek(SeekFrom::Start(entry.extent.offset.get()))?;
        let mut left = entry.native_bytes.get();
        let mut buffer = [0; 65536];
        while left > 0 {
            let count = left.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..count])?;
            output.write_all(&buffer[..count])?;
            left -= count as u64;
        }
        output.sync_all()?;
        download::verify_file(&pending, &entry.native_sha256, entry.native_bytes.get())?;
        fs::rename(&pending, &path)?;
        File::open(&parent)?.sync_all()?;
        File::open(&session.root)?.sync_all()?;
    }
    let ledger = session
        .root
        .join("entries")
        .join(download::path_key(entry.path.as_str()));
    if ledger.try_exists()? {
        let existing: NativeFileEntry = session::read_json(&ledger, 256 * 1024)?;
        if existing != *entry {
            return Err(RestoreError::Invalid("saved native entry"));
        }
    } else {
        session.remove_spool_file(&ledger.with_extension("pending"))?;
        session.reserve_spool(4096)?;
        if let Err(error) = session::atomic_json(&ledger, entry) {
            if ledger.with_extension("pending").try_exists()? {
                session.add_spool_file(&ledger.with_extension("pending"))?;
            }
            return Err(error);
        }
        session.add_spool_file(&ledger)?;
    }
    Ok(())
}

fn decrypt_json<T: serde::de::DeserializeOwned>(path: &Path, key: &[u8; 32]) -> Result<T> {
    let bytes = download::bytes(path, MANIFEST_PAGE_ENVELOPE_MAX_BYTES)?;
    let plain = fold_db::crypto::decrypt_envelope(key, &bytes)
        .map_err(|_| RestoreError::Invalid("manifest encryption"))?;
    if plain.len() as u64 > MANIFEST_PAGE_PLAINTEXT_MAX_BYTES {
        return Err(RestoreError::Limit("manifest plaintext"));
    }
    serde_json::from_slice(&plain).map_err(|_| RestoreError::Invalid("manifest JSON"))
}
