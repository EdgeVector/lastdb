use super::{shared, GenerationHold, GenerationInventory};
use crate::disk_pack::bytes::*;
use crate::disk_pack::page_write::{encode_page, PageFields};
use crate::disk_pack::{artifact, corrupt, generation, Current, PAGE_BYTES};
use crate::{durability, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const MAX_REFERENCES: usize = 1024;

/// A durable source-generation hold. Pending conversion outputs use their own receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReferencePurpose {
    /// An incomplete physical backup must retain its complete source generation.
    Backup = 1,
    /// An explicitly selected rollback point, never inferred from its age.
    Rollback = 2,
    /// A retained source for an independently managed cloud recovery operation.
    CloudRecovery = 4,
    /// Published source boundary retained during conversion (lifetime feature v1).
    ConversionSource = 5,
}

/// Verified durable reference identity. Expiry never authorizes file deletion.
#[derive(Clone, Debug)]
pub struct ReferenceInfo {
    /// Exact reference filename identity.
    pub identity: Uuid,
    /// Selected immutable generation.
    pub generation: u64,
    /// Matching generation receipt object.
    pub receipt: Uuid,
    /// Required retention purpose.
    pub purpose: ReferencePurpose,
    /// Unix milliseconds at durable creation.
    pub created_ms: u64,
    /// Unix milliseconds after which new work cannot acquire this reference; zero means no expiry.
    pub expiry_ms: u64,
    /// Digest of the complete selected CURRENT.
    pub current_digest: [u8; 32],
}

/// Named retention survives process exit. Drop does not release it.
pub struct DurableReference {
    root: PathBuf,
    info: ReferenceInfo,
    current: Current,
}

impl DurableReference {
    /// Sync a complete named generation reference before external work starts.
    pub fn create(
        root: &Path,
        current: &Current,
        purpose: ReferencePurpose,
        expiry_ms: u64,
    ) -> Result<Self> {
        let _hold = GenerationHold::acquire(root, current)?;
        let generation = generation::verify(root, current)?;
        let parent = root.join("references");
        if !parent.try_exists()? {
            crate::disk_pack::page_write::directory(&parent)?;
        }
        if list_unlocked(root)?.len() >= MAX_REFERENCES {
            return Err(crate::disk_pack::resource());
        }
        let now = artifact::unix_ms();
        if expiry_ms != 0 && expiry_ms <= now {
            return Err(corrupt("reference already expired"));
        }
        let info = ReferenceInfo {
            identity: Uuid::new_v4(),
            generation: current.generation,
            receipt: generation.object,
            purpose,
            created_ms: now,
            expiry_ms,
            current_digest: digest(&current.encode()?),
        };
        let temporary = root.join(format!("REFERENCE.pending-{}", info.identity));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&encode(&info)?)?;
        durability::sync_dirty_file(&file)?;
        std::fs::rename(temporary, parent.join(info.identity.to_string()))?;
        durability::sync_directory(&File::open(parent)?)?;
        durability::sync_directory(&File::open(root)?)?;
        Ok(Self {
            root: root.to_path_buf(),
            info,
            current: current.clone(),
        })
    }

    /// Reopen named work. Expired work stays retained but cannot resume implicitly.
    pub fn open(root: &Path, identity: Uuid) -> Result<Self> {
        let _hold = shared(root)?;
        let info = read(root, identity)?;
        if info.expiry_ms != 0 && info.expiry_ms <= artifact::unix_ms() {
            return Err(corrupt("reference expired; explicit release is required"));
        }
        let current = validate(root, &info)?;
        Ok(Self {
            root: root.to_path_buf(),
            info,
            current,
        })
    }

    /// Reference identity for a durable owner receipt.
    pub fn info(&self) -> &ReferenceInfo {
        &self.info
    }

    /// Selected immutable root, independent of the live CURRENT.
    pub fn current(&self) -> &Current {
        &self.current
    }

    /// Keep every file protected for this active copy or restore operation.
    pub fn hold(&self) -> Result<GenerationHold> {
        let hold = GenerationHold::acquire(&self.root, &self.current)?;
        if read(&self.root, self.info.identity)?.current_digest != self.info.current_digest {
            return Err(corrupt("durable reference changed"));
        }
        Ok(hold)
    }

    /// Verify exact backup paths while a temporary call hold protects them.
    pub fn inventory(&self) -> Result<GenerationInventory> {
        self.hold()?.inventory()
    }

    /// Explicit owner release, including expired work. No automatic expiry deletion.
    pub fn release(self) -> Result<()> {
        release_reference(&self.root, self.info.identity)
    }
}

/// Release abandoned named work by its exact identity, with a directory barrier.
pub fn release_reference(root: &Path, identity: Uuid) -> Result<()> {
    let _hold = shared(root)?;
    read(root, identity)?;
    std::fs::remove_file(root.join("references").join(identity.to_string()))?;
    durability::sync_directory(&File::open(root.join("references"))?)
}

/// Bounded named references. Expired entries remain in this live retention set.
pub fn list_references(root: &Path) -> Result<Vec<ReferenceInfo>> {
    let _hold = shared(root)?;
    list_unlocked(root)
}

pub(in crate::disk_pack) fn list_unlocked(root: &Path) -> Result<Vec<ReferenceInfo>> {
    let parent = root.join("references");
    if !parent.try_exists()? {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        if out.len() >= MAX_REFERENCES {
            return Err(crate::disk_pack::resource());
        }
        if !entry.file_type()?.is_file() {
            return Err(corrupt("reference is not a regular file"));
        }
        let identity = entry
            .file_name()
            .to_str()
            .and_then(|name| Uuid::parse_str(name).ok())
            .ok_or_else(|| corrupt("invalid reference filename"))?;
        let info = read(root, identity)?;
        validate(root, &info)?;
        out.push(info);
    }
    Ok(out)
}

pub(super) fn validate(root: &Path, info: &ReferenceInfo) -> Result<Current> {
    let current = generation::load_current(root, info.generation)?;
    if digest(&current.encode()?) != info.current_digest
        || generation::read_receipt(root, &current)?.object != info.receipt
    {
        return Err(corrupt("reference differs from generation receipt"));
    }
    Ok(current)
}

fn encode(info: &ReferenceInfo) -> Result<Vec<u8>> {
    let mut payload = [0; 128];
    payload[..16].copy_from_slice(info.identity.as_bytes());
    put64(&mut payload, 16, info.generation);
    payload[24..40].copy_from_slice(info.receipt.as_bytes());
    payload[48] = info.purpose as u8;
    put64(&mut payload, 56, info.created_ms);
    put64(&mut payload, 64, info.expiry_ms);
    payload[72..104].copy_from_slice(&info.current_digest);
    encode_page(
        info.identity,
        info.generation,
        0,
        PageFields {
            kind: 12,
            level: 0,
            count: 1,
            width: 128,
            left: 0,
            next: 0,
            auxiliary: 0,
        },
        &payload,
    )
}

fn read(root: &Path, identity: Uuid) -> Result<ReferenceInfo> {
    nonnil(identity)?;
    let path = root.join("references").join(identity.to_string());
    if !std::fs::symlink_metadata(&path)?.is_file() {
        return Err(corrupt("reference is not a regular file"));
    }
    let mut file = File::open(path)?;
    if file.metadata()?.len() != PAGE_BYTES as u64 {
        return Err(corrupt("reference page length"));
    }
    let mut bytes = vec![0; PAGE_BYTES];
    file.read_exact(&mut bytes)?;
    verify_checksum(&bytes, 80)?;
    if &bytes[..8] != b"LDPAGE1\0"
        || u16_at(&bytes, 8) != 1
        || bytes[10..12] != [12, 0]
        || u16_at(&bytes, 12) != 128
        || bytes[14..16] != [0, 0]
        || uuid_at(&bytes, 24) != identity
        || u64_at(&bytes, 40) != 0
        || u32_at(&bytes, 48) != 128
        || u16_at(&bytes, 52) != 1
        || u16_at(&bytes, 54) != 128
    {
        return Err(corrupt("reference page identity"));
    }
    zero(&bytes[56..80])?;
    zero(&bytes[112..128])?;
    zero(&bytes[256..])?;
    let payload = &bytes[128..256];
    if uuid_at(payload, 0) != identity || u64_at(&bytes, 16) != u64_at(payload, 16) {
        return Err(corrupt("reference generation identity"));
    }
    zero(&payload[40..48])?;
    zero(&payload[49..56])?;
    zero(&payload[104..])?;
    let purpose = match payload[48] {
        1 => ReferencePurpose::Backup,
        2 => ReferencePurpose::Rollback,
        4 => ReferencePurpose::CloudRecovery,
        5 => ReferencePurpose::ConversionSource,
        _ => return Err(corrupt("unsupported durable reference purpose")),
    };
    Ok(ReferenceInfo {
        identity,
        generation: u64_at(payload, 16),
        receipt: nonnil(uuid_at(payload, 24))?,
        purpose,
        created_ms: u64_at(payload, 56),
        expiry_ms: u64_at(payload, 64),
        current_digest: payload[72..104].try_into().expect("reference digest"),
    })
}
