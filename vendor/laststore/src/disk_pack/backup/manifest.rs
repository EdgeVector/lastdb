use super::BackupReceipt;
use crate::disk_pack::corrupt;
use crate::Result;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};

const MANIFEST_BYTES: u64 = 32 * 1024 * 1024;
const MAX_FILES: usize = 65_552;

pub(super) struct Entry {
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
    pub(super) digest: [u8; 32],
}

pub(super) struct Manifest {
    pub(super) generation: u64,
    pub(super) frontier: u64,
    pub(super) entries: Vec<Entry>,
}

impl Manifest {
    fn encode(&self) -> Result<Vec<u8>> {
        if self.entries.len() > MAX_FILES {
            return Err(crate::disk_pack::resource());
        }
        let files: Vec<_> = self
            .entries
            .iter()
            .map(|entry| {
                json!({
                    "path": entry.path, "bytes": entry.bytes, "sha256": hex(&entry.digest)
                })
            })
            .collect();
        let bytes = serde_json::to_vec(&json!({
            "format":"lastdb-native-backup-2", "generation":self.generation,
            "frontier":self.frontier, "files":files
        }))
        .map_err(|_| corrupt("backup manifest encoding"))?;
        if bytes.len() as u64 > MANIFEST_BYTES {
            return Err(crate::disk_pack::resource());
        }
        Ok(bytes)
    }

    pub(super) fn write(&self, root: &Path) -> Result<()> {
        let bytes = self.encode()?;
        super::files::write(root, "BACKUP.json", &bytes)?;
        super::files::write(
            root,
            "BACKUP.sha256",
            hex(&Sha256::digest(&bytes)).as_bytes(),
        )?;
        Ok(())
    }

    pub(super) fn output_bytes(&self) -> Result<u64> {
        let mut directories = BTreeSet::new();
        // The root and the three snapshot namespaces need space,
        // even when no live file currently depends on one of those namespaces.
        let mut bytes = 4 * 4096u64;
        for entry in &self.entries {
            super::files::validate_path(&entry.path)?;
            bytes = allocated_add(bytes, entry.bytes)?;
            let mut parent = entry.path.parent();
            while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
                directories.insert(path.to_path_buf());
                parent = path.parent();
            }
        }
        bytes = allocated_add(bytes, self.encode()?.len() as u64)?;
        bytes = allocated_add(bytes, 64)?;
        bytes
            .checked_add(directories.len() as u64 * 4096)
            .ok_or_else(|| corrupt("backup output quote overflow"))
    }

    pub(super) fn read(root: &Path) -> Result<Self> {
        let file = super::files::open(root, Path::new("BACKUP.json"))?;
        if file.metadata()?.len() > MANIFEST_BYTES {
            return Err(crate::disk_pack::resource());
        }
        let mut bytes = Vec::new();
        file.take(MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MANIFEST_BYTES {
            return Err(crate::disk_pack::resource());
        }
        let mut digest = [0; 64];
        let mut sum_file = super::files::open(root, Path::new("BACKUP.sha256"))?;
        if sum_file.metadata()?.len() != 64 {
            return Err(corrupt("backup manifest checksum length"));
        }
        sum_file.read_exact(&mut digest)?;
        if digest.as_slice() != hex(&Sha256::digest(&bytes)).as_bytes() {
            return Err(corrupt("backup manifest checksum"));
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| corrupt("backup manifest JSON"))?;
        if value["format"] != "lastdb-native-backup-2" {
            return Err(corrupt("backup format"));
        }
        let rows = value["files"]
            .as_array()
            .ok_or_else(|| corrupt("backup file entries"))?;
        if rows.len() > MAX_FILES {
            return Err(crate::disk_pack::resource());
        }
        let mut entries = Vec::with_capacity(rows.len());
        let mut names = BTreeSet::new();
        for row in rows {
            let path = PathBuf::from(
                row["path"]
                    .as_str()
                    .ok_or_else(|| corrupt("backup file path"))?,
            );
            super::files::validate_path(&path)?;
            if !names.insert(path.clone()) {
                return Err(corrupt("duplicate backup file"));
            }
            let bytes = number(row, "bytes")?;
            if bytes == 0 || bytes > crate::disk_pack::PART_BYTES {
                return Err(corrupt("backup file exceeds part limit"));
            }
            let encoded = row["sha256"]
                .as_str()
                .ok_or_else(|| corrupt("backup file checksum"))?;
            let digest = decode_digest(encoded)?;
            entries.push(Entry {
                path,
                bytes,
                digest,
            });
        }
        Ok(Self {
            generation: number(&value, "generation")?,
            frontier: number(&value, "frontier")?,
            entries,
        })
    }

    pub(super) fn receipt(&self) -> BackupReceipt {
        BackupReceipt {
            generation: self.generation,
            frontier: self.frontier,
            files: self.entries.len() as u64,
            bytes: self.entries.iter().map(|entry| entry.bytes).sum(),
        }
    }
}

fn allocated_add(total: u64, bytes: u64) -> Result<u64> {
    total
        .checked_add(
            bytes
                .div_ceil(4096)
                .checked_mul(4096)
                .ok_or_else(|| corrupt("backup allocation quote overflow"))?,
        )
        .ok_or_else(|| corrupt("backup output quote overflow"))
}

fn number(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .ok_or_else(|| corrupt("backup unsigned integer"))
}

pub(super) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

fn decode_digest(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(corrupt("backup checksum encoding"));
    }
    let mut out = [0; 32];
    for (slot, pair) in out.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(corrupt("backup checksum digit")),
        };
        *slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(out)
}
