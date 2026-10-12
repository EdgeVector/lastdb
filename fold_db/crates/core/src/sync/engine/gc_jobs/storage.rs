//! Exact packed receipt access. Legacy numbered files remain readable.

use super::*;
use std::collections::BTreeMap;

pub(super) fn block_path(directory: &Path, sequence: u64) -> PathBuf {
    directory.join(format!(
        "objects-v1-{:08}.pack",
        (sequence - 1) / u64::from(packed::BLOCK_SLOTS)
    ))
}

pub(super) fn loose_raw(path: &Path) -> SyncResult<Option<Vec<u8>>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    if !metadata.is_file() || metadata.len() > u64::from(packed::MAX_RECORD_BYTES) {
        return Err(io_error("invalid loose object receipt"));
    }
    match std::fs::read(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

impl GcJobManager {
    pub(super) fn object_raw(&self, id: &str, sequence: u64) -> SyncResult<Option<Vec<u8>>> {
        if sequence == 0 || sequence > u64::from(packed::MAX_SLOTS) {
            return Err(io_error("receipt sequence exceeds archive bound"));
        }
        let directory = self
            .job_path(id)?
            .parent()
            .expect("job directory")
            .to_path_buf();
        let archive = directory.join("objects-v1.pack");
        if archive.exists() {
            let (mut file, header) = packed::open(&archive, id)?;
            if header.first != 1 {
                return Err(io_error("invalid terminal archive range"));
            }
            return packed::read_raw(&mut file, header, sequence);
        }
        let block = block_path(&directory, sequence);
        if block.exists() {
            match packed::open(&block, id).and_then(|(mut file, header)| {
                let first = (sequence - 1) / u64::from(packed::BLOCK_SLOTS)
                    * u64::from(packed::BLOCK_SLOTS)
                    + 1;
                if header.first != first || header.slots != packed::BLOCK_SLOTS {
                    return Err(io_error("invalid receipt block range"));
                }
                packed::read_raw(&mut file, header, sequence)
            }) {
                Ok(Some(raw)) => return Ok(Some(raw)),
                Ok(None) => {}
                Err(_) if archive.exists() => {
                    let (mut file, header) = packed::open(&archive, id)?;
                    return packed::read_raw(&mut file, header, sequence);
                }
                Err(error) => return Err(error),
            }
        }
        let raw = loose_raw(&self.object_path(id, sequence)?)?;
        if raw.is_none() && archive.exists() {
            // A terminal compactor can publish between the first lookup and
            // legacy-file removal. Retry that exact archive; never enumerate.
            let (mut file, header) = packed::open(&archive, id)?;
            return packed::read_raw(&mut file, header, sequence);
        }
        Ok(raw)
    }

    pub(super) fn read_object(&self, id: &str, sequence: u64) -> SyncResult<GcObjectReceipt> {
        let raw = self
            .object_raw(id, sequence)?
            .ok_or_else(|| io_error("object receipt missing"))?;
        packed::decode(&raw, id, sequence)
    }

    pub(super) fn read_objects(
        &self,
        id: &str,
        sequences: &[u64],
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        let archive = self.job_path(id)?.with_file_name("objects-v1.pack");
        if archive.exists() {
            let (mut file, header) = packed::open(&archive, id)?;
            if header.first != 1 {
                return Err(io_error("invalid terminal archive range"));
            }
            return sequences
                .iter()
                .map(|sequence| {
                    let raw = packed::read_raw(&mut file, header, *sequence)?
                        .ok_or_else(|| io_error("object receipt missing"))?;
                    packed::decode(&raw, id, *sequence)
                })
                .collect();
        }
        sequences
            .iter()
            .map(|sequence| self.read_object(id, *sequence))
            .collect()
    }

    pub(super) fn put_objects(&self, id: &str, objects: &[GcObjectReceipt]) -> SyncResult<()> {
        let directory = self
            .job_path(id)?
            .parent()
            .expect("job directory")
            .to_path_buf();
        if directory.join("objects-v1.pack").exists() {
            return Err(io_error("terminal receipt archive is immutable"));
        }
        let mut changes: BTreeMap<u64, BTreeMap<u64, Vec<u8>>> = BTreeMap::new();
        for object in objects {
            if object.job_id != id
                || object.sequence == 0
                || object.sequence > u64::from(packed::MAX_SLOTS)
            {
                return Err(io_error("object receipt has invalid identity"));
            }
            let mut object = object.clone();
            object.version = object.format_version();
            let block = (object.sequence - 1) / u64::from(packed::BLOCK_SLOTS);
            changes.entry(block).or_default().insert(
                object.sequence,
                serde_json::to_vec(&object).map_err(io_error)?,
            );
        }
        for (block, updates) in changes {
            let first = block * u64::from(packed::BLOCK_SLOTS) + 1;
            let mut records = BTreeMap::new();
            let path = block_path(&directory, first);
            if path.exists() {
                let (mut file, header) = packed::open(&path, id)?;
                if header.first != first || header.slots != packed::BLOCK_SLOTS {
                    return Err(io_error("invalid receipt block range"));
                }
                for sequence in first..first + u64::from(packed::BLOCK_SLOTS) {
                    if let Some(raw) = packed::read_raw(&mut file, header, sequence)? {
                        records.insert(sequence, raw);
                    }
                }
            } else {
                for sequence in first..first + u64::from(packed::BLOCK_SLOTS) {
                    if let Some(raw) = loose_raw(&self.object_path(id, sequence)?)? {
                        records.insert(sequence, raw);
                    }
                }
            }
            records.extend(updates);
            packed::publish(
                &block_path(&directory, first),
                id,
                first,
                packed::BLOCK_SLOTS,
                records.into_iter().map(Ok),
            )?;
        }
        Ok(())
    }
}
