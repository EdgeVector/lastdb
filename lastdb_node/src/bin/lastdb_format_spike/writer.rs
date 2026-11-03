//! Sole native write owner. Callers can replace whole records, never raw bytes.
use super::{format::Format, Result};
use lastdb_model::record_replacement::RecordValues;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHome};
use rand::RngCore;
use serde::Serialize;
use serde_json::{Map, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use zeroize::Zeroizing;

pub(super) struct Store<F> {
    native: RunHome,
    format: F,
}

#[derive(Serialize)]
pub(super) struct Maintenance {
    steps: u8,
    frontier: u64,
    runs: usize,
    pending: bool,
}

impl<F: Format> Store<F> {
    pub(super) fn create(root: &Path, format: F) -> Result<Self> {
        let layout = format.layout();
        // Creation cannot open or modify an existing LastDB or other directory.
        fs::DirBuilder::new().mode(0o700).create(root)?;
        let mut key = Zeroizing::new([0_u8; 32]);
        rand::rngs::OsRng.fill_bytes(&mut *key);
        private_file(&root.join(layout.identity), key.as_ref())?;
        let native = RunHome::create(&root.join(layout.data), &key)?;
        private_file(&root.join(layout.marker), &format.marker()?)?;
        File::open(root)?.sync_all()?;
        let parent = root.parent().filter(|p| !p.as_os_str().is_empty());
        File::open(parent.unwrap_or_else(|| Path::new(".")))?.sync_all()?;
        Ok(Self { native, format })
    }

    pub(super) fn open(root: &Path, format: F) -> Result<Self> {
        let layout = format.layout();
        if !fs::symlink_metadata(root)?.is_dir() {
            return Err("spike home must be a directory, not a link".into());
        }
        if fs::read(root.join(layout.marker))? != format.marker()? {
            return Err("home does not have the selected spike format".into());
        }
        let mut key = Zeroizing::new([0_u8; 32]);
        let mut identity = File::open(root.join(layout.identity))?;
        identity.read_exact(&mut *key)?;
        if identity.read(&mut [0_u8])? != 0 {
            return Err("invalid spike identity size".into());
        }
        Ok(Self {
            native: RunHome::open(&root.join(layout.data), &key)?,
            format,
        })
    }

    pub(super) fn replace(&self, values: &impl RecordValues) -> Result<u64> {
        let (collection, key, body) = self.format.encode(values)?.into_parts();
        // Maintenance fails before this mutation, never after a successful commit.
        self.maintain(2)?;
        // One final commit includes the parent's envelope and the nested block.
        let committed = self.native.commit(vec![HomeMutation {
            collection,
            key,
            body: Some(body),
        }])?;
        Ok(committed.frontier)
    }

    pub(super) fn maintain(&self, maximum: u8) -> Result<Maintenance> {
        let mut steps = 0;
        for _ in 0..maximum {
            if self.native.merge_step()?.is_none() {
                break;
            }
            steps += 1;
        }
        let status = self.native.status();
        Ok(Maintenance {
            steps,
            frontier: status.frontier,
            runs: status.runs(),
            pending: self.native.needs_merge(),
        })
    }

    pub(super) fn get(&self, kind: &str, key: &str) -> Result<Option<Map<String, Value>>> {
        let (collection, encoded) = self.format.address(kind, key)?.into_parts();
        let mut read = self.native.read(ReadBudget {
            pages: 256,
            blocks: 256,
            bytes: 8 * 1024 * 1024,
            keys: 1,
        });
        read.get_bounded(
            &collection,
            &encoded,
            self.format.layout().max_record_bytes as u64,
        )?
        .map(|bytes| self.format.decode(&bytes))
        .transpose()
    }
}

fn private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
