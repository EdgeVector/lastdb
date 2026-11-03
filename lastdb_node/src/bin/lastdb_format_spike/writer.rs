//! Sole native write owner. Callers can replace whole records, never raw bytes.
use super::{format, Result};
use lastdb_model::record_replacement::RecordValues;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHome};
use rand::RngCore;
use serde_json::{Map, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use zeroize::Zeroizing;

pub(super) struct Store {
    native: RunHome,
}

impl Store {
    pub(super) fn create(root: &Path) -> Result<Self> {
        // Creation cannot open or modify an existing LastDB or other directory.
        fs::DirBuilder::new().mode(0o700).create(root)?;
        let mut key = Zeroizing::new([0_u8; 32]);
        rand::rngs::OsRng.fill_bytes(&mut *key);
        private_file(&root.join(format::HOME.identity), key.as_ref())?;
        let native = RunHome::create(&root.join(format::HOME.data), &key)?;
        private_file(&root.join(format::HOME.marker), &format::marker()?)?;
        File::open(root)?.sync_all()?;
        Ok(Self { native })
    }

    pub(super) fn open(root: &Path) -> Result<Self> {
        if !fs::symlink_metadata(root)?.is_dir() {
            return Err("spike home must be a directory, not a link".into());
        }
        if fs::read(root.join(format::HOME.marker))? != format::marker()? {
            return Err("home does not have the selected spike format".into());
        }
        let mut key = Zeroizing::new([0_u8; 32]);
        let mut identity = File::open(root.join(format::HOME.identity))?;
        identity.read_exact(&mut *key)?;
        if identity.read(&mut [0_u8])? != 0 {
            return Err("invalid spike identity size".into());
        }
        Ok(Self {
            native: RunHome::open(&root.join(format::HOME.data), &key)?,
        })
    }

    pub(super) fn replace(&self, values: &impl RecordValues) -> Result<u64> {
        let (key, body) = format::encode(values)?.into_parts();
        // No old-value read, field merge, or independent child commit.
        let committed = self.native.commit(vec![HomeMutation {
            collection: format::HOME.collection.to_owned(),
            key,
            body: Some(body),
        }])?;
        Ok(committed.frontier)
    }

    pub(super) fn get(&self, kind: &str, key: &str) -> Result<Option<Map<String, Value>>> {
        let encoded = format::key(kind, key)?;
        let mut read = self.native.read(ReadBudget {
            pages: 256,
            blocks: 256,
            bytes: 8 * 1024 * 1024,
            keys: 1,
        });
        read.get(format::HOME.collection, &encoded)?
            .map(|bytes| format::decode(&bytes))
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
