//! Sole native write owner. Callers can replace whole records, never raw bytes.
use super::{
    format::{EncodedAddress, Format},
    Result,
};
use lastdb_model::record_replacement::RecordValues;
use laststore::disk_run::home::{HomeMutation, RangeBounds, ReadBudget, RunHome, RunRead};
use rand::RngCore;
use serde::Serialize;
use serde_json::{Map, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Mutex;
use zeroize::Zeroizing;

const MAX_LOOKUP_RECORDS: usize = 64;

pub(super) struct Store<F> {
    native: RunHome,
    format: F,
    replacement: Mutex<()>,
}

#[derive(Serialize)]
pub(super) struct Maintenance {
    steps: u8,
    frontier: u64,
    runs: usize,
    pending: bool,
}

#[derive(Serialize)]
pub(super) struct WriteReceipt {
    pub frontier: u64,
    pub operations: u64,
}

#[derive(Serialize)]
pub(super) struct Lookup {
    frontier: u64,
    records: Vec<LookupRecord>,
}

#[derive(Serialize)]
struct LookupRecord {
    key: String,
    fields: Map<String, Value>,
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
        Ok(Self {
            native,
            format,
            replacement: Mutex::new(()),
        })
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
            replacement: Mutex::new(()),
        })
    }

    pub(super) fn replace(&self, values: &impl RecordValues) -> Result<WriteReceipt> {
        // Serialize the old-record read through the final commit, not just the flush.
        let _replacement = self
            .replacement
            .lock()
            .map_err(|_| "replacement lock poisoned")?;
        let previous = self.read_one(self.format.address(values.kind(), values.key())?)?;
        let operations = self
            .format
            .encode(values, previous.as_deref())?
            .into_operations();
        let operations: Vec<_> = operations
            .into_iter()
            .map(|operation| {
                let (collection, key, body) = operation.into_parts();
                HomeMutation {
                    collection,
                    key,
                    body,
                }
            })
            .collect();
        RunHome::check_mutations(&operations)?;
        // Maintenance fails before this mutation, never after a successful commit.
        self.maintain(2)?;
        // Record, pointer insertion and old pointer tombstone share one CURRENT.
        let committed = self.native.commit(operations)?;
        Ok(WriteReceipt {
            frontier: committed.frontier,
            operations: committed.records,
        })
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
        self.read_one(self.format.address(kind, key)?)?
            .map(|body| self.format.decode(&body))
            .transpose()
    }

    pub(super) fn lookup(&self, kind: &str, value: &str) -> Result<Lookup> {
        let (collection, prefix) = self.format.alternate_range(kind, value)?.into_parts();
        // Both the pointer range and the batch of primary reads use one held CURRENT.
        let mut read = self.read_view();
        let frontier = read.frontier();
        let page = read
            .hash_range(
                &collection,
                &prefix,
                RangeBounds {
                    start: String::new(),
                    end: None,
                },
            )?
            .next_page(MAX_LOOKUP_RECORDS + 1)?;
        if !page.exhausted || page.records.len() > MAX_LOOKUP_RECORDS {
            return Err(format!(
                "spike lookup exceeds {MAX_LOOKUP_RECORDS} records; refusing an incomplete result"
            )
            .into());
        }
        let keys = page
            .records
            .iter()
            .map(|(key, body)| self.format.decode_pointer(kind, value, key, body))
            .collect::<Result<Vec<_>>>()?;
        let coordinates = keys
            .iter()
            .map(|key| {
                self.format
                    .address(kind, key)
                    .map(EncodedAddress::into_parts)
            })
            .collect::<Result<Vec<_>>>()?;
        let bodies =
            read.get_many_bounded(&coordinates, self.format.layout().max_record_bytes as u64)?;
        let records = keys
            .into_iter()
            .zip(bodies)
            .map(|(key, body)| {
                let body = body?.ok_or("alternate points to a missing primary record")?;
                Ok(LookupRecord {
                    key,
                    fields: self.format.decode_lookup(value, &body)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Lookup { frontier, records })
    }

    fn read_one(&self, address: EncodedAddress) -> Result<Option<Vec<u8>>> {
        let (collection, key) = address.into_parts();
        self.read_view()
            .get_bounded(
                &collection,
                &key,
                self.format.layout().max_record_bytes as u64,
            )
            .map_err(Into::into)
    }

    fn read_view(&self) -> RunRead {
        self.native.read(ReadBudget {
            pages: 512,
            blocks: 512,
            bytes: 8 * 1024 * 1024,
            keys: 512,
        })
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
