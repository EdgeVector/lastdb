//! The sole native write owner. Only admitted format batches reach commit.
use super::{
    format::{Address, Batch, Definition, LookupRecord},
    Result,
};
use lastdb_model::whole_record::{DeleteRecord, Key, PutRecord, Record, Schema};
use laststore::disk_run::home::{HomeMutation, RangeBounds, ReadBudget, RunHome, RunRead};
use rand::RngCore;
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Mutex;
use zeroize::Zeroizing;

pub(super) struct Store {
    native: RunHome,
    format: Definition,
    mutation: Mutex<()>,
}

#[derive(Serialize)]
pub(super) struct Receipt {
    frontier: u64,
    operations: u64,
    revision: Option<String>,
}

#[derive(Serialize)]
pub(super) struct Lookup {
    frontier: u64,
    records: Vec<LookupRecord>,
}

impl Store {
    pub(super) fn create(root: &Path, format: Definition) -> Result<Self> {
        fs::DirBuilder::new().mode(0o700).create(root)?;
        let mut key = Zeroizing::new([0_u8; 32]);
        rand::rngs::OsRng.fill_bytes(&mut *key);
        private_file(&root.join(format.identity), key.as_ref())?;
        let native = RunHome::create(&root.join(format.data), &key)?;
        private_file(&root.join(format.marker), &format.marker()?)?;
        File::open(root)?.sync_all()?;
        let parent = root.parent().filter(|p| !p.as_os_str().is_empty());
        File::open(parent.unwrap_or_else(|| Path::new(".")))?.sync_all()?;
        Ok(Self {
            native,
            format,
            mutation: Mutex::new(()),
        })
    }

    pub(super) fn open(root: &Path, format: Definition) -> Result<Self> {
        if !fs::symlink_metadata(root)?.is_dir()
            || fs::read(root.join(format.marker))? != format.marker()?
        {
            return Err("not a home for the selected spike format".into());
        }
        let mut key = Zeroizing::new([0_u8; 32]);
        let mut identity = File::open(root.join(format.identity))?;
        identity.read_exact(&mut *key)?;
        if identity.read(&mut [0_u8])? != 0 {
            return Err("invalid identity size".into());
        }
        Ok(Self {
            native: RunHome::open(&root.join(format.data), &key)?,
            format,
            mutation: Mutex::new(()),
        })
    }

    pub(super) fn install_schema(&self, name: &str, schema: &Schema) -> Result<Receipt> {
        let _guard = self.mutation.lock().map_err(|_| "mutation lock poisoned")?;
        if self.read_one(self.format.schema_address(name)?)?.is_some() {
            return Err("schema already exists; spike schemas are immutable".into());
        }
        self.commit(self.format.install_schema(name, schema)?, None)
    }

    pub(super) fn put(&self, mutation: PutRecord) -> Result<Receipt> {
        let _guard = self.mutation.lock().map_err(|_| "mutation lock poisoned")?;
        let schema = self.schema(&mutation.schema)?;
        let previous = self.get(&mutation.schema, &mutation.key)?;
        let revision = uuid::Uuid::new_v4().to_string();
        let batch = self
            .format
            .put(mutation, &schema, previous, revision.clone())?;
        self.commit(batch, Some(revision))
    }

    pub(super) fn delete(&self, mutation: &DeleteRecord) -> Result<Receipt> {
        let _guard = self.mutation.lock().map_err(|_| "mutation lock poisoned")?;
        let schema = self.schema(&mutation.schema)?;
        let previous = self
            .get(&mutation.schema, &mutation.key)?
            .ok_or("record is absent")?;
        self.commit(self.format.delete(mutation, &schema, previous)?, None)
    }

    pub(super) fn get(&self, schema: &str, key: &Key) -> Result<Option<Record>> {
        let definition = self.schema(schema)?;
        definition.primary.validate(key)?;
        let record = self
            .read_one(self.format.address(schema, key)?)?
            .map(|bytes| self.format.decode(&bytes))
            .transpose()?;
        if let Some(record) = &record {
            definition.validate_keys(key, &record.alternates)?;
        }
        Ok(record)
    }

    pub(super) fn lookup(&self, schema: &str, name: &str, key: &Key) -> Result<Lookup> {
        let definition = self.schema(schema)?;
        definition
            .alternates
            .get(name)
            .ok_or("unknown alternate key")?
            .validate(key)?;
        let (collection, prefix) = self.format.alternate_prefix(schema, name, key)?;
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
            .next_page(65)?;
        if !page.exhausted || page.records.len() > 64 {
            return Err("spike lookup limit is 64; refusing an incomplete reply".into());
        }
        let keys = page
            .records
            .iter()
            .map(|(address, body)| self.format.pointer(schema, name, key, address, body))
            .collect::<Result<Vec<_>>>()?;
        let addresses = keys
            .iter()
            .map(|primary| {
                definition.primary.validate(primary)?;
                self.format
                    .address(schema, primary)
                    .map(Address::into_parts)
            })
            .collect::<Result<Vec<_>>>()?;
        let bodies = read.get_many_bounded(&addresses, self.format.max_record_bytes as u64)?;
        let records = keys
            .into_iter()
            .zip(bodies)
            .map(|(primary, body)| {
                let value = self
                    .format
                    .decode(&body?.ok_or("dangling alternate pointer")?)?;
                definition.validate_keys(&primary, &value.alternates)?;
                self.format.verify_match(&value, name, key)?;
                Ok(LookupRecord {
                    key: primary,
                    value,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Lookup { frontier, records })
    }

    pub(super) fn maintain(&self, maximum: u8) -> Result<serde_json::Value> {
        let mut steps = 0;
        for _ in 0..maximum {
            if self.native.merge_step()?.is_none() {
                break;
            }
            steps += 1;
        }
        let status = self.native.status();
        Ok(serde_json::json!({
            "steps": steps, "frontier": status.frontier,
            "runs": status.runs(), "pending": self.native.needs_merge()
        }))
    }

    fn schema(&self, name: &str) -> Result<Schema> {
        let bytes = self
            .read_one(self.format.schema_address(name)?)?
            .ok_or("unknown schema")?;
        let schema: Schema = serde_json::from_slice(&bytes)?;
        schema.validate()?;
        Ok(schema)
    }

    fn commit(&self, batch: Batch, revision: Option<String>) -> Result<Receipt> {
        let operations = batch
            .into_parts()
            .map(|(collection, key, body)| HomeMutation {
                collection,
                key,
                body,
            })
            .collect::<Vec<_>>();
        RunHome::check_mutations(&operations)?;
        self.maintain(2)?;
        let committed = self.native.commit(operations)?;
        Ok(Receipt {
            frontier: committed.frontier,
            operations: committed.records,
            revision,
        })
    }

    fn read_one(&self, address: Address) -> Result<Option<Vec<u8>>> {
        let (collection, key) = address.into_parts();
        Ok(self
            .read_view()
            .get_bounded(&collection, &key, self.format.max_record_bytes as u64)?)
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
