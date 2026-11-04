//! A replacement selects one whole record. No fields or stamps are merged.
use super::{Batch, Definition, Operation, Result};
use lastdb_model::whole_record::{DeleteRecord, Expected, PutRecord, Record, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stored {
    version: u32,
    revision: String,
    alternates: BTreeMap<String, lastdb_model::whole_record::Key>,
    record: Value,
}

impl Definition {
    pub(in super::super) fn decode(&self, bytes: &[u8]) -> Result<Record> {
        let stored: Stored = serde_json::from_slice(bytes)?;
        if stored.version != self.version || stored.revision.is_empty() {
            return Err("record does not match the selected format".into());
        }
        Ok(Record {
            revision: stored.revision,
            alternates: stored.alternates,
            record: (self.decode_payload)(stored.record)?,
        })
    }

    pub(in super::super) fn put(
        &self,
        mutation: PutRecord,
        schema: &Schema,
        previous: Option<&Record>,
        revision: String,
    ) -> Result<Batch> {
        schema.validate_keys(&mutation.key, &mutation.alternates)?;
        let matches = match (&mutation.expected, previous) {
            (Expected::Absent, None) => true,
            (Expected::Revision { revision }, Some(old)) => revision == &old.revision,
            _ => false,
        };
        if !matches {
            return Err("revision conflict; no writes accepted".into());
        }
        let mut operations = self.pointer_changes(
            &mutation.schema,
            &mutation.key,
            previous.map(|old| &old.alternates),
            &mutation.alternates,
        )?;
        let body = serde_json::to_vec(&Stored {
            version: self.version,
            revision,
            alternates: mutation.alternates,
            record: (self.encode_payload)(mutation.record)?,
        })?;
        operations.push(Operation {
            address: self.address(&mutation.schema, &mutation.key)?,
            body: Some(body),
        });
        self.admit(operations)
    }

    pub(in super::super) fn delete(
        &self,
        mutation: &DeleteRecord,
        schema: &Schema,
        previous: &Record,
    ) -> Result<Batch> {
        schema.validate_keys(&mutation.key, &previous.alternates)?;
        if mutation.revision != previous.revision {
            return Err("revision conflict; no writes accepted".into());
        }
        let mut operations = self.pointer_changes(
            &mutation.schema,
            &mutation.key,
            Some(&previous.alternates),
            &BTreeMap::new(),
        )?;
        operations.push(Operation {
            address: self.address(&mutation.schema, &mutation.key)?,
            body: None,
        });
        self.admit(operations)
    }
}
