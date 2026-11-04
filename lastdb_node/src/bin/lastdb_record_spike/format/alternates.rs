//! Every alternate is a nonunique pointer. Its explicit value lives on the record.
use super::{Address, Definition, Operation, Result};
use lastdb_model::whole_record::{Key, Record};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pointer {
    version: u32,
    primary: Key,
}

impl Definition {
    pub(in super::super) fn alternate_prefix(
        &self,
        schema: &str,
        name: &str,
        key: &Key,
    ) -> Result<(String, String)> {
        let prefix = format!("{}\0", serde_json::to_string(&(schema, name, key))?);
        if prefix.len() > self.max_key_bytes {
            return Err("alternate address exceeds key limit".into());
        }
        Ok((self.alternates.into(), prefix))
    }

    fn pointer_address(
        &self,
        schema: &str,
        name: &str,
        key: &Key,
        primary: &Key,
    ) -> Result<Address> {
        let (collection, prefix) = self.alternate_prefix(schema, name, key)?;
        Ok(Address {
            collection,
            key: format!("{prefix}{}", serde_json::to_string(primary)?),
        })
    }

    pub(super) fn pointer_changes(
        &self,
        schema: &str,
        primary: &Key,
        before: Option<&BTreeMap<String, Key>>,
        after: &BTreeMap<String, Key>,
    ) -> Result<Vec<Operation>> {
        let mut operations = Vec::new();
        for (name, old) in before.into_iter().flatten() {
            if after.get(name) != Some(old) {
                operations.push(Operation {
                    address: self.pointer_address(schema, name, old, primary)?,
                    body: None,
                });
            }
        }
        for (name, key) in after {
            operations.push(Operation {
                address: self.pointer_address(schema, name, key, primary)?,
                body: Some(serde_json::to_vec(&Pointer {
                    version: self.version,
                    primary: primary.clone(),
                })?),
            });
        }
        Ok(operations)
    }

    pub(in super::super) fn pointer(
        &self,
        schema: &str,
        name: &str,
        matched: &Key,
        address: &str,
        bytes: &[u8],
    ) -> Result<Key> {
        let pointer: Pointer = serde_json::from_slice(bytes)?;
        if pointer.version != self.version
            || self
                .pointer_address(schema, name, matched, &pointer.primary)?
                .key
                != address
        {
            return Err("invalid alternate pointer".into());
        }
        Ok(pointer.primary)
    }

    pub(in super::super) fn verify_match(
        &self,
        record: &Record,
        name: &str,
        key: &Key,
    ) -> Result<()> {
        if record.alternates.get(name) != Some(key) {
            return Err("alternate pointer disagrees with the stored record".into());
        }
        Ok(())
    }
}
