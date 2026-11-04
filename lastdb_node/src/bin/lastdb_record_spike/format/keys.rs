//! Framing keeps external keys independent of payload fields.
use super::{Address, Batch, Definition, Operation, Result};
use lastdb_model::whole_record::{Key, Schema};

impl Definition {
    pub(in super::super) fn address(&self, schema: &str, key: &Key) -> Result<Address> {
        self.framed(self.records, &(schema, key))
    }

    pub(in super::super) fn schema_address(&self, name: &str) -> Result<Address> {
        self.framed(self.schemas, &name)
    }

    fn framed(&self, collection: &str, value: &impl serde::Serialize) -> Result<Address> {
        let key = serde_json::to_string(value)?;
        if key.len() > self.max_key_bytes {
            return Err("encoded address exceeds the key limit".into());
        }
        Ok(Address {
            collection: collection.into(),
            key,
        })
    }

    pub(in super::super) fn install_schema(&self, name: &str, schema: &Schema) -> Result<Batch> {
        if name.is_empty() {
            return Err("schema name must be nonempty".into());
        }
        schema.validate()?;
        self.admit(vec![Operation {
            address: self.schema_address(name)?,
            body: Some(serde_json::to_vec(schema)?),
        }])
    }
}
