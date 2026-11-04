//! One framed mutation identity for idempotency, author signatures, and replay order.

use super::Mutation;
use crate::canonical::{push_field, CanonicalValue};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl Mutation {
    /// Hash the complete written content and its author order.
    ///
    /// The domain, field count, and length prefixes preserve every boundary.
    /// Optional JSON fields preserve absence separately from an empty value.
    /// UUID and local acceptance/durability policy remain outside this hash;
    /// the author signature binds UUID separately. Record identity stays unchanged.
    #[must_use]
    pub fn intent_hash(&self) -> String {
        let mut digest = Sha256::new();
        push_field(&mut digest, b"lastdb-record-intent-v1");
        push_field(&mut digest, self.schema_name.as_bytes());
        json_field(&mut digest, &self.mutation_type);
        json_field(&mut digest, &self.key_value);
        json_field(&mut digest, &self.alternates);
        json_field(&mut digest, &self.file);
        json_field(&mut digest, &self.expected);

        let fields: BTreeMap<_, _> = self.record_fields.iter().collect();
        digest.update((fields.len() as u64).to_be_bytes());
        for (name, value) in fields {
            push_field(&mut digest, name.as_bytes());
            json_field(&mut digest, &CanonicalValue(value));
        }
        push_field(&mut digest, self.pub_key.as_bytes());
        json_field(&mut digest, &self.source_file_name);
        let metadata = self
            .metadata
            .as_ref()
            .map(|metadata| metadata.iter().collect::<BTreeMap<_, _>>());
        json_field(&mut digest, &metadata);
        if let Some(cutoff) = self.erase_written_before_nanos {
            push_field(&mut digest, b"lastdb-ttl-erase-cutoff-v1");
            digest.update(cutoff.to_be_bytes());
        }
        format!("{:x}", digest.finalize())
    }
    /// Author signatures bind the complete intent and allocated author clock.
    #[must_use]
    pub fn content_hash(&self) -> String {
        let mut digest = Sha256::new();
        push_field(&mut digest, b"lastdb-record-authored-content-v1");
        push_field(&mut digest, self.intent_hash().as_bytes());
        json_field(&mut digest, &self.imported_written_at);
        digest.update(self.logical_counter.to_be_bytes());
        push_field(&mut digest, self.author_clock_writer_id.as_bytes());
        format!("{:x}", digest.finalize())
    }

    #[must_use]
    pub fn retry_key(&self) -> String {
        let mut digest = Sha256::new();
        push_field(&mut digest, b"lastdb-record-retry-v1");
        push_field(&mut digest, self.pub_key.as_bytes());
        push_field(&mut digest, self.uuid.as_bytes());
        format!("{:x}", digest.finalize())
    }

    pub fn record_stamp(&self) -> Result<crate::record_value::RecordStamp, String> {
        Ok(crate::record_value::RecordStamp {
            written_at: self
                .imported_written_at
                .ok_or("record write lacks an author clock")?,
            logical_counter: self.logical_counter,
            device_id: if self.author_clock_writer_id.is_empty() {
                self.pub_key.clone()
            } else {
                self.author_clock_writer_id.clone()
            },
            mutation_id: self.uuid.clone(),
        })
    }
}

fn json_field<T: Serialize>(digest: &mut Sha256, value: &T) {
    let bytes = serde_json::to_vec(value).expect("mutation content is serializable");
    push_field(digest, &bytes);
}
