//! Framed complete-record contents and author clocks for signatures.

use super::Mutation;
use crate::canonical::{push_field, CanonicalValue};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl Mutation {
    /// Sign complete contents, the server identity and the allocated author clock.
    /// Field counts, length prefixes and explicit JSON options preserve boundaries.
    #[must_use]
    pub fn content_hash(&self) -> String {
        let mut digest = Sha256::new();
        push_field(&mut digest, b"lastdb-record-authored-content-v2");
        push_field(&mut digest, self.uuid.as_bytes());
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
        json_field(&mut digest, &self.erase_written_before_nanos);
        json_field(&mut digest, &self.imported_written_at);
        digest.update(self.logical_counter.to_be_bytes());
        push_field(&mut digest, self.author_clock_writer_id.as_bytes());
        format!("{:x}", digest.finalize())
    }
}

fn json_field<T: Serialize>(digest: &mut Sha256, value: &T) {
    let bytes = serde_json::to_vec(value).expect("mutation content is serializable");
    push_field(digest, &bytes);
}
