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
    /// the author signature binds UUID separately. Atom identity stays unchanged.
    #[must_use]
    pub fn content_hash(&self) -> String {
        let mut digest = Sha256::new();
        push_field(&mut digest, b"lastdb-mutation-content-v1");
        push_field(&mut digest, self.schema_name.as_bytes());
        json_field(&mut digest, &self.mutation_type);
        json_field(&mut digest, &self.key_value);

        let fields: BTreeMap<_, _> = self.fields_and_values.iter().collect();
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
        json_field(&mut digest, &self.provenance);
        json_field(&mut digest, &self.imported_written_at);
        digest.update(self.logical_counter.to_be_bytes());
        push_field(&mut digest, self.author_clock_writer_id.as_bytes());
        json_field(&mut digest, &self.imported_version);
        json_field(&mut digest, &self.aggregate_set);
        if let Some(cutoff) = self.erase_tips_before_nanos {
            push_field(&mut digest, b"lastdb-ttl-erase-cutoff-v1");
            digest.update(cutoff.to_be_bytes());
        }
        format!("{:x}", digest.finalize())
    }
}

fn json_field<T: Serialize>(digest: &mut Sha256, value: &T) {
    let bytes = serde_json::to_vec(value).expect("mutation content is serializable");
    push_field(digest, &bytes);
}
