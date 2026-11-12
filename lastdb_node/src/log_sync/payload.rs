//! Record decode for a cloud entry that is already stored.
//!
//! This is not the local capture path. A file, a file write, an inline
//! write, a schema definition, or a contribution is not represented here.
//! New local writes stay in the native commit. See `accepted`.

use std::collections::{BTreeMap, HashMap};

use cloud_backup_protocol::canonical_bytes;
use fold_db::schema::types::key_value::KeyValue;
use fold_db::schema::types::{CasExpectation, Mutation, MutationType};
use mutation_log::Namespace;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RecordPayload {
    pub schema: String,
    pub action: String,
    pub key: KeyValue,
    pub record: BTreeMap<String, Value>,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub expected: CasExpectation,
    pub mutation_id: String,
    pub pub_key: String,
}

pub(crate) fn decode(bytes: &[u8]) -> Option<RecordPayload> {
    let payload: RecordPayload = serde_json::from_slice(bytes).ok()?;
    let again = canonical_bytes(&payload).ok()?;
    if again != bytes {
        return None;
    }
    if action_name_str(&payload.action).is_none() || namespace_of(&payload.schema).is_none() {
        return None;
    }
    if payload.mutation_id.is_empty() || payload.mutation_id.contains(['/', '\\']) {
        return None;
    }
    Some(payload)
}

pub(crate) fn to_mutation(payload: &RecordPayload) -> Option<Mutation> {
    let mutation_type = action_name_str(&payload.action)?;
    let mut record_fields = HashMap::with_capacity(payload.record.len());
    for (key, value) in &payload.record {
        record_fields.insert(key.clone(), value.clone());
    }
    Some(Mutation {
        uuid: payload.mutation_id.clone(),
        schema_name: payload.schema.clone(),
        record_fields,
        key_value: payload.key.clone(),
        alternates: payload.alternates.clone(),
        file: None,
        pub_key: payload.pub_key.clone(),
        mutation_type,
        synchronous: None,
        source_file_name: None,
        metadata: None,
        imported_written_at: None,
        logical_counter: 0,
        author_clock_writer_id: String::new(),
        author_clock_signature: String::new(),
        author_clock_signature_version: 0,
        expected: payload.expected.clone(),
        erase_caller: None,
        erase_written_before_nanos: None,
        file_write: None,
        inline_write: None,
        definition_id: None,
        contribution: None,
    })
}

pub(crate) fn namespace_of(schema: &str) -> Option<Namespace> {
    let namespace = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
    Namespace::new(namespace).ok()
}

fn action_name_str(action: &str) -> Option<MutationType> {
    match action {
        "create" => Some(MutationType::Create),
        "replace" => Some(MutationType::Replace),
        "delete" => Some(MutationType::Delete),
        _ => None,
    }
}
