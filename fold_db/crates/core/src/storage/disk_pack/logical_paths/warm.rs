//! Exact-key shortcuts select existing complete paths before metadata disk reads.

use super::body::PathBody;
use super::*;
use crate::access::AccessContext;
use crate::schema::types::field::{FieldValue, HashRangeFilter};
use crate::schema::types::{key_value::KeyValue, Query};
use crate::storage::disk_pack::read::ReadContext;
use std::collections::{BTreeSet, HashMap};

mod wire;
pub use wire::{remember_wire_query, warm_wire_query, WarmQueryFields};

pub(super) const LINKS: &str = "native_complete_query_fields_v1";
pub(super) type FieldResults = HashMap<String, HashMap<KeyValue, FieldValue>>;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct WireMetadata {
    pub conflict_flags: String,
    pub key_form: Option<String>,
    pub field_flag: Option<serde_json::Value>,
}

/// This call-local directory contains no bodies and cannot outlive its query.
#[derive(Default)]
pub(crate) struct ResolvedPaths {
    paths: BTreeMap<String, BTreeSet<String>>,
    count: usize,
}

impl ResolvedPaths {
    pub(super) fn note(&mut self, field: &str, key: &KeyValue, atom: &str, id: String) {
        let key = path_key(field, key, atom);
        if self.count < 10_000 && self.paths.entry(key).or_default().insert(id) {
            self.count += 1;
        }
    }

    pub(super) fn one(&self, field: &str, key: &KeyValue, atom: &str) -> Option<String> {
        let ids = self.paths.get(&path_key(field, key, atom))?;
        (ids.len() == 1).then(|| ids.first().expect("one path").clone())
    }
}

fn path_key(field: &str, key: &KeyValue, atom: &str) -> String {
    serde_json::to_string(&(field, &key.hash, &key.range, atom)).expect("string tuple")
}

pub(super) fn key(query: &Query) -> Option<KeyValue> {
    if query.fields.is_empty()
        || query.as_of.is_some()
        || query.rehydrate_depth.is_some()
        || query.include_tombstones
        || query.expected_total_count.is_some()
        || query.value_filters.is_some()
        || query.field_predicates.is_some()
        || query.order_by.is_some()
        || query.predicate_limit.is_some()
    {
        return None;
    }
    match query.filter.as_ref()? {
        HashRangeFilter::HashRangeKey { hash, range } => {
            Some(KeyValue::new(Some(hash.clone()), Some(range.clone())))
        }
        // A hash/range-only field returns this exact public key shape. A
        // multi-key group cannot create this alias: its result keys contain
        // both components and therefore do not match the key below.
        HashRangeFilter::HashKey(hash) => Some(KeyValue::new(Some(hash.clone()), None)),
        HashRangeFilter::RangeKey(range) => Some(KeyValue::new(None, Some(range.clone()))),
        _ => None,
    }
}

fn owner_context(access: Option<&AccessContext>) -> bool {
    match access {
        Some(access) => access.is_owner,
        None => true,
    }
}

fn query_alias(query: &Query, access: Option<&AccessContext>) -> StorageResult<String> {
    serde_json::to_string(&(
        query,
        access.and_then(|a| a.db_locator.as_deref()),
        access.and_then(|a| a.storage_prefix.as_deref()),
    ))
    .map(crate::hex::sha256_hex)
    .map_err(|error| serialization(&error))
}

fn alias(query: &str, field: &str) -> String {
    // The query digest has a fixed width. Field bytes cannot create another
    // split, and each retained alias occupies exactly 64 ASCII bytes.
    crate::hex::sha256_hex([query.as_bytes(), field.as_bytes()].concat())
}

pub(crate) fn lookup(
    runtime: Option<Arc<PackRuntime>>,
    query: &Query,
    access: Option<&AccessContext>,
) -> StorageResult<Option<WarmQueryFields>> {
    if !owner_context(access) {
        return Ok(None);
    }
    let Some(key) = key(query) else {
        return Ok(None);
    };
    let Some(runtime) = runtime else {
        return Ok(None);
    };
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(None);
    };
    let query_alias = query_alias(query, access)?;
    // Field fetches use this order too: call budget, then logical paths.
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime
        .logical
        .lock()
        .expect("native complete query fields");
    let mut fields = HashMap::new();
    let mut wire = HashMap::new();
    for field in &query.fields {
        let alias = alias(&query_alias, field);
        let Some((collection, id)) = logical.linked_record_target(LINKS, &alias) else {
            return Ok(None);
        };
        let Some(Some(length)) = logical.fetched_record_bytes(&collection, &id) else {
            return Ok(None);
        };
        context.hold_record(&mut logical, &collection, &id)?;
        call.account_memory_record(id.len(), length).map_err(map)?;
        let Some(Some(bytes)) = logical.fetched_record(&collection, &id) else {
            return Ok(None);
        };
        let body: PathBody = serde_json::from_slice(&bytes).map_err(|e| serialization(&e))?;
        let flags = body.wire.clone();
        let Some(value) = body.field(context.version) else {
            return Ok(None);
        };
        context.paths.lock().expect("native resolved paths").note(
            field,
            &key,
            &value.atom_uuid,
            id,
        );
        fields.insert(field.clone(), HashMap::from([(key.clone(), value)]));
        if let Some(flags) = flags {
            wire.insert(field.clone(), flags);
        }
    }
    laststore::record_logical_warm_hits(fields.len() as u64);
    Ok(Some(WarmQueryFields { fields, wire }))
}

pub(crate) fn remember(
    runtime: Option<Arc<PackRuntime>>,
    query: &Query,
    access: Option<&AccessContext>,
    results: &FieldResults,
) -> StorageResult<()> {
    if !owner_context(access) {
        return Ok(());
    }
    let Some(key) = key(query) else { return Ok(()) };
    let Some(runtime) = runtime else {
        return Ok(());
    };
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let query_alias = query_alias(query, access)?;
    let mut logical = runtime
        .logical
        .lock()
        .expect("native complete query fields");
    for field in &query.fields {
        let Some(value) = results.get(field).and_then(|values| values.get(&key)) else {
            continue;
        };
        let id =
            context
                .paths
                .lock()
                .expect("native resolved paths")
                .one(field, &key, &value.atom_uuid);
        if let Some(id) = id {
            logical.link_record(LINKS, &alias(&query_alias, field), PATH_COLLECTION, &id);
        }
    }
    Ok(())
}
