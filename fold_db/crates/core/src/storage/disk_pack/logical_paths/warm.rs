//! Exact-key shortcuts select existing complete paths before metadata disk reads.

use super::body::PathBody;
use super::*;
use crate::access::AccessContext;
use crate::schema::types::field::{FieldValue, HashRangeFilter};
use crate::schema::types::{key_value::KeyValue, Query};
use crate::storage::disk_pack::read::ReadContext;
use std::collections::{BTreeSet, HashMap};

mod keys;
mod lookup;
mod projection;
mod refresh;
mod wire;
pub(crate) use keys::covers_window;
use keys::keys;
pub use wire::{remember_wire_query, warm_wire_query, WarmQueryFields};

pub(super) const LINKS: &str = "native_complete_query_fields_v1";
pub(super) type FieldResults = HashMap<String, HashMap<KeyValue, FieldValue>>;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WireMetadata {
    #[serde(default)]
    pub query: String,
    pub version: super::body::ReadVersion,
    pub witness: Option<u64>,
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

fn row_alias(query: &str, key: &KeyValue) -> String {
    crate::hex::sha256_hex(
        serde_json::to_string(&(query, &key.hash, &key.range)).expect("public key tuple"),
    )
}

pub(crate) fn lookup(
    runtime: Option<Arc<PackRuntime>>,
    query: &Query,
    access: Option<&AccessContext>,
) -> StorageResult<Option<WarmQueryFields>> {
    if !owner_context(access)
        || runtime
            .as_ref()
            .is_some_and(|runtime| transaction::PendingBatch::current(runtime).is_some())
    {
        return Ok(None);
    }
    let Some(keys) = keys(query) else {
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
    let Some((records, bytes, renewal)) =
        lookup::ready(&context, &call, &logical, query, &query_alias, &keys)
    else {
        return Ok(None);
    };
    if !projection::optional_fits(&call, records, bytes) {
        return Ok(None);
    }
    let mut refresh = refresh::Refresh::new(&context, &call, records, bytes, renewal);
    if !projection::optional_record(
        &mut call,
        0,
        keys.iter()
            .map(|key| {
                key.hash.as_ref().map_or(0, String::len) + key.range.as_ref().map_or(0, String::len)
            })
            .sum(),
    )? {
        return Ok(None);
    }
    let mut result = WarmQueryFields {
        fields: HashMap::new(),
        wire: HashMap::new(),
    };
    let mut paths = 0usize;
    for key in keys {
        let Some(row) = lookup::row(
            &context,
            &mut call,
            &mut logical,
            query,
            &query_alias,
            &key,
            &mut refresh,
        )?
        else {
            return Ok(None);
        };
        paths += row.fields.len();
        if paths > 10_000 {
            return Ok(None);
        }
        for (field, values) in row.fields {
            result.fields.entry(field).or_default().extend(values);
        }
        for (field, flags) in row.wire {
            if result
                .wire
                .get(&field)
                .is_some_and(|previous| previous != &flags)
            {
                return Ok(None);
            }
            result.wire.insert(field, flags);
        }
    }
    if !result.complete_wire_metadata() {
        return Ok(None);
    }
    laststore::record_logical_warm_hits(paths as u64);
    Ok(Some(result))
}

pub(crate) fn remember(
    runtime: Option<Arc<PackRuntime>>,
    query: &Query,
    access: Option<&AccessContext>,
    results: &FieldResults,
) -> StorageResult<()> {
    if !owner_context(access)
        || runtime
            .as_ref()
            .is_some_and(|runtime| transaction::PendingBatch::current(runtime).is_some())
    {
        return Ok(());
    }
    let Some(keys) = keys(query) else {
        return Ok(());
    };
    let Some(runtime) = runtime else {
        return Ok(());
    };
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    projection::remember(&runtime, &context, query, access, &keys, results)
}
