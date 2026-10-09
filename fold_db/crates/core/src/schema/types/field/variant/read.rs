use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::resident::{
    ResidentGraph, ResidentKeySetCompleteness, ResidentKeySetSnapshot, ResidentMoleculeKey,
};
use crate::schema::types::field::{HashRangeFilter, KeyWindow, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;

use super::super::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use super::{FieldKind, FieldValue, FieldVariant};

mod collect;
#[path = "partition_window.rs"]
mod partition_window;
mod refresh;
mod resolve;

/// One live field slot from an authoritative, hydrated hash-partition walk.
///
/// Unlike the public query resolver, this internal form never drops a key
/// whose tip or atom body cannot resolve. Aggregate repair uses that stronger
/// contract before it certifies a summary generation.
#[derive(Debug, Clone)]
pub(crate) struct AuthoritativeFieldMatch {
    pub key: KeyValue,
    pub entry: crate::atom::AtomEntry,
    pub value: serde_json::Value,
}

/// Map storage-form range (OPE hex) → API plaintext for query responses.
fn api_key_value_from_storage(kv: KeyValue) -> KeyValue {
    match kv.range.as_deref() {
        Some(r) if !r.is_empty() => {
            let range = crate::crypto::E2eKeys::ope_decode_range_plaintext(r)
                .unwrap_or_else(|| r.to_string());
            KeyValue::new(kv.hash, Some(range))
        }
        _ => kv,
    }
}

/// The complete interval requested by a keyed query. Pages have no certificate.
fn partition_interval(filter: &HashRangeFilter) -> Option<(&str, String, Option<String>)> {
    match filter {
        HashRangeFilter::HashKey(hash) => Some((hash, String::new(), None)),
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Some((hash, start.clone(), Some(end.max(start).clone())))
        }
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            let mut upper = prefix.clone();
            let end = loop {
                let Some(ch) = upper.pop() else { break None };
                let next = match ch as u32 + 1 {
                    0xD800 => 0xE000,
                    n => n,
                };
                if let Some(next) = char::from_u32(next) {
                    upper.push(next);
                    break Some(upper);
                }
            };
            Some((hash, prefix.clone(), end))
        }
        _ => None,
    }
}

/// Select the key interval before cloning tips or encoding storage keys.
/// Resident keys are in API form; the later molecule filter still owns the
/// public filter semantics. This must never broaden a keyed query to every
/// hash partition in a molecule.
fn resident_keys_for_filter(
    resident: &ResidentGraph,
    molecule: &str,
    filter: &HashRangeFilter,
) -> ResidentKeySetSnapshot {
    let bounds = match filter {
        HashRangeFilter::HashRangeKey { hash, range } => Some((
            ResidentMoleculeKey::new(hash, range),
            ResidentMoleculeKey::new(hash, format!("{range}\0")),
        )),
        HashRangeFilter::HashKey(hash) | HashRangeFilter::HashRangePattern { hash, .. } => Some((
            ResidentMoleculeKey::new(hash, ""),
            ResidentMoleculeKey::new(format!("{hash}\0"), ""),
        )),
        HashRangeFilter::HashRangeRange { hash, start, end } => Some((
            ResidentMoleculeKey::new(hash, start),
            ResidentMoleculeKey::new(hash, end.max(start)),
        )),
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            // Increment the final incrementable Unicode scalar. An all-max
            // prefix has no string successor, so stop at the hash boundary.
            let mut upper = prefix.clone();
            let successor = loop {
                let Some(ch) = upper.pop() else { break None };
                let next = match ch as u32 + 1 {
                    0xD800 => 0xE000,
                    n => n,
                };
                if let Some(next) = char::from_u32(next) {
                    upper.push(next);
                    break Some(upper);
                }
            };
            Some((
                ResidentMoleculeKey::new(hash, prefix),
                successor.map_or_else(
                    || ResidentMoleculeKey::new(format!("{hash}\0"), ""),
                    |end| ResidentMoleculeKey::new(hash, end),
                ),
            ))
        }
        HashRangeFilter::HashRange { start, end } => Some((
            ResidentMoleculeKey::new(start, ""),
            ResidentMoleculeKey::new(end.max(start), ""),
        )),
        HashRangeFilter::HashRangeKeys(keys) => {
            let mut result = resident.resident_key_set_range(
                molecule,
                Some(&ResidentMoleculeKey::new("", "")),
                Some(&ResidentMoleculeKey::new("", "")),
            );
            for (hash, range) in keys {
                let snapshot = resident_keys_for_filter(
                    resident,
                    molecule,
                    &HashRangeFilter::HashRangeKey {
                        hash: hash.clone(),
                        range: range.clone(),
                    },
                );
                result.keys.extend(snapshot.keys);
                result.tombstones.extend(snapshot.tombstones);
            }
            result.keys.sort_unstable();
            result.keys.dedup();
            result.tombstones.sort_unstable();
            result.tombstones.dedup();
            return result;
        }
        _ => None,
    };
    resident.resident_key_set_range(
        molecule,
        bounds.as_ref().map(|b| &b.0),
        bounds.as_ref().map(|b| &b.1),
    )
}
