//! Resolve stored or stand-in alternate key addresses for a `/v1/write` delete.

use fold_db::schema::types::{KeyValue, Query};
use schema_types::record_keys::{RecordKeyDefinition, RecordKeyKind};
use schema_types::FieldValueType;

use super::super::refusal::ABSENT;
use super::super::{bad, key_filter};
use super::{Alternates, WriteItem};
use crate::exec::query::{execute_query, QueryResponseFormat};
use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::exec::wire::QueryRequest;
use crate::host::Host;

pub(super) async fn stored_alternates(
    host: &Host,
    item: &WriteItem,
) -> Result<Alternates, HostError> {
    let filter = key_filter(&item.key)?;
    let request = QueryRequest {
        query: Query::new_with_filter(item.schema.clone(), Some(filter)),
        limit: Some(2),
        offset: None,
        cursor: None,
        response_format: QueryResponseFormat::Values,
    };
    let page = execute_query(host, request).await?;
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::internal("write read is incomplete"))?;
    match rows.as_slice() {
        [] => absent_addresses(host, item),
        [row] => serde_json::from_value(row["alternates"].clone())
            .map_err(|_| HostError::internal("stored alternates are invalid")),
        _ => Err(bad(
            "the key names more than one record; a hash and range schema needs key.range",
        )),
    }
}

/// No live record at this key. A shape-valid stand-in lets the commit report
/// `absent`. When none fits, this keyed read has already shown that absence.
fn absent_addresses(host: &Host, item: &WriteItem) -> Result<Alternates, HostError> {
    let Some(name) = resolve_schema_name(host, &item.schema)? else {
        return Ok(Alternates::new());
    };
    let Some(schema) = host.records().schema_manager().get_schema_metadata(&name)? else {
        return Ok(Alternates::new());
    };
    let Some(keys) = schema.record_keys.as_ref() else {
        return Ok(Alternates::new());
    };
    let mut alternates = Alternates::new();
    for (alternate, definition) in &keys.alternates {
        let Some(address) = stand_in_address(definition, &item.key) else {
            return Err(HostError::new(409, "no record holds the key").with_reason(ABSENT));
        };
        alternates.insert(alternate.clone(), address);
    }
    Ok(alternates)
}

/// `Some(None)` is an alternate the schema allows to be missing.
/// `None` means no address fits the declared shape.
fn stand_in_address(
    definition: &RecordKeyDefinition,
    primary: &KeyValue,
) -> Option<Option<KeyValue>> {
    if definition.allow_absent {
        return Some(None);
    }
    let hash = primary.hash.clone().filter(|hash| !hash.is_empty())?;
    stand_in_ranges(definition, primary)
        .into_iter()
        .find(|range| {
            definition
                .validate_coordinates(Some(hash.as_str()), range.as_deref())
                .is_ok()
        })
        .map(|range| {
            Some(KeyValue {
                hash: Some(hash),
                range,
            })
        })
}

fn stand_in_ranges(definition: &RecordKeyDefinition, primary: &KeyValue) -> Vec<Option<String>> {
    if definition.kind == RecordKeyKind::Hash {
        return vec![None];
    }
    let mut ranges = Vec::new();
    if let Some(sample) = sample_range(definition.range_type.as_ref()) {
        ranges.push(Some(sample));
    }
    if let Some(range) = primary.range.clone().filter(|range| !range.is_empty()) {
        ranges.push(Some(range));
    }
    if let Some(hash) = primary.hash.clone().filter(|hash| !hash.is_empty()) {
        ranges.push(Some(hash));
    }
    ranges
}

fn sample_range(range_type: Option<&FieldValueType>) -> Option<String> {
    Some(
        match range_type? {
            FieldValueType::String => "-",
            FieldValueType::Integer => "0",
            FieldValueType::Boolean => "false",
            FieldValueType::Date => "1970-01-01",
            FieldValueType::DateTime => "1970-01-01T00:00:00Z",
            _ => return None,
        }
        .to_owned(),
    )
}
