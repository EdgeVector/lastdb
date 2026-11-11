//! Request parsing for the owner-socket data routes (**I4**).

use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::operations::{MutationConvergence, Operation};
use fold_db::schema::types::{Mutation, Query, QueryCursor};
use serde::Deserialize;
use serde_json::Value;

use super::query::QueryResponseFormat;
use super::reject::{Reject, WireKey};
use super::reply::content_free;

/// Decoded mutation operation before schema resolution and UUID/pubkey stamping.
#[derive(Debug)]
pub struct ParsedMutation {
    pub(in crate::exec) mutation: Mutation,
    pub(in crate::exec) convergence: MutationConvergence,
}

impl ParsedMutation {
    #[must_use]
    pub(in crate::exec) fn from_operation(
        operation: Operation,
        default_convergence: Option<MutationConvergence>,
    ) -> Self {
        let Operation::Mutation {
            schema,
            record_fields,
            key_value,
            alternates,
            mutation_type,
            source_file_name,
            metadata,
            file,
            expected,
            convergence,
            durability,
            definition_id,
        } = operation;
        let target = RecordTarget {
            schema,
            key: key_value,
            alternates,
        };
        let mut mutation = Mutation::with_uuid(
            String::new(),
            target,
            record_fields,
            String::new(),
            mutation_type,
            expected,
        );
        mutation.source_file_name = source_file_name;
        mutation.metadata = metadata;
        mutation.file = file;
        mutation.definition_id = definition_id;
        mutation.synchronous = Some(durability.unwrap_or_default().waits_for_persist());
        Self {
            mutation,
            convergence: convergence.or(default_convergence).unwrap_or_default(),
        }
    }
}

/// Decode a single mutation body before any host or schema access (**I4**).
///
/// # Errors
/// Returns the typed [`Reject`] for an invalid mutation body.
pub fn parse_mutation_body(body: &[u8]) -> Result<ParsedMutation, Reject> {
    let Ok(Value::Object(obj)) = serde_json::from_slice::<Value>(body) else {
        return Err(Reject::malformed_body());
    };
    if let Some(key) = first_missing_key(&obj, MUTATION_REQUIRED_KEYS) {
        return Err(Reject::missing(key));
    }
    let operation = serde_json::from_value::<Operation>(Value::Object(obj))
        .map_err(|error| Reject::from_serde_error(&error, None))?;
    Ok(ParsedMutation::from_operation(operation, None))
}

/// Parse the request body as a JSON object.
pub(in crate::exec) fn body_object(
    req: &UdsRequest,
) -> Result<serde_json::Map<String, Value>, UdsResponse> {
    match serde_json::from_slice::<Value>(&req.body) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) | Err(_) => Err(Reject::malformed_body().response()),
    }
}

/// A typed query and its separate page controls.
pub(in crate::exec) struct QueryRequest {
    pub(in crate::exec) query: Query,
    pub(in crate::exec) limit: Option<usize>,
    pub(in crate::exec) offset: Option<usize>,
    pub(in crate::exec) cursor: Option<QueryCursor>,
    pub(in crate::exec) response_format: QueryResponseFormat,
}

/// Decode a query body before any schema lookup, admission, or record read.
pub(in crate::exec) fn parse_query_body(
    mut obj: serde_json::Map<String, Value>,
) -> Result<QueryRequest, UdsResponse> {
    let limit = take_pagination(&mut obj, WireKey::Limit)?;
    let offset = take_pagination(&mut obj, WireKey::Offset)?;
    let cursor = take_cursor(&mut obj)?;
    let expected_total_count = take_pagination(&mut obj, WireKey::ExpectedTotalCount)?;
    let response_format = obj
        .remove("response_format")
        .map(serde_json::from_value::<QueryResponseFormat>)
        .transpose()
        .map_err(|_| Reject::invalid(WireKey::ResponseFormat).response())?
        .unwrap_or_default();
    require_keys(&obj, QUERY_REQUIRED_KEYS)?;
    let mut query: Query = serde_json::from_value(Value::Object(obj))
        .map_err(|error| Reject::from_serde_error(&error, None).response())?;
    query.expected_total_count = expected_total_count;
    Ok(QueryRequest {
        query,
        limit,
        offset,
        cursor,
        response_format,
    })
}

#[derive(Deserialize)]
struct BatchMutationsRequest {
    mutations: Vec<Operation>,
    #[serde(default)]
    convergence: Option<MutationConvergence>,
}

/// Decode either batch mutation body shape without host or task access.
pub(in crate::exec) fn parse_batch_mutation_body(
    body: &[u8],
) -> Result<Vec<ParsedMutation>, UdsResponse> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Err(content_free(400, "Bad Request"));
    };

    let (operations, default_convergence) = match value {
        Value::Array(items) => {
            let mut operations = Vec::with_capacity(items.len());
            for item in items {
                match serde_json::from_value::<Operation>(item) {
                    Ok(operation) => operations.push(operation),
                    Err(err) => return Err(Reject::from_serde_error(&err, None).response()),
                }
            }
            (operations, None)
        }
        other => match serde_json::from_value::<BatchMutationsRequest>(other) {
            Ok(request) => (request.mutations, request.convergence),
            Err(err) => return Err(Reject::from_serde_error(&err, None).response()),
        },
    };

    Ok(operations
        .into_iter()
        .map(|operation| ParsedMutation::from_operation(operation, default_convergence))
        .collect())
}

fn take_pagination(
    obj: &mut serde_json::Map<String, Value>,
    field: WireKey,
) -> Result<Option<usize>, UdsResponse> {
    let Some(raw) = obj.remove(field.as_str()) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    raw.as_u64()
        .map(|n| Some(n as usize))
        .ok_or_else(|| Reject::invalid(field).response())
}

const QUERY_REQUIRED_KEYS: &[WireKey] = &[WireKey::SchemaName];

const MUTATION_REQUIRED_KEYS: &[WireKey] = &[
    WireKey::OperationType,
    WireKey::Schema,
    WireKey::Record,
    WireKey::KeyValue,
    WireKey::MutationType,
    WireKey::Alternates,
    WireKey::Expected,
];

fn require_keys(
    obj: &serde_json::Map<String, Value>,
    required: &[WireKey],
) -> Result<(), UdsResponse> {
    first_missing_key(obj, required).map_or(Ok(()), |key| Err(Reject::missing(key).response()))
}

#[must_use]
fn first_missing_key(
    obj: &serde_json::Map<String, Value>,
    required: &[WireKey],
) -> Option<WireKey> {
    required
        .iter()
        .find(|key| !obj.contains_key(key.as_str()))
        .copied()
}

fn take_cursor(
    obj: &mut serde_json::Map<String, Value>,
) -> Result<Option<QueryCursor>, UdsResponse> {
    let Some(raw) = obj.remove(WireKey::Cursor.as_str()) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    serde_json::from_value::<QueryCursor>(raw)
        .map(Some)
        .map_err(|_| Reject::invalid(WireKey::Cursor).response())
}

#[must_use]
pub(in crate::exec) fn query_flag(target: &str, key: &str) -> bool {
    let Some((_, qs)) = target.split_once('?') else {
        return false;
    };
    qs.split('&').any(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        k == key && fold_db::env_flag::truthy(v)
    })
}

#[must_use]
pub(in crate::exec) fn query_value(target: &str, key: &str) -> Option<String> {
    let (_, qs) = target.split_once('?')?;
    qs.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == key).then(|| percent_decode(v))
    })
}

#[must_use]
pub(in crate::exec) fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[must_use]
pub(in crate::exec) fn schema_name_from_target(target: &str) -> Option<String> {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    let path = &target[..end];
    let raw = path.strip_prefix("/api/schema/")?;
    (!raw.is_empty() && !raw.contains('/')).then(|| percent_decode(raw))
}
