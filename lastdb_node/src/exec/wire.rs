//! Request parsing and typed I4-safe request-shape rejection for owner-socket data routes.

use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::operations::{MutationConvergence, Operation};
use fold_db::schema::types::{Mutation, Query, QueryCursor};
use serde::Deserialize;
use serde_json::{Map, Value};

use super::query::QueryResponseFormat;
use super::reply::content_free;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum RejectKind {
    MalformedBody,
    MissingRequiredKey,
    InvalidValue,
    UnknownKey,
}

impl RejectKind {
    pub(in crate::exec) const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedBody => "malformed_body",
            Self::MissingRequiredKey => "missing_required_key",
            Self::InvalidValue => "invalid_value",
            Self::UnknownKey => "unknown_key",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::MalformedBody => "request body must be a JSON object",
            Self::MissingRequiredKey => "a required key is missing from the request body",
            Self::InvalidValue => "a key was present with a value outside its grammar",
            Self::UnknownKey => "a key in the request body is not in this node's request grammar; if the client is newer than the node, compare the api_version it requires with GET /api/version and upgrade the node",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum WireKey {
    SchemaName,
    Limit,
    Offset,
    Cursor,
    ExpectedTotalCount,
    ResponseFormat,
    OperationType,
    Schema,
    Record,
    Alternates,
    Expected,
    KeyValue,
    MutationType,
}

const MUTATION_BODY_EXAMPLE: &str = r#"{"type":"mutation","schema":"<Schema>","record":{"<field>":"<value>"},"key_value":{"hash":"<key>","range":null},"alternates":{},"mutation_type":"create","expected":{"type":"absent"}}"#;

impl WireKey {
    pub(in crate::exec) const fn as_str(self) -> &'static str {
        match self {
            Self::SchemaName => "schema_name",
            Self::Limit => "limit",
            Self::Offset => "offset",
            Self::Cursor => "cursor",
            Self::ExpectedTotalCount => "expected_total_count",
            Self::ResponseFormat => "response_format",
            Self::OperationType => "type",
            Self::Schema => "schema",
            Self::Record => "record",
            Self::Alternates => "alternates",
            Self::Expected => "expected",
            Self::KeyValue => "key_value",
            Self::MutationType => "mutation_type",
        }
    }

    const fn remediation(self) -> &'static str {
        match self {
            Self::SchemaName => r#"{"schema_name":"<Schema>","filter":{"HashKey":"<key>"}}"#,
            Self::Limit | Self::Offset => {
                r#""limit": 100    # non-negative integer; prefer "cursor" over "offset""#
            }
            Self::Cursor => {
                r#""cursor": {"hash":"<hash>","range":"<range>"}   # echo next_cursor verbatim"#
            }
            Self::ExpectedTotalCount => {
                r#""expected_total_count": 3    # non-negative keyed COUNT assertion"#
            }
            Self::ResponseFormat => r#""response_format": "full"    # full, values, count"#,
            _ => MUTATION_BODY_EXAMPLE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reject {
    kind: RejectKind,
    key: Option<WireKey>,
}

impl Reject {
    pub(in crate::exec) const fn malformed_body() -> Self {
        Self {
            kind: RejectKind::MalformedBody,
            key: None,
        }
    }

    pub(in crate::exec) const fn missing(key: WireKey) -> Self {
        Self {
            kind: RejectKind::MissingRequiredKey,
            key: Some(key),
        }
    }

    pub(in crate::exec) const fn invalid(key: WireKey) -> Self {
        Self {
            kind: RejectKind::InvalidValue,
            key: Some(key),
        }
    }

    pub(in crate::exec) fn from_serde_error(err: &serde_json::Error, key: Option<WireKey>) -> Self {
        if err.classify() == serde_json::error::Category::Data
            && err.to_string().starts_with("unknown field")
        {
            Self {
                kind: RejectKind::UnknownKey,
                key: None,
            }
        } else {
            Self {
                kind: RejectKind::InvalidValue,
                key,
            }
        }
    }

    pub(in crate::exec) const fn kind(&self) -> RejectKind {
        self.kind
    }

    pub(in crate::exec) fn response(&self) -> UdsResponse {
        let mut map = serde_json::json!({
            "ok": false,
            "kind": self.kind.as_str(),
            "error": self.kind.message(),
        });
        if let Some(key) = self.key {
            map["key"] = Value::String(key.as_str().into());
            map["try"] = serde_json::json!([key.remediation()]);
        } else if self.kind == RejectKind::UnknownKey {
            map["try"] = serde_json::json!([
                "curl -s --unix-socket ~/.lastdb/data/folddb.sock http://localhost/api/version"
            ]);
        }
        serde_json::to_vec(&map).map_or_else(
            |_| content_free(400, "Bad Request"),
            |b| {
                UdsResponse::new(400, "Bad Request", b)
                    .with_header("Content-Type", "application/json")
            },
        )
    }
}

#[derive(Debug)]
pub struct ParsedMutation {
    pub(in crate::exec) mutation: Mutation,
    pub(in crate::exec) convergence: MutationConvergence,
}

impl ParsedMutation {
    pub(in crate::exec) fn from_operation(
        op: Operation,
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
        } = op;
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

pub fn parse_mutation_body(body: &[u8]) -> Result<ParsedMutation, Reject> {
    let Ok(Value::Object(obj)) = serde_json::from_slice::<Value>(body) else {
        return Err(Reject::malformed_body());
    };
    const REQUIRED: &[WireKey] = &[
        WireKey::OperationType,
        WireKey::Schema,
        WireKey::Record,
        WireKey::KeyValue,
        WireKey::MutationType,
        WireKey::Alternates,
        WireKey::Expected,
    ];
    if let Some(&key) = REQUIRED.iter().find(|k| !obj.contains_key(k.as_str())) {
        return Err(Reject::missing(key));
    }
    let operation = serde_json::from_value::<Operation>(Value::Object(obj))
        .map_err(|error| Reject::from_serde_error(&error, None))?;
    Ok(ParsedMutation::from_operation(operation, None))
}

pub(in crate::exec) fn body_object(req: &UdsRequest) -> Result<Map<String, Value>, UdsResponse> {
    match serde_json::from_slice::<Value>(&req.body) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(Reject::malformed_body().response()),
    }
}

pub(in crate::exec) struct QueryRequest {
    pub(in crate::exec) query: Query,
    pub(in crate::exec) limit: Option<usize>,
    pub(in crate::exec) offset: Option<usize>,
    pub(in crate::exec) cursor: Option<QueryCursor>,
    pub(in crate::exec) response_format: QueryResponseFormat,
}

pub(in crate::exec) fn parse_query_body(
    mut obj: Map<String, Value>,
) -> Result<QueryRequest, UdsResponse> {
    let take_num = |map: &mut Map<String, Value>, field: WireKey| match map.remove(field.as_str()) {
        None | Some(Value::Null) => Ok(None),
        Some(raw) => raw
            .as_u64()
            .map(|n| Some(n as usize))
            .ok_or_else(|| Reject::invalid(field).response()),
    };
    let limit = take_num(&mut obj, WireKey::Limit)?;
    let offset = take_num(&mut obj, WireKey::Offset)?;
    let cursor = match obj.remove(WireKey::Cursor.as_str()) {
        None | Some(Value::Null) => None,
        Some(raw) => serde_json::from_value::<QueryCursor>(raw)
            .map(Some)
            .map_err(|_| Reject::invalid(WireKey::Cursor).response())?,
    };
    let expected_total_count = take_num(&mut obj, WireKey::ExpectedTotalCount)?;
    let response_format = obj
        .remove("response_format")
        .map(serde_json::from_value::<QueryResponseFormat>)
        .transpose()
        .map_err(|_| Reject::invalid(WireKey::ResponseFormat).response())?
        .unwrap_or_default();
    if !obj.contains_key(WireKey::SchemaName.as_str()) {
        return Err(Reject::missing(WireKey::SchemaName).response());
    }
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

pub(in crate::exec) fn parse_batch_mutation_body(
    body: &[u8],
) -> Result<Vec<ParsedMutation>, UdsResponse> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Err(content_free(400, "Bad Request"));
    };
    let (ops, conv) = match value {
        Value::Array(items) => (
            items
                .into_iter()
                .map(|i| {
                    serde_json::from_value::<Operation>(i)
                        .map_err(|e| Reject::from_serde_error(&e, None).response())
                })
                .collect::<Result<Vec<_>, _>>()?,
            None,
        ),
        other => {
            let req = serde_json::from_value::<BatchMutationsRequest>(other)
                .map_err(|e| Reject::from_serde_error(&e, None).response())?;
            (req.mutations, req.convergence)
        }
    };
    Ok(ops
        .into_iter()
        .map(|op| ParsedMutation::from_operation(op, conv))
        .collect())
}

pub(in crate::exec) fn query_flag(target: &str, key: &str) -> bool {
    target.split_once('?').is_some_and(|(_, qs)| {
        qs.split('&').any(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            k == key && fold_db::env_flag::truthy(v)
        })
    })
}

pub(in crate::exec) fn query_value(target: &str, key: &str) -> Option<String> {
    let (_, qs) = target.split_once('?')?;
    qs.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == key).then(|| percent_decode(v))
    })
}

pub(in crate::exec) fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (
                    (bytes[i + 1] as char).to_digit(16),
                    (bytes[i + 2] as char).to_digit(16),
                ) {
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

pub(in crate::exec) fn schema_name_from_target(target: &str) -> Option<String> {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    let raw = target[..end].strip_prefix("/api/schema/")?;
    (!raw.is_empty() && !raw.contains('/')).then(|| percent_decode(raw))
}
