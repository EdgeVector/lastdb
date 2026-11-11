//! Typed I4-safe request-shape rejection contract for the owner socket.

use crate::lastdb_uds::uds_http::UdsResponse;
use serde_json::{Map, Value};

/// The discriminator a caller branches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum RejectKind {
    MalformedBody,
    MissingRequiredKey,
    InvalidValue,
    UnknownKey,
}

impl RejectKind {
    #[must_use]
    pub(in crate::exec) const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedBody => "malformed_body",
            Self::MissingRequiredKey => "missing_required_key",
            Self::InvalidValue => "invalid_value",
            Self::UnknownKey => "unknown_key",
        }
    }

    #[must_use]
    const fn message(self) -> &'static str {
        match self {
            Self::MalformedBody => "request body must be a JSON object",
            Self::MissingRequiredKey => "a required key is missing from the request body",
            Self::InvalidValue => "a key was present with a value outside its grammar",
            Self::UnknownKey => {
                "a key in the request body is not in this node's request grammar; \
                 if the client is newer than the node, compare the api_version it \
                 requires with GET /api/version and upgrade the node"
            }
        }
    }

    const fn remediation(self) -> &'static [&'static str] {
        match self {
            Self::UnknownKey => {
                &["curl -s --unix-socket ~/.lastdb/data/folddb.sock http://localhost/api/version"]
            }
            Self::MalformedBody | Self::MissingRequiredKey | Self::InvalidValue => &[],
        }
    }
}

/// Closed vocabulary of request keys a rejection may name.
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

const MUTATION_BODY_EXAMPLE: &str = concat!(
    r#"{"type":"mutation","schema":"<Schema>","record":{"<field>":"<value>"},"#,
    r#""key_value":{"hash":"<key>","range":null},"alternates":{},"mutation_type":"create","expected":{"type":"absent"}}"#,
);

impl WireKey {
    #[must_use]
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

    #[must_use]
    const fn remediation(self) -> &'static [&'static str] {
        match self {
            Self::SchemaName => &[r#"{"schema_name":"<Schema>","filter":{"HashKey":"<key>"}}"#],
            Self::Limit | Self::Offset => {
                &[r#""limit": 100    # non-negative integer; prefer "cursor" over "offset""#]
            }
            Self::Cursor => {
                &[r#""cursor": {"hash":"<hash>","range":"<range>"}   # echo next_cursor verbatim"#]
            }
            Self::ExpectedTotalCount => {
                &[r#""expected_total_count": 3    # non-negative keyed COUNT assertion"#]
            }
            Self::ResponseFormat => &[r#""response_format": "full"    # full, values, count"#],
            Self::OperationType
            | Self::Schema
            | Self::Record
            | Self::Alternates
            | Self::Expected
            | Self::KeyValue
            | Self::MutationType => &[MUTATION_BODY_EXAMPLE],
        }
    }
}

/// A typed, I4-safe request-shape rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reject {
    kind: RejectKind,
    key: Option<WireKey>,
}

impl Reject {
    #[must_use]
    const fn new(kind: RejectKind) -> Self {
        Self { kind, key: None }
    }

    #[must_use]
    pub(in crate::exec) const fn malformed_body() -> Self {
        Self::new(RejectKind::MalformedBody)
    }

    #[must_use]
    const fn unknown_key() -> Self {
        Self::new(RejectKind::UnknownKey)
    }

    #[must_use]
    pub(in crate::exec) fn from_serde_error(err: &serde_json::Error, key: Option<WireKey>) -> Self {
        if err.classify() == serde_json::error::Category::Data
            && err.to_string().starts_with("unknown field")
        {
            return Self::unknown_key();
        }
        key.map_or_else(|| Self::new(RejectKind::InvalidValue), Self::invalid)
    }

    #[must_use]
    pub(in crate::exec) const fn missing(key: WireKey) -> Self {
        Self {
            kind: RejectKind::MissingRequiredKey,
            key: Some(key),
        }
    }

    #[must_use]
    pub(in crate::exec) const fn invalid(key: WireKey) -> Self {
        Self {
            kind: RejectKind::InvalidValue,
            key: Some(key),
        }
    }

    #[must_use]
    pub(in crate::exec) const fn kind(&self) -> RejectKind {
        self.kind
    }

    #[must_use]
    fn body(&self) -> Value {
        let mut map = Map::new();
        map.insert("ok".to_string(), Value::Bool(false));
        map.insert(
            "kind".to_string(),
            Value::String(self.kind.as_str().to_string()),
        );
        map.insert(
            "error".to_string(),
            Value::String(self.kind.message().to_string()),
        );
        let lines = if let Some(key) = self.key {
            map.insert("key".to_string(), Value::String(key.as_str().to_string()));
            key.remediation()
        } else {
            self.kind.remediation()
        };
        if !lines.is_empty() {
            map.insert(
                "try".to_string(),
                Value::Array(
                    lines
                        .iter()
                        .map(|l| Value::String((*l).to_string()))
                        .collect(),
                ),
            );
        }
        Value::Object(map)
    }

    #[must_use]
    pub(in crate::exec) fn response(&self) -> UdsResponse {
        match serde_json::to_vec(&self.body()) {
            Ok(bytes) => UdsResponse::new(400, "Bad Request", bytes)
                .with_header("Content-Type", "application/json"),
            Err(_) => super::reply::content_free(400, "Bad Request"),
        }
    }
}
