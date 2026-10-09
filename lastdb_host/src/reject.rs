//! The **typed rejection contract** — one machine-actionable shape for every
//! request-shape refusal the owner socket serves.
//!
//! # Why this module exists
//!
//! The client and the node disagreed in writing. `@lastdb/app-sdk` documents a
//! request-shape rejection as `400 {kind, error}` and exposes `.kind` on
//! `RequestRejectedError` — but its own doc comment concedes that "production
//! 400s are not uniform (`{kind, error}` from the dev mirror, `{error}` /
//! `{message}` / richer envelopes from production handlers)", so the SDK
//! surfaces the raw body verbatim and lets the app guess. On the owner socket
//! the shape degraded further: every parse failure in [`crate::wire`] collapsed
//! to [`content_free`](crate::envelope::content_free)`(400, "Bad Request")` — an
//! 11-byte string with no discriminator, no field name, and no remediation.
//!
//! The cost is measured, not theoretical: `POST /api/query` rejects a body with
//! no `fields` key and answers the literal bytes `Bad Request`, so the first
//! query anyone writes by hand is the one shape that fails and says nothing
//! about why. A caller cannot branch on that, so it branches on prose or on
//! nothing — and the recurring failure in this workspace is a caller reading a
//! designed refusal as an outage.
//!
//! # The contract
//!
//! A rejection body is exactly:
//!
//! ```json
//! { "ok": false, "kind": "<discriminator>", "error": "<static prose>",
//!   "key": "<wire key>", "try": ["<runnable line>", ...] }
//! ```
//!
//! `key` and `try` are present only when the rejection has them. `kind` is the
//! discriminator the SDK already declares; callers branch on it and never on
//! `error`.
//!
//! # Why this does not weaken I4
//!
//! **I4** (side-channel closure) forbids a non-owner error from echoing a
//! caller-supplied byte — a schema name, a namespace, an offending value — back
//! onto the wire. It does *not* require the node to hide *which of its own parse
//! rules* refused: that fact is a property of the request grammar, which is
//! public, and reveals nothing about what the node stores or whether a named
//! thing exists.
//!
//! The property is enforced structurally rather than by review: every field of
//! [`Reject`] is a closed enum over `&'static str`, so a rejection body is built
//! entirely from compile-time constants. There is no constructor that accepts a
//! `String`, a `Value`, or any byte derived from the request — adding one would
//! be the reviewable moment, and [`reject_bodies_are_built_only_from_static_bytes`]
//! pins the property against every caller-controlled input the routes parse.
//!
//! [`reject_bodies_are_built_only_from_static_bytes`]: tests::reject_bodies_are_built_only_from_static_bytes

use lastdb_uds::uds_http::UdsResponse;
use serde_json::{Map, Value};

/// The discriminator a caller branches on. Closed set: a new refusal reason is
/// a new variant here, which is the single place the wire contract grows.
///
/// The values match the `kind` vocabulary `@lastdb/app-sdk` already parses into
/// `RequestRejectedError.kind`, so aligning the socket to this enum removes the
/// SDK's "400s are not uniform" caveat rather than adding a third shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectKind {
    /// The request body was not a JSON object (array, scalar, malformed, empty).
    MalformedBody,
    /// A key the route requires was absent from an otherwise well-formed body.
    MissingRequiredKey,
    /// A key was present but its value did not match the grammar for that key.
    InvalidValue,
    /// The route exists but does not serve this method.
    UnsupportedMethod,
    /// A key was present that this node's request grammar does not know.
    ///
    /// The client↔node version-skew signal. Every strict request type is
    /// `#[serde(deny_unknown_fields)]`, so a client one grammar ahead of the
    /// node (brain 0.8.0 sending per-mutation `durability` to a 0.23.3-1435
    /// primary, 2026-09-03) used to get the same 11 bytes as a typo. The
    /// caller branches here, then compares the `api_version` it needs with
    /// `GET /api/version`. The unknown key itself is a caller byte and is
    /// never echoed (I4); the version handshake is what makes naming it
    /// unnecessary.
    UnknownKey,
}

impl RejectKind {
    /// The stable wire discriminator. `&'static str` by construction.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedBody => "malformed_body",
            Self::MissingRequiredKey => "missing_required_key",
            Self::InvalidValue => "invalid_value",
            Self::UnsupportedMethod => "unsupported_method",
            Self::UnknownKey => "unknown_key",
        }
    }

    /// Fixed prose for the discriminator. Never contains a caller byte, and is
    /// advisory only — a caller branches on [`as_str`](Self::as_str).
    #[must_use]
    const fn message(self) -> &'static str {
        match self {
            Self::MalformedBody => "request body must be a JSON object",
            Self::MissingRequiredKey => "a required key is missing from the request body",
            Self::InvalidValue => "a key was present with a value outside its grammar",
            Self::UnsupportedMethod => "this route does not serve that method",
            Self::UnknownKey => {
                "a key in the request body is not in this node's request grammar; \
                 if the client is newer than the node, compare the api_version it \
                 requires with GET /api/version and upgrade the node"
            }
        }
    }

    /// Kind-level remediation for a rejection that names no key.
    ///
    /// Only [`Self::UnknownKey`] has one today: the fix is to read the node's
    /// version, so the line is the read itself. The socket path is the
    /// documented default; a node on another path is the operator's own
    /// deployment and needs no caller byte to name it.
    const fn remediation(self) -> &'static [&'static str] {
        match self {
            Self::UnknownKey => {
                &["curl -s --unix-socket ~/.lastdb/data/folddb.sock http://localhost/api/version"]
            }
            Self::MalformedBody
            | Self::MissingRequiredKey
            | Self::InvalidValue
            | Self::UnsupportedMethod => &[],
        }
    }
}

/// The closed vocabulary of request keys a rejection may name.
///
/// Naming the key is what makes the refusal actionable, and it stays I4-safe
/// because the variant is chosen by the *route* from this fixed list — it is
/// never parsed out of, or derived from, the caller's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireKey {
    /// `fields` — the projection list on `POST /api/query`.
    Fields,
    /// `schema_name` — the schema a data-path request addresses.
    SchemaName,
    /// `filter` — the key filter narrowing a query to a partition.
    Filter,
    /// `limit` — page size.
    Limit,
    /// `offset` — page start (legacy; prefer `cursor`).
    Offset,
    /// `cursor` — the structured `{hash, range}` page cursor.
    Cursor,
    /// `expected_total_count` — an optional assertion for a keyed COUNT request.
    ExpectedTotalCount,
    /// `min_score` — the native-index search score floor.
    MinScore,
    /// `q` — the native-index search query string.
    Query,
    /// `type` — the operation discriminator on `POST /api/mutation`.
    ///
    /// `Operation` is `#[serde(tag = "type")]`, so an absent `type` fails
    /// before any other key is looked at.
    OperationType,
    /// `schema` — the schema a mutation addresses.
    ///
    /// Deliberately distinct from [`Self::SchemaName`]: the query route spells
    /// this key `schema_name` and the mutation route spells it `schema`. That
    /// asymmetry is itself a common first-request failure, so the two keys stay
    /// separate variants and each rejection names the spelling its own route
    /// wants.
    Schema,
    /// `fields_and_values` — the field map a mutation writes.
    FieldsAndValues,
    /// `key_value` — the `{hash, range}` selector a mutation addresses.
    KeyValue,
    /// `mutation_type` — `create` / `update` / `delete` (`purge` = legacy must-exist alias).
    MutationType,
}

/// The canonical minimal `POST /api/mutation` body, handed back verbatim as
/// remediation for every structural mutation key.
///
/// `Operation` is internally tagged (`#[serde(tag = "type")]`), so the keys sit
/// flat alongside `type` rather than nested under it — the shape a caller is
/// most likely to get wrong by analogy with an externally-tagged enum.
///
/// Executed, not merely displayed: [`tests::the_mutation_example_body_is_a_real_operation`]
/// strips the comment and deserializes the remainder into the real `Operation`
/// type, so this line cannot drift from the grammar it advertises.
const MUTATION_BODY_EXAMPLE: &str = concat!(
    r#"{"type":"mutation","schema":"<Schema>","fields_and_values":{"<field>":"<value>"},"#,
    r#""key_value":{"hash":"<key>","range":null},"mutation_type":"create"}"#,
    "   # mutation_type: create, update, delete  (purge = legacy must-exist alias)",
);

impl WireKey {
    /// The key's name exactly as it appears on the wire.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fields => "fields",
            Self::SchemaName => "schema_name",
            Self::Filter => "filter",
            Self::Limit => "limit",
            Self::Offset => "offset",
            Self::Cursor => "cursor",
            Self::ExpectedTotalCount => "expected_total_count",
            Self::MinScore => "min_score",
            Self::Query => "q",
            Self::OperationType => "type",
            Self::Schema => "schema",
            Self::FieldsAndValues => "fields_and_values",
            Self::KeyValue => "key_value",
            Self::MutationType => "mutation_type",
        }
    }

    /// Runnable remediation for this key, in the caller's own vocabulary.
    ///
    /// Per the standing rule that a rejection's remediation is executable advice
    /// and must be correct as executed: these lines are copy-pasteable request
    /// bodies, not prose about them. An empty slice means no remediation is
    /// better than a wrong one.
    #[must_use]
    const fn remediation(self) -> &'static [&'static str] {
        match self {
            Self::Fields => &[
                r#"{"schema_name":"<Schema>","fields":[],"filter":{"HashKey":"<key>"}}   # [] = all fields"#,
            ],
            Self::SchemaName => {
                &[r#"{"schema_name":"<Schema>","fields":[],"filter":{"HashKey":"<key>"}}"#]
            }
            Self::Filter => &[
                r#"{"schema_name":"<Schema>","fields":[],"filter":{"HashKey":"<key>"}}          # partition"#,
                r#"{"schema_name":"<Schema>","fields":[],"filter":{"HashRange":{"hash":"<k>"}}} # range"#,
            ],
            Self::Limit | Self::Offset => {
                &[r#""limit": 100    # non-negative integer; prefer "cursor" over "offset""#]
            }
            Self::Cursor => {
                &[r#""cursor": {"hash":"<hash>","range":"<range>"}   # echo next_cursor verbatim"#]
            }
            Self::ExpectedTotalCount => {
                &[r#""expected_total_count": 3    # non-negative keyed COUNT assertion"#]
            }
            Self::MinScore => &[r#""min_score": 0.5    # finite, 0.0 ..= 1.0"#],
            Self::Query => &[r#"?q=<text>&k=10"#],
            // Every structural mutation key hands back the whole canonical
            // body rather than a fragment. A caller who got one of these
            // wrong has, in practice, hand-written the request — the useful
            // reply is the shape that works, not the name of the one field.
            Self::OperationType
            | Self::Schema
            | Self::FieldsAndValues
            | Self::KeyValue
            | Self::MutationType => &[MUTATION_BODY_EXAMPLE],
        }
    }
}

/// A typed, I4-safe request-shape rejection.
///
/// Constructed only from the closed enums above, so no caller-supplied byte can
/// reach the wire through this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reject {
    kind: RejectKind,
    key: Option<WireKey>,
}

impl Reject {
    /// A rejection naming only its discriminator.
    #[must_use]
    pub const fn new(kind: RejectKind) -> Self {
        Self { kind, key: None }
    }

    /// The body was not a JSON object.
    #[must_use]
    pub const fn malformed_body() -> Self {
        Self::new(RejectKind::MalformedBody)
    }

    /// A key the node's grammar does not know was present.
    #[must_use]
    pub const fn unknown_key() -> Self {
        Self::new(RejectKind::UnknownKey)
    }

    /// Classify a `serde` decode failure after every required key was seen
    /// present: [`Self::unknown_key`] when the strict type refused a key it
    /// does not declare, otherwise `invalid` / [`RejectKind::InvalidValue`]
    /// with the given key (or none).
    ///
    /// The decision reads `serde`'s own message prefix (`unknown field`), a
    /// property of the deserializer and not of the request; nothing from the
    /// message reaches the wire. The unknown key's name is inside that message
    /// and stays there (I4).
    #[must_use]
    pub fn from_serde_error(err: &serde_json::Error, key: Option<WireKey>) -> Self {
        if err.classify() == serde_json::error::Category::Data
            && err.to_string().starts_with("unknown field")
        {
            return Self::unknown_key();
        }
        match key {
            Some(key) => Self::invalid(key),
            None => Self::new(RejectKind::InvalidValue),
        }
    }

    /// A required key was absent.
    #[must_use]
    pub const fn missing(key: WireKey) -> Self {
        Self {
            kind: RejectKind::MissingRequiredKey,
            key: Some(key),
        }
    }

    /// A key was present with a value outside its grammar.
    #[must_use]
    pub const fn invalid(key: WireKey) -> Self {
        Self {
            kind: RejectKind::InvalidValue,
            key: Some(key),
        }
    }

    /// The discriminator a caller branches on.
    #[must_use]
    pub const fn kind(&self) -> RejectKind {
        self.kind
    }

    /// The JSON body served for this rejection. Every byte originates in a
    /// `&'static str` on [`RejectKind`] or [`WireKey`].
    #[must_use]
    pub fn body(&self) -> Value {
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
        let lines = match self.key {
            Some(key) => {
                map.insert("key".to_string(), Value::String(key.as_str().to_string()));
                key.remediation()
            }
            None => self.kind.remediation(),
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

    /// Render as the `400 Bad Request` socket response.
    ///
    /// The HTTP reason phrase stays the fixed `Bad Request` so nothing keying on
    /// the status line moves; the discriminator lives in the JSON body. A
    /// serialization failure collapses to the content-free form rather than
    /// serving a partial body.
    #[must_use]
    pub fn response(&self) -> UdsResponse {
        match serde_json::to_vec(&self.body()) {
            Ok(bytes) => UdsResponse::new(400, "Bad Request", bytes)
                .with_header("Content-Type", "application/json"),
            Err(_) => crate::envelope::content_free(400, "Bad Request"),
        }
    }
}

impl From<Reject> for UdsResponse {
    fn from(reject: Reject) -> Self {
        reject.response()
    }
}
