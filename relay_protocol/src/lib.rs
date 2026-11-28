//! The LastDB relay protocol: the one definition of every relay route, request header, request
//! body, answer body and error. No I/O.
//!
//! The node's relay client, the reference relay and the Exemem relay (through its `fold/`
//! submodule) all send and parse only these types, so a route or field that is not here cannot be
//! sent or answered. `docs/designs/relay-protocol.openapi.json` is generated from them
//! (`cargo run -p relay_protocol --features schema --bin relay-protocol-openapi`), and CI refuses a
//! committed file that differs and any breaking change under `/relay/v1/`. The behavior behind the
//! shapes (what a relay stores, trims, drops and charges) is in `docs/designs/relay-protocol.md`.
//!
//! # The rule
//!
//! Within protocol 1 a change may only add: a route, an optional request field, or an answer field.
//! It may not remove or rename a route or field, change a type, make an optional request field
//! required, narrow the values a field accepts, or change what an existing field means. Answer
//! types accept fields they do not know; request types refuse them, as a relay does. Each change
//! raises [`REVISION`], which a relay announces on every answer; a node uses an addition that not
//! every relay serves only where [`supports`] says the relay does. A breaking change is protocol 2,
//! under `/relay/v2/`.
//!
//! Signed log entries are not defined here: an entry is the JSON of `mutation_log::SignedEntry`,
//! carried as a JSON value, and `mutation_log` is its one definition.

mod blob;
mod error;
mod log;
#[cfg(feature = "schema")]
pub mod openapi;

pub use blob::*;
pub use error::*;
pub use log::*;

/// The protocol version every request names in [`headers::PROTOCOL`].
pub const PROTOCOL: &str = "1";

/// The contract revision within [`PROTOCOL`]. It rises by one with each change to the contract
/// (each is an addition: a route, an optional request field, an answer field, an error status),
/// and CI refuses a change to the generated contract that does not raise it. A relay names the
/// revision it serves on every answer in [`answer_headers::REVISION`] as [`revision`]; a node
/// uses an addition only where [`supports`] says the relay serves it.
///
/// Revision 3 adds publisher-wide stream listings and writer-qualified snapshot positions.
pub const REVISION: u32 = 3;

/// The revision as a relay announces it and the OpenAPI document names it:
/// `<PROTOCOL>.<REVISION>`, for example `1.1`.
#[must_use]
pub fn revision() -> String {
    format!("{PROTOCOL}.{REVISION}")
}

/// The revision a relay announced, from the value of [`answer_headers::REVISION`]. `None` when the
/// value is not `<PROTOCOL>.<n>`: another protocol, or not a revision at all.
#[must_use]
pub fn parse_revision(value: &str) -> Option<u32> {
    let (protocol, revision) = value.trim().split_once('.')?;
    if protocol != PROTOCOL || revision.is_empty() || !revision.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    revision.parse().ok()
}

/// The answer headers. A relay sends them on every answer of a route, a refusal included.
pub mod answer_headers {
    /// The contract revision the relay serves, as [`crate::revision`] writes it. A relay from
    /// before revision 1 does not send it; a node reads its absence as revision 0.
    pub const REVISION: &str = "X-LastDB-Relay-Revision";
}

/// An addition to the contract that a node uses only on a relay that serves it, named once here
/// with the revision it arrived in. A node never chooses on the relay's name or `User-Agent`;
/// it asks [`supports`]. When the relay is too old, the node leaves out an optional field or holds
/// that one operation with a status that says the relay must be upgraded, and keeps no older
/// fallback path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Feature {
    /// The publisher can list and fence stream positions by writer and device.
    WriterQualifiedStreams,
}

impl Feature {
    /// Every feature, oldest first. A node reports each one for its relay.
    pub const ALL: [Self; 1] = [Self::WriterQualifiedStreams];

    /// The revision that first serves the feature.
    #[must_use]
    pub const fn since(self) -> u32 {
        match self {
            Self::WriterQualifiedStreams => 3,
        }
    }

    /// The name a status message uses for the feature.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::WriterQualifiedStreams => "writer_qualified_streams",
        }
    }
}

/// Whether a relay that announced `revision` (`None`: it announced none, revision 0) serves
/// `feature`.
#[must_use]
pub fn supports(revision: Option<u32>, feature: Feature) -> bool {
    revision.unwrap_or(0) >= feature.since()
}

/// The request headers. Every request carries all of them: the relay checks the signature over
/// [`signed_message`] before it reads the body.
pub mod headers {
    /// The protocol version, [`crate::PROTOCOL`]. Another value is answered 426.
    pub const PROTOCOL: &str = "X-LastDB-Relay-Protocol";
    /// The caller's Ed25519 public key, 64 lowercase hex.
    pub const NODE_KEY: &str = "X-LastDB-Node-Key";
    /// The database the request is about, 64 lowercase hex. It must equal the body's
    /// `database_id`.
    pub const DATABASE_ID: &str = "X-LastDB-Database-Id";
    /// The caller's device stream, 64 lowercase hex. Not part of the signature or the grant gate.
    pub const DEVICE_ID: &str = "X-LastDB-Device-Id";
    /// Unix seconds when the request was signed (the relay allows 60 seconds either way).
    pub const ISSUED_AT: &str = "X-LastDB-Issued-At";
    /// 32 random bytes, 64 lowercase hex. The relay refuses a nonce it has seen, and consumes the
    /// nonce of a request it refuses, so a retry is signed again.
    pub const NONCE: &str = "X-LastDB-Nonce";
    /// Ed25519 signature of [`crate::signed_message`] by the node key, base64url without padding.
    pub const SIGNATURE: &str = "X-LastDB-Signature";
}

/// The `User-Agent` a LastDB node sends on every relay request: `lastdb/<build version>`. It is
/// not signed and grants nothing; a relay logs it with each refusal and failure, so a failure in
/// production names the version that sent it. A relay reduces it before logging (at most 80
/// characters of ASCII letters, digits, `/ . - + _ ( ) ;` and spaces).
#[must_use]
pub fn user_agent(build_version: &str) -> String {
    format!("lastdb/{build_version}")
}

/// The bytes a request signature covers:
/// `POST\n<path>\n<protocol>\n<node_key>\n<database_id>\n<body_sha256 hex>\n<issued_at>\n<nonce>`.
#[must_use]
pub fn signed_message(
    path: &str,
    node_key: &str,
    database_id: &str,
    body_sha256_hex: &str,
    issued_at: &str,
    nonce: &str,
) -> String {
    format!("POST\n{path}\n{PROTOCOL}\n{node_key}\n{database_id}\n{body_sha256_hex}\n{issued_at}\n{nonce}")
}

/// Every relay route. Each is `POST` with a JSON body and a JSON answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Route {
    /// Store signed entries, or one log segment, on the caller's stream.
    LogAppend,
    /// Read one writer's entries.
    LogRead,
    /// List one writer's device streams and how far each reaches.
    LogStreams,
    /// Drop what a snapshot entry covers (personal database, publisher only).
    LogTrim,
    /// Begin or commit the upload of one blob.
    BlobPut,
    /// Get a presigned download of one blob.
    BlobGet,
    /// List the committed blobs of a database.
    BlobList,
    /// Drop named blobs (personal database, publisher only).
    BlobDrop,
}

impl Route {
    /// Every route, in the order the OpenAPI document lists them.
    pub const ALL: [Self; 8] = [
        Self::LogAppend,
        Self::LogRead,
        Self::LogStreams,
        Self::LogTrim,
        Self::BlobPut,
        Self::BlobGet,
        Self::BlobList,
        Self::BlobDrop,
    ];

    /// The path, which is also the path the signature covers.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::LogAppend => "/relay/v1/log/append",
            Self::LogRead => "/relay/v1/log/read",
            Self::LogStreams => "/relay/v1/log/streams",
            Self::LogTrim => "/relay/v1/log/trim",
            Self::BlobPut => "/relay/v1/blob/put",
            Self::BlobGet => "/relay/v1/blob/get",
            Self::BlobList => "/relay/v1/blob/list",
            Self::BlobDrop => "/relay/v1/blob/drop",
        }
    }

    /// The route for a path, if it is one.
    #[must_use]
    pub fn from_path(path: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|route| route.path() == path)
    }

    /// The error statuses this route answers, beside 200. Each carries an [`ErrorAnswer`].
    #[must_use]
    pub const fn error_statuses(self) -> &'static [u16] {
        match self {
            Self::LogAppend | Self::BlobPut | Self::BlobDrop => {
                &[400, 401, 403, 404, 409, 426, 429, 500]
            }
            Self::LogRead => &[400, 401, 403, 404, 410, 426, 429, 500],
            Self::LogStreams | Self::BlobList => &[400, 401, 403, 426, 429, 500],
            Self::LogTrim | Self::BlobGet => &[400, 401, 403, 404, 426, 429, 500],
        }
    }
}

/// A route with its request and answer types. A client sends `Request` to `ROUTE` and parses the
/// 200 answer as `Answer`; a relay parses `Request` and answers `Answer`.
pub trait Endpoint {
    /// The route.
    const ROUTE: Route;
    /// The request body.
    type Request: serde::Serialize + serde::de::DeserializeOwned;
    /// The 200 answer body.
    type Answer: serde::Serialize + serde::de::DeserializeOwned;
    /// The database the request names, which the [`headers::DATABASE_ID`] header must repeat.
    fn database_id(request: &Self::Request) -> &str;
}

macro_rules! endpoint {
    ($(#[$doc:meta])* $name:ident, $route:ident, $request:ty, $answer:ty) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug)]
        pub struct $name;

        impl Endpoint for $name {
            const ROUTE: Route = Route::$route;
            type Request = $request;
            type Answer = $answer;
            fn database_id(request: &Self::Request) -> &str {
                &request.database_id
            }
        }
    };
}

endpoint!(
    /// `log.append`.
    LogAppend, LogAppend, LogAppendRequest, LogAppendAnswer
);
endpoint!(
    /// `log.read`.
    LogRead, LogRead, LogReadRequest, LogReadAnswer
);
endpoint!(
    /// `log.streams`.
    LogStreams, LogStreams, LogStreamsRequest, LogStreamsAnswer
);
endpoint!(
    /// `log.trim`.
    LogTrim, LogTrim, LogTrimRequest, LogTrimAnswer
);
endpoint!(
    /// `blob.put`.
    BlobPut, BlobPut, BlobPutRequest, BlobPutAnswer
);
endpoint!(
    /// `blob.get`.
    BlobGet, BlobGet, BlobGetRequest, BlobGetAnswer
);
endpoint!(
    /// `blob.list`.
    BlobList, BlobList, BlobListRequest, BlobListAnswer
);
endpoint!(
    /// `blob.drop`.
    BlobDrop, BlobDrop, BlobDropRequest, BlobDropAnswer
);

/// `skip_serializing_if` helper for answer flags that are only written when set.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}
