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
//! types accept fields they do not know; request types refuse them, as a relay does. A relay serves
//! an addition before any client sends it. A breaking change is protocol 2, under `/relay/v2/`.
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
            Self::LogAppend | Self::BlobDrop => &[400, 401, 403, 404, 409, 426, 429, 500],
            Self::LogRead => &[400, 401, 403, 410, 426, 429, 500],
            Self::LogStreams | Self::BlobPut | Self::BlobList => &[400, 401, 403, 426, 429, 500],
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
