//! Feature-flagged node-key + proof-of-work gate for unowned shared schema
//! mutations.
//!
//! Challenge verification is stateless: issued challenges carry an HMAC over
//! the nonce, caller identity, schema hash, difficulty, and expiry. The only
//! mutable store is quota accounting, which the in-memory implementation keeps
//! under the same bucket keys a Lambda/DynamoDB adapter uses.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use app_identity_crypto::{
    canonicalize, compute_payload_hash, key_id, verify_envelope, verifying_key_from_base64,
    Purpose, SignatureEnvelope,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use hmac::{Hmac, Mac};
use schema_types::{Schema, SchemaSource};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::lock_helpers::{read_lock, write_lock};
use crate::state::SchemaServiceState;
use crate::types::{SchemaMutationGateObservation, SchemaMutationGateRequirement};

/// Canonical Title-Case HTTP header names for the schema-mutation PoW gate.
///
/// HTTP header compare is case-insensitive. Client, actix, and Lambda adapters
/// must use these names rather than a second copied list. Lambda historically
/// looked up ASCII-lowercase forms; `header_name_ascii_lowercase` derives that
/// form from the canonical name so the wire names cannot drift.
pub const HEADER_NODE_PUBLIC_KEY: &str = "X-Node-Public-Key";
pub const HEADER_NODE_SIGNATURE: &str = "X-Node-Signature";
pub const HEADER_POW_CHALLENGE: &str = "X-Pow-Challenge";
pub const HEADER_POW_NONCE: &str = "X-Pow-Nonce";
pub const HEADER_POW_CHALLENGE_MAC: &str = "X-Pow-Challenge-Mac";
pub const HEADER_POW_DIFFICULTY_BITS: &str = "X-Pow-Difficulty-Bits";
pub const HEADER_POW_EXPIRES_AT: &str = "X-Pow-Expires-At";
pub const HEADER_POW_COUNTER: &str = "X-Pow-Counter";
pub const HEADER_DEV_PUBKEY: &str = "X-Dev-Pubkey";

/// ASCII-lowercase form of a canonical Title-Case header name.
pub fn header_name_ascii_lowercase(canonical: &str) -> String {
    canonical.to_ascii_lowercase()
}

type HmacSha256 = Hmac<Sha256>;

mod config;
pub use config::*;
mod types;
pub use types::*;
mod error;
pub use error::*;
mod pow;
mod state_enforce;
mod state_issue;
mod state_quota;
pub use pow::*;
mod quota_store;
use quota_store::*;
mod challenge_mac;
use challenge_mac::*;
