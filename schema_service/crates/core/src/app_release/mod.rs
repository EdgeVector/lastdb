//! Exemem app registry — release, channel, and revocation records (`/v2`).
//!
//! The `/v1` app surface answers "who owns this namespace". This module
//! answers "which exact bytes is this app, right now" — the four-way bind a
//! release carries:
//!
//! 1. the **locked schema identities** the Schema Service already returned,
//! 2. the **source commit** that produced the artifact,
//! 3. the **artifact digest**, `SHA-256` over the artifact bytes in R2, and
//! 4. the **release id**, `SHA-256` over the canonical release manifest.
//!
//! ## No schema write after the lock
//!
//! Publishing a release is a *read* of the catalog, never a write. Every
//! identity in `manifest.schemas` must already resolve; an unresolved one
//! fails the publish with [`ReleaseError::UnresolvedSchema`] instead of
//! quietly registering a schema at release time. Install is the same: it
//! reads a channel, reads a release, and fetches bytes. Nothing on the
//! release or install path touches the schema catalog.
//!
//! ## Exact-key access only
//!
//! LastDB is Dynamo-style: there is no scan and no field filter. Every
//! record here has one deterministic key and every read is a point get.
//!
//! | Record | Key |
//! |---|---|
//! | Release | `release_id` |
//! | Channel | [`channel_key`]`(app_id, channel)` |
//! | App | `app_id` (owned by [`crate::app_identity`]) |
//!
//! ## Writes need a DevCert; reads need nothing
//!
//! `POST /v2/apps/{app_id}/releases`, `PUT …/channels/{channel}`, and
//! `POST …/revocations` each carry one `X-Exemem-Dev-Cert` plus one
//! `X-Signature` envelope whose purpose is pinned per route
//! ([`Purpose::AppReleasePublish`], [`Purpose::AppChannelSet`],
//! [`Purpose::AppReleaseRevoke`]). The three `GET` routes are anonymous.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use app_identity_crypto::{canonicalize, Purpose};

use crate::app_identity::{
    env_label, is_valid_app_id, verify_cert_and_signature, AppIdentityConfig, AuthFailure,
};
use crate::lock_helpers::{read_lock, write_lock};
use crate::state::SchemaServiceState;

mod compat_write;
mod errors;
mod grammar;
mod persistence;
mod read_v2;
mod records;
mod state;
mod validate;

pub use errors::*;
pub use grammar::*;
pub use records::*;
use validate::*;
