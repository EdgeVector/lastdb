//! `lastdb app …` — developer-facing app registration + publish flow.
//!
//! Load app manifests, manage developer keys, and check schema coverage on
//! the local Mini daemon. Signed registry publication uses these helpers.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use app_identity_crypto::SigningKey;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use schema_service_core::app_identity::validate_app_version;
use schema_service_core::snapshot::{AppArtifact, AppMetadata};
use serde::Deserialize;
use serde_json::{json, Value};

mod devkey;
mod http;
mod manifest;
mod schema_check;

pub use devkey::*;
use http::*;
pub use manifest::*;
pub use schema_check::*;
