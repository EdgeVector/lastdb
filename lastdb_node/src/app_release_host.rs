//! Host side of the Exemem app registry: two execution identities, one host.
//!
//! A developer session and a published release live on the same machine and
//! never touch. They are separated at four levels, and no function here
//! crosses the line:
//!
//! | | Development | Published release |
//! |---|---|---|
//! | identity | `dev:<app_id>:<workspace_id>:<dev_session_id>` | `release:<app_uuid>:<release_id>:<activation_epoch>` |
//! | place | a mutable workspace | `~/.host-track/apps/<app>/versions/<release-id>/` |
//! | grants | workspace scope only | release scope only |
//! | status | `DEV` or `UNMANAGED` | `CURRENT` on a four-way match + a green probe |
//! | writes | schemas, during development | no schema write, ever |
//!
//! ## The two status rules
//!
//! 1. Development reports [`AppStatus::Dev`] or [`AppStatus::Unmanaged`]. It
//!    can never report [`AppStatus::Current`] — [`dev_status`] is the only
//!    function that produces a development status, and it cannot return
//!    `Current`.
//! 2. A published app reports [`AppStatus::Current`] only when the desired,
//!    installed, active, and observed release ids all match and its probe is
//!    green. Any inequality removes `Current` ([`FourWayProof::evaluate`]).
//!
//! ## Activation order
//!
//! Read the channel and keep its generation → read the release manifest by
//! release id → download the artifact, compare the byte digest, verify the
//! signature → unpack into the version directory → move the `current`
//! pointer → observe the live process and prove the four-way match.
//!
//! A digest fault or a signature fault stops at step 3. The `current`
//! pointer never moves for a faulty artifact.
//!
//! ## Nothing here writes a schema
//!
//! Install reads a channel, reads a release, and fetches bytes. There is no
//! schema-service write path in this module — the schema identities were
//! locked during development and the release only carries them.

use fold_db::clock::unix_secs;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use app_identity_crypto::{verify, verifying_key_from_base64};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use schema_service_core::app_release::{is_sha256_hex, sha256_hex, ReleaseManifest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── Operator-settled constants ───────────────────────────────────────────

/// How many release directories to keep under `versions/`.
///
/// Rollback needs at least the prior verified release on disk. Three
/// directories cover one rollback and one repeat failure; older directories
/// are deleted after a green probe on the active release.
pub const RELEASE_RETENTION: usize = 3;

/// How often the host re-checks the channel, the revocations, and the
/// four-way match after activation.
///
/// One cycle reads the channel, reads the active release's revocation, runs
/// the four-way check, and restores the prior verified release on drift.
/// `lastdb app release-check --watch` runs that cycle on this interval;
/// [`check_interval`] is what resolves it.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// The delay between two recurring check cycles.
///
/// `None` is the operator setting, [`PROBE_INTERVAL`]. An override is for a
/// test or a proof that cannot wait a minute. An override of zero is raised
/// to one second, so a `--watch` run can never become a busy loop.
#[must_use]
pub fn check_interval(override_secs: Option<u64>) -> Duration {
    match override_secs {
        None => PROBE_INTERVAL,
        Some(secs) => Duration::from_secs(secs.max(1)),
    }
}

/// How old an observation may be before the host stops trusting it.
///
/// `CURRENT` is a live claim, not a cached one: past this window the
/// observed release id reads as unknown and the status drops out of
/// `CURRENT` until a fresh observation arrives.
pub const OBSERVATION_STALE_AFTER: Duration = Duration::from_secs(180);

/// Timeout for one registry or artifact HTTP call.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

mod activation;
mod artifact;
mod host_track;
mod identity;
mod proof;
mod registry;

pub use activation::*;
pub use artifact::*;
pub use host_track::*;
pub use identity::*;
pub use proof::*;
pub use registry::*;
