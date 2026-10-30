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
//! 1. Read the channel and keep its generation.
//! 2. Sync the release log ([`ReleaseLog`]) and apply its install rule:
//!    a head signed within [`RELEASE_LOG_MAX_HEAD_AGE`], a `pass` as the
//!    latest verdict for the release on this node's contract version, and
//!    no revocation of the release, its app, or its dev key. A channel move
//!    to a release first passed before the active one is refused, unless
//!    the active one is revoked. Then read the release manifest by release
//!    id: it must hash to the release id, and its publisher key must be the
//!    dev key of the `pass`.
//! 3. Download the artifact, compare the byte digest, verify the signature.
//! 4. Unpack into the version directory.
//! 5. Move the `current` pointer.
//! 6. Observe the live process and prove the four-way match.
//!
//! A log refusal stops at step 2. A digest fault or a signature fault stops
//! at step 3. The `current` pointer never moves for a refused release or a
//! faulty artifact. The registry `revoked` flag decides nothing: the
//! recurring drift check reads revocations from the synced release log.
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
use schema_service_core::app_release::{
    compute_release_id_from_value, is_sha256_hex, sha256_hex, ReleaseManifest,
};
use serde::{Deserialize, Serialize};

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
/// One cycle reads the channel, syncs the release log, runs the four-way
/// check, and restores the prior verified release on drift or revocation.
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
mod release_log;

pub use activation::*;
pub use artifact::*;
pub use host_track::*;
pub use identity::*;
pub use proof::*;
pub use registry::*;
pub use release_log::*;
