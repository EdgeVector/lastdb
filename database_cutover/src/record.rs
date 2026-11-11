//! An inactive request. Exemem is the only signed-only authority.
//!
//! This JSON is not a service receipt. `service_receipt` stays null until E
//! and I publish an authenticated receipt schema. A non-null value does not
//! parse. Writing this file does not change access and does not allow
//! retirement.

use serde::{Deserialize, Serialize};

use crate::check::{canonical_position, known_kind, label_ok, snapshot_ref_ok};
use crate::error::Reason;
use crate::wire::SCHEMA;

/// File name used in refusal text. The shell chooses the path.
pub const REQUEST_FILE: &str = "cutover-request.json";

const REQUEST_KIND: &str = "cutover_request";
const PHASE_READY: &str = "request_ready";
const SERVICE_AUTHORITY: &str = "exemem";
const RESUME_UNCHANGED: &str = "unchanged";
const RETIREMENT_FORBIDDEN: &str = "forbidden";

/// One prepared request for one database. `activation` is false.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// `cutover_request`.
    pub kind: String,
    /// [`SCHEMA`].
    pub schema: String,
    /// `dev`.
    pub environment: String,
    /// Database the operator named.
    pub database_id: String,
    /// `personal` or `shareable`.
    pub database_kind: String,
    /// `request_ready`. Never `signed_only`.
    pub phase: String,
    /// Always false. This file does not activate signed-only.
    pub activation: bool,
    /// `exemem`. The service is the only committer.
    pub authority: String,
    /// `unchanged`. A local file does not forbid legacy resume.
    pub legacy_resume: String,
    /// `forbidden`. A local file does not allow retirement.
    pub legacy_retirement: String,
    /// Always false.
    pub production_migration: bool,
    /// JSON null. Any receipt object is refused and is not stored.
    pub service_receipt: (),
    /// Snapshot pointer copied from the verified receipt.
    pub snapshot_ref: String,
    /// Canonical decimal copied from the verified legacy position.
    pub final_legacy_position: String,
    /// Owner or publisher node key. Evidence only.
    pub authority_node_key: String,
    /// Grant revision copied from the verified grants.
    pub grant_revision: u64,
    /// Device that would migrate. Evidence only.
    pub migrating_device_id: String,
    /// SHA-256 of the canonical evidence lines.
    pub evidence_sha256: String,
}

impl Request {
    pub(crate) fn consistent(&self) -> Result<(), Reason> {
        if self.kind != REQUEST_KIND
            || self.schema != SCHEMA
            || self.environment != "dev"
            || self.phase != PHASE_READY
            || self.activation
            || self.authority != SERVICE_AUTHORITY
            || self.legacy_resume != RESUME_UNCHANGED
            || self.legacy_retirement != RETIREMENT_FORBIDDEN
            || self.production_migration
            || !known_kind(&self.database_kind)
            || !label_ok(&self.database_id)
            || !snapshot_ref_ok(&self.snapshot_ref)
            || canonical_position(&self.final_legacy_position).is_none()
            || !label_ok(&self.authority_node_key)
            || self.grant_revision == 0
            || !label_ok(&self.migrating_device_id)
            || !hex64(&self.evidence_sha256)
        {
            return Err(Reason::RecordRejected);
        }
        Ok(())
    }
}

pub(crate) fn new_request(bundle: &crate::wire::Bundle, evidence_sha256: &str) -> Request {
    Request {
        kind: REQUEST_KIND.to_owned(),
        schema: SCHEMA.to_owned(),
        environment: "dev".to_owned(),
        database_id: bundle.snapshot.database_id.clone(),
        database_kind: bundle.snapshot.database_kind.clone(),
        phase: PHASE_READY.to_owned(),
        activation: false,
        authority: SERVICE_AUTHORITY.to_owned(),
        legacy_resume: RESUME_UNCHANGED.to_owned(),
        legacy_retirement: RETIREMENT_FORBIDDEN.to_owned(),
        production_migration: false,
        service_receipt: (),
        snapshot_ref: bundle.snapshot.snapshot_ref.clone(),
        final_legacy_position: bundle.legacy.position.clone(),
        authority_node_key: bundle.grants.authority_node_key.clone(),
        grant_revision: bundle.grants.revision,
        migrating_device_id: bundle.daemon.migrating_device_id.clone(),
        evidence_sha256: evidence_sha256.to_owned(),
    }
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
