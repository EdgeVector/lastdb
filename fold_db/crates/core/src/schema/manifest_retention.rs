//! The `retention` key of an app manifest schema entry.
//!
//! This is a request to the node, not part of the schema. It is parsed beside
//! [`crate::schema::types::declarative_schemas::DeclarativeSchemaDefinition`]
//! and never stored in it, so it cannot change a schema artifact, its identity
//! hash, or canonicalization. The identity hash covers the owner app, the
//! readable name, the sorted field names and the key layout, and nothing else.

use crate::schema::SchemaRetentionPolicy;
use serde::{Deserialize, Serialize};

/// The longest TTL a manifest may declare: ten years.
///
/// A larger value is almost certainly a unit mistake (milliseconds given as
/// seconds), and a TTL that long never expires anything in practice.
pub const MAX_MANIFEST_TTL_SECONDS: u64 = 3650 * SECONDS_PER_DAY;

/// The shortest TTL a manifest may declare: one hour.
///
/// A smaller value is almost always a unit slip (`ttl_seconds: 90` meant as
/// days), and a TTL deletes records for good. Lower the floor only with
/// [`MIN_MANIFEST_TTL_ENV`], on the node and on the machine that runs the CLI.
pub const MIN_MANIFEST_TTL_SECONDS: u64 = 3600;

/// Environment variable that replaces [`MIN_MANIFEST_TTL_SECONDS`].
pub const MIN_MANIFEST_TTL_ENV: &str = "LASTDB_MIN_MANIFEST_TTL_SECONDS";

const SECONDS_PER_DAY: u64 = 86_400;

/// The floor in force: [`MIN_MANIFEST_TTL_ENV`] when it holds a positive
/// integer, otherwise [`MIN_MANIFEST_TTL_SECONDS`].
pub fn min_manifest_ttl_seconds() -> u64 {
    std::env::var(MIN_MANIFEST_TTL_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(MIN_MANIFEST_TTL_SECONDS)
}

/// `"retention": { "ttl_days": 90 }` or `"retention": { "ttl_seconds": 7200 }`.
///
/// Give exactly one of the two TTL keys. Any other key is an error, so a
/// misspelled key can never leave a schema without the TTL the author meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRetention {
    /// Maximum age in whole days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_days: Option<u64>,
    /// Maximum age in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    /// Confirms that records which already exist may expire.
    ///
    /// The node refuses a TTL that would delete existing records (a new TTL on
    /// a Range or HashRange schema that holds data, or a shorter TTL than the
    /// one running) unless this is `true`. Prefer the per-run
    /// `lastdb app check --sync --confirm-expire-existing` flag: a value kept
    /// in the manifest confirms every later change as well.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_expire_existing: Option<bool>,
}

impl ManifestRetention {
    /// Parse the JSON value of a manifest `retention` key and validate it.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        let parsed: Self = serde_json::from_value(value.clone()).map_err(|e| {
            format!(
                "invalid retention: {e}; write {{\"ttl_days\": 90}} or {{\"ttl_seconds\": 7200}}"
            )
        })?;
        parsed.ttl()?;
        Ok(parsed)
    }

    /// True when the manifest or the caller confirmed that existing records
    /// may expire.
    pub fn confirmed(&self) -> bool {
        self.confirm_expire_existing == Some(true)
    }

    /// The TTL in seconds. Rejects zero, a missing or doubled key, an
    /// overflow, anything below [`min_manifest_ttl_seconds`] and anything above
    /// [`MAX_MANIFEST_TTL_SECONDS`].
    pub fn ttl(&self) -> Result<u64, String> {
        let seconds = match (self.ttl_days, self.ttl_seconds) {
            (Some(days), None) => days.checked_mul(SECONDS_PER_DAY),
            (None, Some(seconds)) => Some(seconds),
            (None, None) => {
                return Err("retention needs ttl_days or ttl_seconds".to_string());
            }
            (Some(_), Some(_)) => {
                return Err("retention takes ttl_days or ttl_seconds, not both".to_string());
            }
        };
        let floor = min_manifest_ttl_seconds();
        match seconds {
            Some(0) => Err("retention ttl must be greater than zero".to_string()),
            Some(seconds) if seconds < floor => Err(format!(
                "retention ttl of {seconds} seconds is below the floor of {floor} seconds. \
                 A TTL deletes records for good, and a value this small is usually a unit \
                 slip (ttl_seconds: 90 meant as days). Use ttl_days, or set \
                 {MIN_MANIFEST_TTL_ENV} to lower the floor"
            )),
            Some(seconds) if seconds <= MAX_MANIFEST_TTL_SECONDS => Ok(seconds),
            _ => Err(format!(
                "retention ttl is too large: the limit is {MAX_MANIFEST_TTL_SECONDS} seconds (3650 days)"
            )),
        }
    }
}

/// What applying a manifest retention to a schema did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestRetentionStatus {
    /// No policy was in force. The manifest policy is in force now.
    Applied,
    /// A manifest policy with another TTL was in force. The TTL changed.
    Updated,
    /// The same manifest policy was already in force. Nothing changed.
    Unchanged,
    /// An operator policy is in force and wins. The manifest TTL is kept and
    /// returns when the operator clears the operator policy.
    OperatorOverride,
}

/// What [`crate::db_operations::schema_store::SchemaStore::plan_manifest_retention`]
/// found. Nothing is written to produce it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestRetentionPlan {
    /// The schema that will hold the retention state.
    pub anchor: String,
    /// The anchor the manifest name pointed at before, when the declare moved
    /// it. The whole policy, tracking included, follows the schema.
    pub moved_from: Option<String>,
    /// The policy in force before the call (after any move).
    pub current: Option<SchemaRetentionPolicy>,
    /// The policy that will be in force after the call. `None` when nothing
    /// changes.
    pub next: Option<SchemaRetentionPolicy>,
    pub status: ManifestRetentionStatus,
    /// True when the manifest TTL is shorter than the manifest TTL running.
    pub shortened: bool,
}

/// Result of [`crate::db_operations::schema_store::SchemaStore::apply_manifest_retention`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestRetentionOutcome {
    /// The schema that holds the retention state for both installed names.
    pub anchor: String,
    pub status: ManifestRetentionStatus,
    /// The policy in force before the call (after any move).
    pub previous: Option<SchemaRetentionPolicy>,
    /// The policy in force after the call.
    pub effective: SchemaRetentionPolicy,
    /// The anchor the policy came from, when the declare moved it.
    pub moved_from: Option<String>,
    /// True when the manifest TTL is shorter than the manifest TTL running.
    pub shortened: bool,
}

/// Who else reads and writes the records a manifest TTL would delete.
///
/// A TTL sweep runs through the catalog schema. It removes a record from every
/// molecule that schema names, so it reaches every other schema that is bound
/// to it, and every record it holds, whoever wrote it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RetentionSharing {
    /// Other loaded schemas bound to the catalog schema (or to a schema it was
    /// expanded from), by field mapper, retention alias or shared molecule.
    /// The declared manifest name is not listed.
    pub shared_with: Vec<String>,
    /// Schemas in the catalog schema's chain that another app owns.
    pub foreign_owners: Vec<String>,
}

impl RetentionSharing {
    /// The reason a manifest TTL must be refused, or `None` when it is safe.
    pub fn refusal(&self, binding: &str, anchor: &str, owner_app: &str) -> Option<String> {
        let mut reasons = Vec::new();
        if !self.foreign_owners.is_empty() {
            reasons.push(format!(
                "the catalog schema {anchor} chain holds schemas that app '{owner_app}' does \
                 not own: [{}]",
                self.foreign_owners.join(", ")
            ));
        }
        if !self.shared_with.is_empty() {
            reasons.push(format!(
                "shared_with=[{}]: other schemas are bound to the catalog schema {anchor}",
                self.shared_with.join(", ")
            ));
        }
        if reasons.is_empty() {
            return None;
        }
        Some(format!(
            "a manifest TTL on {binding} is refused. The TTL sweep runs through {anchor} and \
             deletes the records of every schema bound to it. {}. Give each schema its own \
             shape, or set a policy on purpose with `lastdb schema-retention set`",
            reasons.join("; ")
        ))
    }
}

/// A retention policy whose sweep would delete through a schema that shares
/// records with a schema being declared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RetentionPolicyInReach {
    /// The schema that holds the policy. Its sweep deletes through this name.
    pub anchor: String,
    /// The manifest names that declared the TTL, other than the schema being
    /// declared. Empty when an operator set the policy on the schema directly.
    pub declared_by: Vec<String>,
    pub policy: SchemaRetentionPolicy,
}

/// The retention policies a schema being declared would fall under.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RetentionReach {
    pub policies: Vec<RetentionPolicyInReach>,
    /// True when the schema is already bound to the catalog schema. Declaring
    /// it again joins nothing new, so an app that declares its schemas on every
    /// start is warned, not refused.
    pub binding_bound: bool,
}
