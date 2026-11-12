//! App manifest `retention` entry parsing and plan/outcome types.

use crate::schema::SchemaRetentionPolicy;
use serde::{Deserialize, Serialize};

pub const MAX_MANIFEST_TTL_SECONDS: u64 = 3650 * SECONDS_PER_DAY;
pub const MIN_MANIFEST_TTL_SECONDS: u64 = 3600;
pub const MIN_MANIFEST_TTL_ENV: &str = "LASTDB_MIN_MANIFEST_TTL_SECONDS";
const SECONDS_PER_DAY: u64 = 86_400;

pub fn min_manifest_ttl_seconds() -> u64 {
    std::env::var(MIN_MANIFEST_TTL_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(MIN_MANIFEST_TTL_SECONDS)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRetention {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_days: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_expire_existing: Option<bool>,
}

impl ManifestRetention {
    pub fn confirmed(&self) -> bool {
        self.confirm_expire_existing == Some(true)
    }

    pub fn ttl(&self) -> Result<u64, String> {
        let seconds = match (self.ttl_days, self.ttl_seconds) {
            (Some(d), None) => d.checked_mul(SECONDS_PER_DAY),
            (None, Some(s)) => Some(s),
            (None, None) => return Err("retention needs ttl_days or ttl_seconds".into()),
            (Some(_), Some(_)) => {
                return Err("retention takes ttl_days or ttl_seconds, not both".into())
            }
        };
        let floor = min_manifest_ttl_seconds();
        match seconds {
            Some(0) => Err("retention ttl must be greater than zero".into()),
            Some(s) if s < floor => Err(format!(
                "retention ttl of {s} seconds is below the floor of {floor} seconds. \
                 A TTL deletes records for good, and a value this small is usually a unit \
                 slip (ttl_seconds: 90 meant as days). Use ttl_days, or set \
                 {MIN_MANIFEST_TTL_ENV} to lower the floor"
            )),
            Some(s) if s <= MAX_MANIFEST_TTL_SECONDS => Ok(s),
            _ => Err(format!(
                "retention ttl is too large: the limit is {MAX_MANIFEST_TTL_SECONDS} seconds (3650 days)"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestRetentionStatus {
    Applied,
    Updated,
    Unchanged,
    OperatorOverride,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestRetentionPlan {
    pub anchor: String,
    pub current: Option<SchemaRetentionPolicy>,
    pub next: Option<SchemaRetentionPolicy>,
    pub status: ManifestRetentionStatus,
    pub shortened: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManifestRetentionOutcome {
    pub anchor: String,
    pub status: ManifestRetentionStatus,
    pub previous: Option<SchemaRetentionPolicy>,
    pub effective: SchemaRetentionPolicy,
    pub shortened: bool,
}
