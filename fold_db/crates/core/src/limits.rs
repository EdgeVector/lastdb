//! Runtime field-value byte limits and manifest retention TTL bounds.

pub mod content_limit {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub use lastdb_model::content_limit::{
        ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES, MAX_FIELD_VALUE_BYTES_ENV,
        MIN_MAX_FIELD_VALUE_BYTES,
    };

    static EFFECTIVE_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FIELD_VALUE_BYTES);

    pub fn set_max_field_value_bytes(limit: usize) {
        let prev = EFFECTIVE_LIMIT.swap(limit, Ordering::AcqRel);
        if prev != limit && limit != DEFAULT_MAX_FIELD_VALUE_BYTES {
            tracing::info!(
                target: "fold_db::content_limit",
                limit,
                default = DEFAULT_MAX_FIELD_VALUE_BYTES,
                absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
                env = MAX_FIELD_VALUE_BYTES_ENV,
                "peer field value size limit overridden from env"
            );
        }
    }

    #[must_use]
    pub fn max_field_value_bytes() -> usize {
        EFFECTIVE_LIMIT.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn parse_max_field_value_bytes(raw: Option<String>) -> usize {
        let Some(raw) = raw else {
            return DEFAULT_MAX_FIELD_VALUE_BYTES;
        };
        let s = raw.trim();
        if s.is_empty() {
            return DEFAULT_MAX_FIELD_VALUE_BYTES;
        }
        let lower = s.to_ascii_lowercase();
        let (num, mult) = [
            ("kib", 1024usize),
            ("mib", 1024 * 1024),
            ("kb", 1024),
            ("mb", 1024 * 1024),
            ("k", 1024),
            ("m", 1024 * 1024),
        ]
        .into_iter()
        .find_map(|(u, m)| lower.strip_suffix(u).map(|r| (r, m)))
        .unwrap_or((lower.as_str(), 1));
        let Some(parsed) = num
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(mult))
        else {
            tracing::warn!(
                raw = %s, env = MAX_FIELD_VALUE_BYTES_ENV,
                "invalid LASTDB_MAX_FIELD_VALUE_BYTES; using default {}",
                DEFAULT_MAX_FIELD_VALUE_BYTES
            );
            return DEFAULT_MAX_FIELD_VALUE_BYTES;
        };
        if parsed < MIN_MAX_FIELD_VALUE_BYTES {
            tracing::warn!(
                requested = parsed,
                min = MIN_MAX_FIELD_VALUE_BYTES,
                env = MAX_FIELD_VALUE_BYTES_ENV,
                "field value limit below minimum; clamping"
            );
            MIN_MAX_FIELD_VALUE_BYTES
        } else if parsed > ABSOLUTE_MAX_FIELD_VALUE_BYTES {
            tracing::warn!(
                requested = parsed, absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES, env = MAX_FIELD_VALUE_BYTES_ENV,
                "field value limit above absolute max; clamping (use the record's file for larger payloads)"
            );
            ABSOLUTE_MAX_FIELD_VALUE_BYTES
        } else {
            parsed
        }
    }
}

pub mod manifest_retention {
    use crate::platform::env_flag::var_parsed;
    use crate::schema::types::catalog_types::SchemaRetentionPolicy;
    use serde::{Deserialize, Serialize};

    const SECONDS_PER_DAY: u64 = 86_400;
    const MAX_MANIFEST_TTL_SECONDS: u64 = 3650 * SECONDS_PER_DAY;
    const MIN_MANIFEST_TTL_SECONDS: u64 = 3600;
    const MIN_MANIFEST_TTL_ENV: &str = "LASTDB_MIN_MANIFEST_TTL_SECONDS";

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
                    return Err("retention takes ttl_days or ttl_seconds, not both".into());
                }
            };
            let floor = var_parsed::<u64>(MIN_MANIFEST_TTL_ENV)
                .filter(|&s| s > 0)
                .unwrap_or(MIN_MANIFEST_TTL_SECONDS);
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
}
