use super::*;

/// Product write limits exposed on `/api/status` and `lastdb status`.
///
/// Byte ceilings are Instant config gauges; the env var name stays a string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LimitsHealth {
    /// Effective maximum HTTP request body on the owner Unix socket (bytes).
    #[serde(serialize_with = "serialize_gauge_option")]
    pub max_request_body_bytes: Gauge,
    /// Effective max serialized record field content (bytes).
    #[serde(serialize_with = "serialize_gauge_option")]
    pub max_field_value_bytes: Gauge,
    /// Compiled default (256 KiB) when env is unset.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub max_field_value_bytes_default: Gauge,
    /// Absolute max even if env is higher.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub max_field_value_bytes_absolute_max: Gauge,
    /// Env var name that overrides the default.
    pub max_field_value_bytes_env: String,
    /// The cap for the encoded record of a home (64 KiB). A record
    /// holds every field of one key, so it is the write limit there.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub max_record_bytes: Gauge,
}

impl Default for LimitsHealth {
    fn default() -> Self {
        Self {
            max_request_body_bytes: g_instant(
                lastdb_uds::uds_http::MAX_BODY_LEN as u64,
                Unit::Bytes,
            ),
            max_field_value_bytes: g_instant(
                fold_db::content_limit::max_field_value_bytes() as u64,
                Unit::Bytes,
            ),
            max_field_value_bytes_default: g_instant(
                fold_db::content_limit::DEFAULT_MAX_FIELD_VALUE_BYTES as u64,
                Unit::Bytes,
            ),
            max_field_value_bytes_absolute_max: g_instant(
                fold_db::content_limit::ABSOLUTE_MAX_FIELD_VALUE_BYTES as u64,
                Unit::Bytes,
            ),
            max_field_value_bytes_env: fold_db::content_limit::MAX_FIELD_VALUE_BYTES_ENV
                .to_string(),
            max_record_bytes: g_instant(
                fold_db::record_value::MAX_RECORD_BYTES as u64,
                Unit::Bytes,
            ),
        }
    }
}

impl<'de> Deserialize<'de> for LimitsHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            max_request_body_bytes: Option<u64>,
            #[serde(default)]
            max_field_value_bytes: Option<u64>,
            #[serde(default)]
            max_field_value_bytes_default: Option<u64>,
            #[serde(default)]
            max_field_value_bytes_absolute_max: Option<u64>,
            #[serde(default)]
            max_field_value_bytes_env: String,
            #[serde(default)]
            max_record_bytes: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            max_request_body_bytes: g_instant_wire(raw.max_request_body_bytes, Unit::Bytes),
            max_field_value_bytes: g_instant_wire(raw.max_field_value_bytes, Unit::Bytes),
            max_field_value_bytes_default: g_instant_wire(
                raw.max_field_value_bytes_default,
                Unit::Bytes,
            ),
            max_field_value_bytes_absolute_max: g_instant_wire(
                raw.max_field_value_bytes_absolute_max,
                Unit::Bytes,
            ),
            max_field_value_bytes_env: raw.max_field_value_bytes_env,
            max_record_bytes: g_instant_wire(raw.max_record_bytes, Unit::Bytes),
        })
    }
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
