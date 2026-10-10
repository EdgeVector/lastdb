use serde::Serialize;

/// Standard sensitivity levels for data classification.
/// Higher values indicate greater sensitivity.
pub const PUBLIC: u8 = 0;
pub const INTERNAL: u8 = 1;
pub const CONFIDENTIAL: u8 = 2;
pub const RESTRICTED: u8 = 3;
pub const HIGHLY_RESTRICTED: u8 = 4;

/// Maximum allowed sensitivity level.
pub const MAX_SENSITIVITY_LEVEL: u8 = 4;

/// Two-dimensional data classification label for a field.
///
/// Every field carries a classification with two independent dimensions:
/// - **sensitivity_level**: How sensitive the data is (0=Public … 4=Highly Restricted)
/// - **data_domain**: What kind of data it is (e.g. "financial", "medical", "identity")
///
/// Labels are partially ordered by sensitivity within the same domain.
/// Labels with different domains are incomparable — cross-domain information
/// flow requires explicit authorization.
///
/// Validated on construction AND deserialization — sensitivity_level > 4
/// or empty data_domain will produce an error in both paths.
///
/// ```text
/// ┌───────┬───────────────────┬──────────────────────────────────────────────┐
/// │ Level │ Name              │ Description                                  │
/// ├───────┼───────────────────┼──────────────────────────────────────────────┤
/// │   0   │ Public            │ Freely distributable. No access restrictions. │
/// │   1   │ Internal          │ Not sensitive but not for public release.     │
/// │   2   │ Confidential      │ Business-sensitive. Competitive value.        │
/// │   3   │ Restricted        │ Personally identifiable or attributable.      │
/// │   4   │ Highly Restricted │ Regulated data (HIPAA, financial, biometric). │
/// └───────┴───────────────────┴──────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Serialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct DataClassification {
    /// Sensitivity level: 0 (Public) through 4 (Highly Restricted).
    pub sensitivity_level: u8,
    /// Data domain tag identifying the type of data (e.g. "financial", "medical",
    /// "identity", "behavioral", "location", "general").
    pub data_domain: String,
}

// Custom Deserialize that validates sensitivity_level and data_domain,
// preventing invalid values from entering the system via JSON.
impl<'de> serde::Deserialize<'de> for DataClassification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            sensitivity_level: u8,
            data_domain: String,
        }
        let h = Helper::deserialize(deserializer)?;
        Self::new(h.sensitivity_level, h.data_domain).map_err(serde::de::Error::custom)
    }
}

impl DataClassification {
    /// Create a new DataClassification with validated sensitivity level.
    ///
    /// Returns `Err` if `sensitivity_level` exceeds `MAX_SENSITIVITY_LEVEL` (4)
    /// or if `data_domain` is empty.
    ///
    /// `data_domain` is normalized by trimming leading and trailing
    /// whitespace before storage so padded tags like `" medical "` do not
    /// persist as a distinct label from `"medical"`. Trim is the simplest
    /// fix that keeps trim-only inputs rejected by the existing empty-check.
    pub fn new(sensitivity_level: u8, data_domain: impl Into<String>) -> Result<Self, String> {
        if sensitivity_level > MAX_SENSITIVITY_LEVEL {
            return Err(format!(
                "sensitivity_level {sensitivity_level} exceeds maximum {MAX_SENSITIVITY_LEVEL} (Highly Restricted)"
            ));
        }
        let data_domain = data_domain.into();
        let trimmed = data_domain.trim();
        if trimmed.is_empty() {
            return Err("data_domain must not be empty".to_string());
        }
        Ok(Self {
            sensitivity_level,
            data_domain: trimmed.to_string(),
        })
    }

    /// Returns the human-readable name for this sensitivity level.
    pub fn sensitivity_name(&self) -> &'static str {
        match self.sensitivity_level {
            0 => "Public",
            1 => "Internal",
            2 => "Confidential",
            3 => "Restricted",
            4 => "Highly Restricted",
            _ => "Unknown",
        }
    }

    /// Convenience constructor for Low (Public, general domain).
    pub fn low() -> Self {
        Self {
            sensitivity_level: PUBLIC,
            data_domain: "general".to_string(),
        }
    }

    /// Convenience constructor for Medium (Confidential, general domain).
    pub fn medium() -> Self {
        Self {
            sensitivity_level: CONFIDENTIAL,
            data_domain: "general".to_string(),
        }
    }

    /// Convenience constructor for High (Highly Restricted, general domain).
    pub fn high() -> Self {
        Self {
            sensitivity_level: HIGHLY_RESTRICTED,
            data_domain: "general".to_string(),
        }
    }
}

impl PartialOrd for DataClassification {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataClassification {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sensitivity_level.cmp(&other.sensitivity_level)
    }
}

impl std::fmt::Display for DataClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.sensitivity_name(), self.data_domain)
    }
}
