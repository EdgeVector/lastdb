//! The error answer every route gives with a status other than 200.

use serde::{Deserialize, Serialize};

/// The class of a refusal. The HTTP status goes with it (see [`ErrorCode::status`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// 400: a field is malformed or out of range; `field` names it.
    InvalidRequest,
    /// 400: a batch is over its cap.
    BatchTooLarge,
    /// 401: a bad or missing request signature.
    Unauthorized,
    /// 402: billing refuses the publisher.
    PaymentRequired,
    /// 403: no current grant for this node key and role, or not the publisher.
    Forbidden,
    /// 404: a missing entry or blob.
    NotFound,
    /// 409: the stream is not where the request needs it (a segment out of order).
    Conflict,
    /// 409 on `blob.drop`: a stream is not as the caller named it, or was not named. Nothing was
    /// dropped; compute the list again.
    Heads,
    /// 410 on `log.read`: the read starts at an entry that `log.trim` dropped.
    Trimmed,
    /// 426: the request names another protocol version.
    UpgradeRequired,
    /// 429: a quota, rate or provider limit. `field` says whether the same request can succeed a
    /// moment later (see [`ErrorAnswer::is_transient`]).
    RateLimited,
    /// 500: an unknown or provider failure; retry.
    Unavailable,
    /// A code this build does not know. Never sent.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    /// The HTTP status a relay answers with this code.
    #[must_use]
    pub const fn status(self) -> u16 {
        match self {
            Self::InvalidRequest | Self::BatchTooLarge => 400,
            Self::Unauthorized => 401,
            Self::PaymentRequired => 402,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Conflict | Self::Heads => 409,
            Self::Trimmed => 410,
            Self::UpgradeRequired => 426,
            Self::RateLimited => 429,
            Self::Unavailable | Self::Unknown => 500,
        }
    }

    /// The code as it appears on the wire.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::BatchTooLarge => "batch_too_large",
            Self::Unauthorized => "unauthorized",
            Self::PaymentRequired => "payment_required",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Heads => "heads",
            Self::Trimmed => "trimmed",
            Self::UpgradeRequired => "upgrade_required",
            Self::RateLimited => "rate_limited",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
}

/// The body of every answer other than 200.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ErrorAnswer {
    /// The class of the refusal.
    pub code: ErrorCode,
    /// What it is about: a request field (`segment.first_seq`), a gate (`publisher`, `grant`), or
    /// a provider (`provider.dynamo`).
    #[serde(default)]
    pub field: String,
    /// On 426: the lowest protocol version the relay serves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_version: Option<u32>,
    /// On 403 with field `grant`: the grant index the relay consulted, for diagnosis. A relay may
    /// leave it out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<GrantDenial>,
}

impl ErrorAnswer {
    /// An answer with `code` and `field`.
    #[must_use]
    pub fn new(code: ErrorCode, field: impl Into<String>) -> Self {
        Self {
            code,
            field: field.into(),
            minimum_version: None,
            grant: None,
        }
    }

    /// Whether the same request can succeed a moment later: `unavailable` (500), and
    /// `rate_limited` (429) with a field that names a provider, a stored-progress listing, or a read
    /// that failed. A field that ends in `.configuration`, and every other refusal, is final. A
    /// gateway 502, 503 or 504 (no relay answer) is transient too; the caller decides that from the
    /// status.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        let field = self.field.as_str();
        match self.code {
            ErrorCode::Unavailable => true,
            ErrorCode::RateLimited => {
                !field.ends_with(".configuration")
                    && (field.starts_with("provider.")
                        || field.starts_with("log.streams.")
                        || matches!(
                            field,
                            "mutation_log.read" | "quota.read" | "payment.read" | "payment.clock"
                        ))
            }
            _ => false,
        }
    }
}

/// The grant index a relay consulted for a 403 with field `grant`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GrantDenial {
    /// The database whose index was read.
    pub database_id: String,
    /// The caller's node key.
    pub caller: String,
    /// Whether the index has a publisher.
    pub has_publisher: bool,
    /// The publisher's node key, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher_node_key: Option<String>,
    /// Whether `grant#any` gives read.
    pub any_read: bool,
    /// The role `grant#any` gives, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub any_role: Option<String>,
    /// The caller's role, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_role: Option<String>,
    /// Whether the caller is the publisher.
    pub publisher: bool,
    /// The grant records consulted.
    #[serde(default)]
    pub subjects: Vec<GrantSubject>,
}

/// One grant record a relay consulted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GrantSubject {
    /// The record's id.
    pub record_id: String,
    /// The grantee's node key, or `any`.
    pub grantee: String,
    /// The role it gives.
    pub role: String,
}
