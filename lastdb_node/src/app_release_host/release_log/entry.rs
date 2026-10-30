use super::*;

use app_identity_crypto::canonicalize;
use serde_json::{json, Value};

// ─── Entries ──────────────────────────────────────────────────────────────

/// One verified release log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The position in the log. The first entry is 1.
    pub seq: u64,
    /// The hash of the entry before, 64 lowercase hex.
    pub prev: String,
    /// The append time, RFC 3339 UTC with whole seconds.
    pub time: String,
    pub kind: LogEntryKind,
}

/// What one entry records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryKind {
    /// The verifier checked the release on one contract version: it passed.
    Pass(LogVerdict),
    /// The verifier checked the release on one contract version: it failed.
    Fail { verdict: LogVerdict, reason: String },
    /// A permanent revocation of a release, an app, or a developer key.
    Revoke {
        target_kind: RevokeTarget,
        target: String,
        reason: String,
    },
}

/// The fields a `pass` and a `fail` share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogVerdict {
    pub release_id: String,
    pub app_id: String,
    /// The exact LastDB version the release was checked on.
    pub contract_version: String,
    /// SHA-256 hex of the publisher's raw 32-byte Ed25519 key.
    pub dev_cert_sha256: String,
    /// The run id, `vr_` and 16 hex.
    pub run: String,
    /// The static rule version.
    pub rules: i64,
}

/// What a `revoke` entry names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RevokeTarget {
    Release,
    App,
    DevCert,
}

impl RevokeTarget {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::App => "app",
            Self::DevCert => "dev_cert",
        }
    }
}

impl LogEntry {
    /// The verdict of a `pass` or a `fail`.
    #[must_use]
    pub fn verdict(&self) -> Option<&LogVerdict> {
        match &self.kind {
            LogEntryKind::Pass(verdict) | LogEntryKind::Fail { verdict, .. } => Some(verdict),
            LogEntryKind::Revoke { .. } => None,
        }
    }

    #[must_use]
    pub fn is_pass(&self) -> bool {
        matches!(self.kind, LogEntryKind::Pass(_))
    }

    /// One line for an operator, for example
    /// `entry 12: revoke release 4b1e…: owner request`.
    #[must_use]
    pub fn summary(&self) -> String {
        match &self.kind {
            LogEntryKind::Pass(verdict) => format!(
                "entry {}: pass of {} on {}",
                self.seq, verdict.release_id, verdict.contract_version
            ),
            LogEntryKind::Fail { verdict, reason } => format!(
                "entry {}: fail of {} on {}: {reason}",
                self.seq, verdict.release_id, verdict.contract_version
            ),
            LogEntryKind::Revoke {
                target_kind,
                target,
                reason,
            } => format!(
                "entry {}: revoke {} {target}: {reason}",
                self.seq,
                target_kind.label()
            ),
        }
    }
}

/// The JSON shape of a tile line, before the checks for its kind.
///
/// Fields the format does not name are allowed: they are part of the
/// canonical bytes, so the entry hash covers them.
#[derive(Deserialize)]
struct RawEntry {
    seq: u64,
    prev: String,
    time: String,
    kind: String,
    release_id: Option<String>,
    app_id: Option<String>,
    contract_version: Option<String>,
    #[serde(rename = "dev_cert")]
    dev_cert_sha256: Option<String>,
    run: Option<String>,
    rules: Option<i64>,
    reason: Option<String>,
    target_kind: Option<String>,
    target: Option<String>,
}

/// Parse and check one tile line as entry `seq`, chained to `prev`.
///
/// The line must be the canonical bytes of its own JSON value, so the hash
/// of the line is the hash of the entry. The field checks are the ones the
/// writer runs before it appends, so the node accepts exactly the entries
/// the verifier can write. Returns the entry and its hash.
///
/// # Errors
/// Names the entry and the check it failed.
pub(super) fn parse_tile_line(
    line: &str,
    seq: u64,
    prev: &str,
) -> Result<(LogEntry, String), String> {
    let value: Value = serde_json::from_str(line)
        .map_err(|e| format!("release log entry {seq} is not JSON: {e}"))?;
    let canonical = canonicalize(&value)
        .map_err(|e| format!("release log entry {seq} does not canonicalize: {e}"))?;
    if canonical != line.as_bytes() {
        return Err(format!(
            "release log entry {seq} is not in RFC 8785 canonical form"
        ));
    }
    let raw: RawEntry = serde_json::from_value(value)
        .map_err(|e| format!("release log entry {seq} has invalid fields: {e}"))?;
    let entry = raw.into_entry(seq, prev)?;
    Ok((entry, sha256_hex(line.as_bytes())))
}

impl RawEntry {
    fn into_entry(self, seq: u64, prev: &str) -> Result<LogEntry, String> {
        let invalid = |what: &str| format!("release log entry {seq} has an invalid {what}");
        if self.seq != seq {
            return Err(format!("release log entry {seq} carries seq {}", self.seq));
        }
        if self.prev != prev {
            return Err(format!(
                "release log entry {seq} does not chain to the entry before"
            ));
        }
        if !is_log_time(&self.time) {
            return Err(invalid("time"));
        }
        let kind = match self.kind.as_str() {
            "pass" | "fail" => {
                let verdict = LogVerdict {
                    release_id: self
                        .release_id
                        .filter(|id| is_sha256_hex(id))
                        .ok_or_else(|| invalid("release_id"))?,
                    app_id: self.app_id.ok_or_else(|| invalid("app_id"))?,
                    contract_version: self
                        .contract_version
                        .ok_or_else(|| invalid("contract_version"))?,
                    dev_cert_sha256: self
                        .dev_cert_sha256
                        .filter(|hex| is_sha256_hex(hex))
                        .ok_or_else(|| invalid("dev_cert"))?,
                    run: self
                        .run
                        .filter(|run| is_run_id(run))
                        .ok_or_else(|| invalid("run"))?,
                    rules: self.rules.ok_or_else(|| invalid("rules"))?,
                };
                if self.kind == "pass" {
                    if self.reason.is_some() {
                        return Err(invalid("pass: a pass carries no reason"));
                    }
                    LogEntryKind::Pass(verdict)
                } else {
                    LogEntryKind::Fail {
                        verdict,
                        reason: self
                            .reason
                            .filter(|r| is_reason(r))
                            .ok_or_else(|| invalid("reason"))?,
                    }
                }
            }
            "revoke" => LogEntryKind::Revoke {
                target_kind: match self.target_kind.as_deref() {
                    Some("release") => RevokeTarget::Release,
                    Some("app") => RevokeTarget::App,
                    Some("dev_cert") => RevokeTarget::DevCert,
                    _ => return Err(invalid("target_kind")),
                },
                target: self
                    .target
                    .filter(|target| !target.is_empty())
                    .ok_or_else(|| invalid("target"))?,
                reason: self
                    .reason
                    .filter(|r| is_reason(r))
                    .ok_or_else(|| invalid("reason"))?,
            },
            _ => return Err(invalid("kind")),
        };
        Ok(LogEntry {
            seq,
            prev: self.prev,
            time: self.time,
            kind,
        })
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ`: RFC 3339 UTC with whole seconds.
fn is_log_time(time: &str) -> bool {
    let bytes = time.as_bytes();
    bytes.len() == 20
        && bytes.iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            10 => *b == b'T',
            13 | 16 => *b == b':',
            19 => *b == b'Z',
            _ => b.is_ascii_digit(),
        })
}

/// `vr_` and 16 lowercase hex.
fn is_run_id(run: &str) -> bool {
    run.strip_prefix("vr_").is_some_and(|hex| {
        hex.len() == 16
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// 1 to 512 characters, counted as the writer counts them (UTF-16 units).
fn is_reason(reason: &str) -> bool {
    (1..=REASON_MAX_UNITS).contains(&reason.encode_utf16().count())
}

// ─── The signed head ──────────────────────────────────────────────────────

/// The signed head: the last entry the log covers, and when the verifier
/// signed that statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedHead {
    /// The last entry the head covers. An empty log has 0.
    pub seq: u64,
    /// The hash of entry `seq`, or 64 zeros for an empty log.
    pub hash: String,
    /// When the verifier signed the head, RFC 3339 UTC.
    pub time: String,
    /// SHA-256 hex of the raw 32-byte Ed25519 key that signed the head.
    pub key_id: String,
    /// Base64 Ed25519 signature over the canonical head without `sig`.
    pub sig: String,
}

impl SignedHead {
    /// Check the fields and the signature against the trusted keys.
    ///
    /// # Errors
    /// Refuses a key id that is not trusted, a malformed field, and a
    /// signature that does not verify.
    pub(super) fn verify(&self, signers: &[(String, VerifyingKey)]) -> Result<(), String> {
        if !is_sha256_hex(&self.hash) {
            return Err("release log head hash is not 64 lowercase hex".to_string());
        }
        self.signed_at()?;
        let (_, key) = signers
            .iter()
            .find(|(id, _)| *id == self.key_id)
            .ok_or_else(|| {
                format!(
                    "release log head is signed by key {}, which this node does not trust",
                    self.key_id
                )
            })?;
        let raw = BASE64
            .decode(self.sig.as_bytes())
            .map_err(|e| format!("release log head sig is not base64: {e}"))?;
        let signature: [u8; app_identity_crypto::SIGNATURE_LEN] = raw
            .as_slice()
            .try_into()
            .map_err(|_| format!("release log head sig is {} bytes, want 64", raw.len()))?;
        // The signed body is the head without `sig`. It is built from the
        // four named fields, as the writer builds it.
        let body = canonicalize(&json!({
            "hash": self.hash,
            "key_id": self.key_id,
            "seq": self.seq,
            "time": self.time,
        }))
        .map_err(|e| format!("release log head does not canonicalize: {e}"))?;
        verify(key, &signature, &body)
            .map_err(|e| format!("release log head signature does not verify: {e}"))
    }

    /// The head time as a UTC instant.
    ///
    /// # Errors
    /// Returns the parse failure.
    pub fn signed_at(&self) -> Result<chrono::DateTime<chrono::Utc>, String> {
        chrono::DateTime::parse_from_rfc3339(&self.time)
            .map(|at| at.with_timezone(&chrono::Utc))
            .map_err(|e| format!("release log head time '{}' does not parse: {e}", self.time))
    }
}
