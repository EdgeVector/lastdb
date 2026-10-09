use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("encryption error: {0}")]
    Crypto(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("network error: {0}")]
    Network(String),

    #[error("auth error: {0}")]
    Auth(String),

    #[error("auth banned: {0}")]
    Banned(String),

    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

    /// An owner-requested photograph reached a held cut that still needs
    /// cloud chunks. The continuous drain owns the same cut and will retry it,
    /// so this is a bounded retry state rather than a storage failure.
    #[error(
        "backup snapshot incomplete ({missing_chunks} chunks not yet in cloud); \
         the drain is holding this cut and will finish it \
         (uploaded_this_drain={uploaded_this_drain})"
    )]
    BackupSnapshotInProgress {
        missing_chunks: usize,
        uploaded_this_drain: usize,
    },

    /// The CAS verify step found held-cut chunks that cloud HEAD does not see
    /// yet, and every one of them is still a drainable candidate. The
    /// continuous drain owns the cut and will land them, so this is the same
    /// retry state as [`Self::BackupSnapshotInProgress`] observed one step
    /// later (after the local drain ledger said "complete"). It carries no
    /// per-drain upload count because the verify step has none.
    ///
    /// Sentry 7656855724: this site used to be `Storage(..)`, so the owner
    /// route answered an ERROR-logged 500 for a cut that was progressing.
    #[error(
        "backup snapshot incomplete ({missing_chunks} chunks not yet in cloud); \
         the drain is holding this cut and will finish it \
         (cloud verify lags the local drain)"
    )]
    BackupSnapshotVerifyPending { missing_chunks: usize },

    #[error("S3 error: {0}")]
    S3(String),

    #[error("corrupt log entry in '{target}' at seq {seq}: {reason}")]
    CorruptEntry {
        target: String,
        seq: u64,
        reason: String,
    },

    /// A cloud object selected only as a pre-upload decrypt proof was present
    /// but undecryptable. This fails closed for the current cycle, but it is not
    /// sufficient evidence of local key drift, so it must not tell the user to
    /// restore their account key/mnemonic.
    #[error("corrupt cloud proof object for '{target}' at {object}: {reason}")]
    CorruptProofObject {
        target: String,
        object: String,
        reason: String,
    },

    /// The current sync key could not decrypt the EXISTING cloud prefix, so we
    /// refuse to upload rather than append wrong-key ciphertext the correct
    /// node can never unseal (which would poison the shared log/snapshot).
    ///
    /// Unlike [`Self::CorruptEntry`] (one specific bad object hit during
    /// replay), this is a *pre-upload* proof failure: the log tail was empty or
    /// compacted, so replay decrypted nothing, and an explicit head-entry /
    /// snapshot decrypt-proof then failed. This is the cursor-at-head poison
    /// vector — a node whose local account/sync key drifted but whose download
    /// cursor is already at head. Operator action: restore the correct account
    /// key / mnemonic, then retry sync.
    #[error("sync key mismatch for '{target}': {reason}")]
    KeyProofFailed { target: String, reason: String },

    /// The local partitioner classified an upload entry as scoped, but the sync
    /// target list no longer contains the destination prefix. Refuse to reroute
    /// it to personal storage, which would seal share/org data under the wrong
    /// key and path.
    #[error("missing sync target for scoped destination '{share_prefix}'")]
    MissingSyncTarget { share_prefix: String },

    /// A sealed object used a log-envelope version this build does not
    /// understand (a forward-compatibility marker, not a key problem). Carried
    /// as a *typed* variant so replay can decide to skip-and-advance without
    /// string-matching decrypt error text.
    #[error("unsupported log envelope version: {version}")]
    UnsupportedEnvelope { version: u8 },

    /// A log entry that was fetched and **successfully decrypted** but whose
    /// decoded payload is **deterministically un-applicable** — e.g. the
    /// plaintext JSON does not deserialize into the record type its key
    /// implies. Unlike at-rest unwrap / seal failures ([`Self::Crypto`], which
    /// may be key drift and must NOT advance the cursor), a poison entry fails
    /// identically on every retry after a successful decrypt, so the replay
    /// loop SKIPS it (logs + advances the cursor) rather than wedging forever
    /// re-hitting the same seq.
    #[error("poison log entry in namespace '{namespace}': {reason}")]
    PoisonEntry { namespace: String, reason: String },

    /// A replayed entry was fetched, decrypted, and decoded, but **applying**
    /// it to the local store failed, so the download cursor did not advance
    /// past `seq`.
    ///
    /// This exists to give the pin a *location*. The apply site already knows
    /// the target and seq it stopped on; without this variant that knowledge
    /// only reached the daemon log, and the structured
    /// [`SyncReplayBlocker`](crate::sync::SyncReplayBlocker) — the field whose
    /// entire job is to name a stopped replay — stayed `None`, because
    /// `replay_blocker_from_error` matched two variants and an apply failure
    /// was neither.
    ///
    /// Measured on Tom's primary, 2026-08-16T21:52Z → 2026-08-17T02:1xZ: a
    /// LastStore catalog guard refused one replayed row with
    /// `StorageError::InvalidOperation`, which surfaced as [`Self::Storage`].
    /// Replay retried the same seq for over four hours; because a download
    /// failure blocks the same cycle's upload, **every** backup stopped with
    /// it. Throughout, `replay_blocker` read `null` and two scheduled runs had
    /// to grep the daemon log to learn which seq was pinned.
    ///
    /// Wrapping is deliberately narrow — see
    /// `SyncEngine::pin_replay_error`. `Network` keeps its own variant so the
    /// cycle still reports `Offline`, and the two already-typed poison
    /// variants keep their richer, more specific operator guidance.
    #[error("replay apply failed in '{target}' at seq {seq}: {reason}")]
    ReplayApplyFailed {
        target: String,
        seq: u64,
        reason: String,
    },

    #[error("sequence gap: expected {expected}, found {found}")]
    SequenceGap { expected: u64, found: u64 },

    #[error("device locked by {device_id}, expires at {expires_at}")]
    DeviceLocked {
        device_id: String,
        expires_at: String,
    },

    #[error("snapshot too large: {size_bytes} bytes")]
    SnapshotTooLarge { size_bytes: u64 },

    /// Bootstrap found NO snapshot (`latest.enc`) for a target whose cloud log
    /// prefix already holds history. Replaying only the log tail would restore a
    /// silently hollow (near-empty) store — the "missing snapshot" migration
    /// trap: a device restoring an existing identity must not come up hollow
    /// when its snapshot is missing or its upload failed. Pass an explicit
    /// accept-fresh opt-in (`SyncConfig::accept_fresh_bootstrap`) to start fresh
    /// on purpose despite the detected history.
    #[error(
        "missing snapshot for target '{target}': no snapshot (latest.enc) found, \
         but the cloud log prefix already holds {log_entries} log entries — \
         replaying only the log tail would restore a hollow store. Refusing to \
         bootstrap; set accept_fresh (SyncConfig::accept_fresh_bootstrap) to \
         start fresh anyway."
    )]
    MissingSnapshot { target: String, log_entries: usize },

    /// The pre-upload decryptability proof cannot be satisfied and **cannot
    /// become satisfiable by retrying**: the target prefix holds a log head
    /// this build cannot open as a proof object (forward envelope version, or
    /// over `max_download_entry_bytes`) *and* `snapshots/latest.enc` is absent,
    /// so there is no second proof object to fall back to.
    ///
    /// Distinct from [`Self::S3`], which the cycle treats as transient. This
    /// condition is **deterministic**: the same objects fail identically on
    /// every cycle, forever, while each attempt still pays the full staging and
    /// partition cost of a doomed upload. Measured 2026-08-09 on the primary:
    /// RSS 6.4 → 10.6 GiB over ~10 minutes of retrying a cycle that could never
    /// succeed, heading for the memory guard.
    ///
    /// Same reasoning class as [`Self::PoisonEntry`] on the replay side — a
    /// deterministic failure must be latched and surfaced, not retried.
    /// Never widen this to mean "upload anyway": an unproven prefix must still
    /// not be written. It exists to stop the *retry*, not the *refusal*.
    ///
    /// The latch is in-memory, so a daemon restart (e.g. after an upgrade that
    /// understands the envelope version, or a `max_download_entry_bytes`
    /// change) re-evaluates from scratch.
    #[error(
        "backup bootstrap blocked for '{target}': {reason}. This cannot succeed \
         by retrying — the cloud prefix holds no proof object this build can \
         open, and no snapshot to fall back to."
    )]
    BackupBootstrapBlocked { target: String, reason: String },

    /// The cloud `backup/latest` pointer names a backup object format this
    /// build does not read. The pointer decoded fine; only its
    /// `format_version` is outside what the v1 restore and publish paths
    /// understand. Carried as a *typed* variant so the restore CLI, the
    /// publisher's held-cut comparison, and failure classes can name it
    /// (`unsupported_backup_format`) instead of reporting a decode failure or
    /// a chain-walk miss. A v1 reader stops before any manifest or chunk
    /// request; it never installs a partial cut.
    #[error(
        "unsupported backup format: backup/latest names format_version {format_version}; \
         this build reads format_version 1"
    )]
    UnsupportedBackupFormat { format_version: u32 },

    #[error("wrong encryption key")]
    WrongKey,

    #[error("storage error: {0}")]
    Storage(String),
}

pub type SyncResult<T> = Result<T, SyncError>;

pub(crate) fn redact_sync_error_text(input: &str) -> String {
    let without_urls = redact_urls(input);
    redact_sensitive_pairs(&without_urls)
}

fn redact_urls(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;

    while let Some((idx, scheme_len)) = find_url_start(rest) {
        out.push_str(&rest[..idx]);
        out.push_str("[redacted-url]");

        let url_start = idx + scheme_len;
        let url_tail = &rest[url_start..];
        let url_end = url_tail
            .char_indices()
            .find_map(|(i, c)| is_url_delimiter(c).then_some(i))
            .unwrap_or(url_tail.len());
        rest = &url_tail[url_end..];
    }

    out.push_str(rest);
    out
}

fn find_url_start(input: &str) -> Option<(usize, usize)> {
    let http = input.find("http://").map(|idx| (idx, "http://".len()));
    let https = input.find("https://").map(|idx| (idx, "https://".len()));
    match (http, https) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn is_url_delimiter(c: char) -> bool {
    c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | '`' | ')' | ']')
}

fn redact_sensitive_pairs(input: &str) -> String {
    const SENSITIVE_KEYS: &[&str] = &[
        "x-amz-signature",
        "x-amz-credential",
        "x-amz-security-token",
        "x-amz-expires",
        "x-amz-date",
        "awsaccesskeyid",
        "signature",
        "authorization",
        "api_key",
        "apikey",
        "token",
        "secret",
    ];

    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        let remaining = &input[i..];
        let Some(key) = SENSITIVE_KEYS.iter().find(|key| {
            let key_bytes = key.as_bytes();
            remaining.len() >= key_bytes.len()
                && remaining.as_bytes()[..key_bytes.len()].eq_ignore_ascii_case(key_bytes)
        }) else {
            let ch = remaining.chars().next().expect("non-empty remaining");
            out.push(ch);
            i += ch.len_utf8();
            continue;
        };

        let after_key = i + key.len();
        let mut j = after_key;
        while j < input.len() {
            let ch = input[j..].chars().next().expect("valid utf-8 boundary");
            if ch.is_whitespace() {
                j += ch.len_utf8();
            } else {
                break;
            }
        }

        let Some(sep) = input[j..].chars().next() else {
            out.push_str(&input[i..]);
            break;
        };
        if sep != '=' && sep != ':' {
            let ch = remaining.chars().next().expect("non-empty remaining");
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        j += sep.len_utf8();
        while j < input.len() {
            let ch = input[j..].chars().next().expect("valid utf-8 boundary");
            if ch.is_whitespace() {
                j += ch.len_utf8();
            } else {
                break;
            }
        }

        let value_start = j;
        let redact_through_whitespace = *key == "authorization";
        while j < input.len() {
            let ch = input[j..].chars().next().expect("valid utf-8 boundary");
            if matches!(ch, '&' | ',' | ';' | ')' | ']' | '"' | '\'')
                || (!redact_through_whitespace && ch.is_whitespace())
            {
                break;
            }
            j += ch.len_utf8();
        }

        out.push_str(&input[i..value_start]);
        out.push_str("[redacted]");
        i = j;
    }
    out
}

impl From<crate::sharing::file_blob_seal::FileBlobSealError> for SyncError {
    fn from(error: crate::sharing::file_blob_seal::FileBlobSealError) -> Self {
        Self::Crypto(error.message().to_string())
    }
}

impl From<crate::crypto::CryptoError> for SyncError {
    fn from(e: crate::crypto::CryptoError) -> Self {
        Self::Crypto(e.to_string())
    }
}

impl From<crate::storage::error::StorageError> for SyncError {
    fn from(e: crate::storage::error::StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<serde_json::Error> for SyncError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

impl From<reqwest::Error> for SyncError {
    fn from(e: reqwest::Error) -> Self {
        Self::Network(redact_sync_error_text(&e.to_string()))
    }
}
