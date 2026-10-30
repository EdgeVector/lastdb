//! The release log: the only record a node reads to decide an app install.
//!
//! The release verifier checks each published release and appends signed,
//! hash-chained entries to a public log (format 1; the spec is
//! `docs/release-log.md` in `EdgeVector/lastdb-verifier`). The registry
//! stores files and is not on the trust path. A node installs release `R` on
//! its contract version `V` only when both hold:
//!
//! 1. the latest `pass` or `fail` entry for `(R, V)` is a `pass`, and
//! 2. no `revoke` entry names `R`, the app id of `R`, or the dev key of `R`.
//!
//! ## What a node reads
//!
//! `{base}/head.json` — the signed head — and `{base}/tiles/<t>.jsonl`, where
//! tile `t` holds entries `64t+1` to `64t+64`, one canonical line each. A
//! sync reads only the tiles that hold entries after the kept ones.
//!
//! ## The kept log
//!
//! ```text
//! <host-track root>/release-log/<log>/
//!   entries.jsonl   the tile lines of entries 1..=head.seq
//!   head.json       the last head this node verified
//! ```
//!
//! `<log>` is the environment name (`dev`, `prod`), so each environment's
//! log is kept apart. A new entry must chain to the last kept hash, so a log
//! that rewrites history fails the first new entry, and a head below the
//! kept seq is refused outright. A registry or log outage stops new installs;
//! the kept entries still answer the revocation check for installed apps.

use super::*;
use std::collections::HashMap;

use app_identity_crypto::{key_id, VerifyingKey};
use folddb_profile::endpoints::{release_log_keys_for, release_log_url_for, Environment};

mod entry;
mod rules;
mod sync;

pub use entry::*;
pub use sync::*;

/// Entries per tile: tile `t` holds entries `64t+1` to `64t+64`.
pub const RELEASE_LOG_TILE_SIZE: u64 = 64;

/// The oldest signed head a node accepts for a new install or update.
///
/// The verifier signs a new head at least once an hour. A head older than
/// this means the node cannot see the current log, so it refuses new
/// installs and updates. Installed apps keep running.
pub const RELEASE_LOG_MAX_HEAD_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// The `prev` of entry 1 and the `hash` of an empty log's head.
pub const RELEASE_LOG_ZERO_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

/// The contract version this node checks a release against: the exact
/// `lastdbd --version` text without `lastdbd `.
pub const NODE_CONTRACT_VERSION: &str = env!("FOLDDB_BUILD_VERSION");

/// Names a release log other than the environment's own, for example a
/// verifier under test. Set it together with [`RELEASE_LOG_KEYS_ENV`].
pub const RELEASE_LOG_URL_ENV: &str = "LASTDB_RELEASE_LOG_URL";

/// Comma-separated base64 keys that replace the environment's trusted
/// release log keys.
pub const RELEASE_LOG_KEYS_ENV: &str = "LASTDB_RELEASE_LOG_KEYS";

/// The longest entry `reason`, in UTF-16 units as the writer counts.
const REASON_MAX_UNITS: usize = 512;

const ENTRIES_FILE: &str = "entries.jsonl";
const HEAD_FILE: &str = "head.json";

// ─── Configuration ────────────────────────────────────────────────────────

/// Which log a node reads and which keys it trusts to sign the head.
#[derive(Debug, Clone)]
pub struct ReleaseLogConfig {
    /// The kept-log directory under `<root>/release-log/`. One log, one name.
    pub name: String,
    /// The log base URL, without a trailing slash. `file://` reads from disk.
    pub base_url: String,
    /// Base64 raw 32-byte Ed25519 public keys of the verifier.
    pub signer_keys: Vec<String>,
}

impl ReleaseLogConfig {
    /// The environment's log from `environments.json`, unless
    /// [`RELEASE_LOG_URL_ENV`] or [`RELEASE_LOG_KEYS_ENV`] replace it.
    #[must_use]
    pub fn for_environment(env: Environment) -> Self {
        let signer_keys = env_value(RELEASE_LOG_KEYS_ENV).map_or_else(
            || {
                release_log_keys_for(env)
                    .iter()
                    .copied()
                    .map(str::to_string)
                    .collect()
            },
            |raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|key| !key.is_empty())
                    .map(str::to_string)
                    .collect()
            },
        );
        // An overriding URL gets its own kept log, so its entries never mix
        // with the environment's.
        let (name, base_url) = match env_value(RELEASE_LOG_URL_ENV) {
            Some(url) => (format!("{env}-{}", &sha256_hex(url.as_bytes())[..16]), url),
            None => (env.to_string(), release_log_url_for(env).to_string()),
        };
        Self {
            name,
            base_url: base_url.trim_end_matches('/').to_string(),
            signer_keys,
        }
    }

    /// The trusted keys, each with its key id.
    ///
    /// # Errors
    /// Refuses an environment with no key: it has no release log yet, so no
    /// install can be checked. Also refuses a key that does not parse.
    pub fn verifying_keys(&self) -> Result<Vec<(String, VerifyingKey)>, String> {
        if self.signer_keys.is_empty() {
            return Err(format!(
                "release log is not configured for this environment ({}): no verifier \
                 key is set, so this node refuses every app install",
                self.name
            ));
        }
        self.signer_keys
            .iter()
            .map(|raw| {
                let key = verifying_key_from_base64(raw)
                    .map_err(|e| format!("release log key '{raw}' does not parse: {e}"))?;
                Ok((key_id(&key), key))
            })
            .collect()
    }
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The log's `dev_cert` for a base64 publisher key: SHA-256 hex of the raw
/// 32-byte Ed25519 key.
///
/// # Errors
/// Returns the key parse failure.
pub fn dev_cert_sha256_of(publisher_dev_pubkey: &str) -> Result<String, String> {
    verifying_key_from_base64(publisher_dev_pubkey)
        .map(|key| key_id(&key))
        .map_err(|e| format!("publisher key does not parse: {e}"))
}

// ─── The kept log ─────────────────────────────────────────────────────────

/// The entries this node verified, the head that covers them, and the
/// lookups the install and drift checks need.
#[derive(Debug)]
pub struct ReleaseLog {
    config: ReleaseLogConfig,
    dir: PathBuf,
    /// The tile line of each entry, without its `\n`.
    lines: Vec<String>,
    entries: Vec<LogEntry>,
    hashes: Vec<String>,
    head: Option<SignedHead>,
    index: LogIndex,
}

/// Exact-key lookups over the entries, kept as the entries are added.
#[derive(Debug, Default)]
struct LogIndex {
    /// `(release_id, contract_version)` → position of the latest pass or fail.
    verdicts: HashMap<(String, String), usize>,
    /// `release_id` → seq of its first pass, over all contract versions.
    first_pass: HashMap<String, u64>,
    /// `release_id` → every dev key a verdict attributes the release to.
    dev_cert_hashes: HashMap<String, BTreeSet<String>>,
    /// `(target kind, target)` → position of the first revoke that names it.
    revocations: HashMap<(RevokeTarget, String), usize>,
}

/// What the log says about a release this node may install.
#[derive(Debug, Clone, Serialize)]
pub struct InstallVerdict {
    /// The seq of the pass for this node's contract version.
    pub pass_seq: u64,
    /// The publisher key the pass names.
    pub dev_cert_sha256: String,
    /// The seq of the head the decision was made on.
    pub head_seq: u64,
}

impl ReleaseLog {
    /// Open the kept log for `config` under the Host Track `root`. No kept
    /// log is an empty log at seq 0.
    ///
    /// # Errors
    /// A read failure, or kept files that do not match their own head.
    pub fn open(root: &Path, config: ReleaseLogConfig) -> Result<Self, String> {
        let dir = root.join("release-log").join(&config.name);
        let mut log = Self {
            config,
            dir,
            lines: Vec::new(),
            entries: Vec::new(),
            hashes: Vec::new(),
            head: None,
            index: LogIndex::default(),
        };
        let head: Option<SignedHead> = read_optional(&log.dir.join(HEAD_FILE))?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map_err(|e| format!("the kept release log head does not parse: {e}"))?;
        let covered = head.as_ref().map_or(0, |h| h.seq);
        let kept = read_optional(&log.dir.join(ENTRIES_FILE))?.unwrap_or_default();
        let kept = String::from_utf8(kept)
            .map_err(|e| format!("the kept release log is not UTF-8: {e}"))?;
        // Lines past the head are from a sync that stopped before it wrote
        // the head. The next sync reads them again.
        for line in kept.split('\n').filter(|line| !line.is_empty()) {
            if log.seq() == covered {
                break;
            }
            let (entry, hash) = parse_tile_line(line, log.seq() + 1, log.last_hash())?;
            log.push(line.to_string(), entry, hash);
        }
        if log.seq() != covered || head.as_ref().is_some_and(|h| h.hash != log.last_hash()) {
            return Err(format!(
                "the kept release log in {} does not match its head; remove that \
                 directory and the next sync reads the log again",
                log.dir.display()
            ));
        }
        log.head = head;
        Ok(log)
    }

    #[must_use]
    pub fn config(&self) -> &ReleaseLogConfig {
        &self.config
    }

    /// The last kept seq. 0 for an empty log.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.entries.len() as u64
    }

    /// The last head this node verified.
    #[must_use]
    pub fn head(&self) -> Option<&SignedHead> {
        self.head.as_ref()
    }

    fn last_hash(&self) -> &str {
        self.hashes
            .last()
            .map_or(RELEASE_LOG_ZERO_HASH, String::as_str)
    }

    fn push(&mut self, line: String, entry: LogEntry, hash: String) {
        let position = self.entries.len();
        if let Some(verdict) = entry.verdict() {
            let release_id = verdict.release_id.clone();
            self.index.verdicts.insert(
                (release_id.clone(), verdict.contract_version.clone()),
                position,
            );
            self.index
                .dev_cert_hashes
                .entry(release_id.clone())
                .or_default()
                .insert(verdict.dev_cert_sha256.clone());
            if entry.is_pass() {
                self.index.first_pass.entry(release_id).or_insert(entry.seq);
            }
        } else if let LogEntryKind::Revoke {
            target_kind,
            target,
            ..
        } = &entry.kind
        {
            self.index
                .revocations
                .entry((*target_kind, target.clone()))
                .or_insert(position);
        }
        self.lines.push(line);
        self.entries.push(entry);
        self.hashes.push(hash);
    }

    /// Write the kept log. Entries first, then the head, each by rename, so
    /// a crash leaves at worst extra lines that no kept head covers.
    fn persist(&self, entries_changed: bool) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| format!("failed to create {}: {e}", self.dir.display()))?;
        if entries_changed {
            let mut text = String::new();
            for line in &self.lines {
                text.push_str(line);
                text.push('\n');
            }
            write_by_rename(&self.dir.join(ENTRIES_FILE), text.as_bytes())?;
        }
        if let Some(head) = &self.head {
            let bytes = serde_json::to_vec_pretty(head)
                .map_err(|e| format!("failed to encode the release log head: {e}"))?;
            write_by_rename(&self.dir.join(HEAD_FILE), &bytes)?;
        }
        Ok(())
    }
}

/// The file bytes, or `None` when the file does not exist.
fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, String> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("failed to read {}: {e}", path.display())),
    }
}

fn write_by_rename(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let staging = path.with_extension("staging");
    std::fs::write(&staging, bytes)
        .map_err(|e| format!("failed to write {}: {e}", staging.display()))?;
    std::fs::rename(&staging, path)
        .map_err(|e| format!("failed to move {} into place: {e}", path.display()))
}
