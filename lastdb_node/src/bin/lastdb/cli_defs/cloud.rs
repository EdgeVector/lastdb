//! Clap subcommands for `lastdb cloud` and `lastdb cloud resume-primary`.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum CloudCommand {
    /// Owner-authorized removal of already-reclaimed main atom bodies from
    /// exact prior cloud chunks. Dry-run by default; cloud must remain Off.
    RewriteDeletedAtoms {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Fresh-DB paid path: create identity → Stripe Checkout (sandbox) → register.
    ///
    /// No invite code. Test card: 4242 4242 4242 4242 (any future expiry/CVC).
    SetupPaid {
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
        /// Replace an existing identity.key (orphans local data under old key).
        #[arg(long)]
        force: bool,
    },
    /// Open Stripe Checkout to upgrade an already-connected account to paid.
    Upgrade {
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
    },
    /// Fix failed payment: open Stripe Billing Portal to update card / pay invoice.
    ///
    /// Use this when `lastdb cloud status` shows plan=suspended or access_allowed=false.
    FixBilling {
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
    },
    /// Open the Exemem account page for the connected anonymous account.
    Account {
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
        /// Print JSON payload instead of opening the account URL.
        #[arg(long)]
        json: bool,
        /// Do not open a browser; print the account URL and summary only.
        #[arg(long)]
        no_open: bool,
    },
    /// Print subscription / plan / quota status for the connected account.
    ///
    /// If payment failed, prints loud instructions on how to restore access.
    Status {
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
    },
    /// Live cloud snapshot + clear upload staging (owner socket).
    ///
    /// Talks to the **running** daemon — does **not** stop Mini or require
    /// exclusive offline open. Staging is cleared only after snapshot upload
    /// succeeds.
    HealStaging {
        #[arg(long)]
        json: bool,
    },
    /// Live LastStore backup snapshot: upload missing chunks + manifest and
    /// CAS-flip backup/latest (owner socket).
    Snapshot {
        #[arg(long)]
        json: bool,
    },
    /// Orphan sweep of unreferenced cloud backup chunks (owner socket).
    ///
    /// Default is dry-run (lists orphans only). Pass `--execute` to
    /// presign-DELETE selected objects. Chunks referenced by the local
    /// manifest cache are never selected.
    ///
    /// Returns a durable job ID promptly. Use `--wait` for the terminal
    /// receipt, or `--job ID` to attach after a disconnect. The daemon does
    /// not cancel accepted work when the CLI exits.
    ///
    /// After tip + GC, North Star billable footprint expects cloud db_hash
    /// used ≤ 1.5× local data-dir (or sealed cut + 20%); this route is what
    /// clears unreferenced chunks so that bound can hold.
    BackupGc {
        /// Actually delete orphans (default: dry-run only).
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
        /// Attach to an existing durable job. Does not start another sweep.
        #[arg(long, conflicts_with_all = ["execute", "request_id", "status"])]
        job: Option<String>,
        /// Read the latest durable job, without cloud work.
        #[arg(long, conflicts_with_all = ["execute", "request_id"])]
        status: bool,
        /// Wait for a terminal receipt. Disconnect does not cancel daemon work.
        #[arg(long)]
        wait: bool,
        /// Idempotent acceptance ID (UUID). Reuse after a lost response.
        #[arg(long, conflicts_with = "job")]
        request_id: Option<String>,
    },
    /// Read-only prefix/category size breakdown for the connected cloud
    /// account, across BOTH backing stores (owner socket).
    ///
    /// `cloud status` reports one `used_bytes` total and `backup-gc` reports
    /// the `backup/chunks/` keep-set footprint; neither names what the rest
    /// of the bytes are. This lists the whole scope (same authenticated
    /// path those commands already use — never raw R2/AWS credentials) and
    /// buckets every object by storage category, so a reading can attribute
    /// growth instead of inferring `remainder = total - chunks`.
    ///
    /// The storage Lambda picks the bucket from the prefix it is given, so
    /// the scope is listed once per routing class and the results unioned:
    /// one empty-prefix list reaches B2 only and cannot see `log/`,
    /// `snapshots/`, `thumbs/` or `backup/` at all. The output names every
    /// prefix it listed.
    ///
    /// List-only: no delete, no lifecycle rule. Uses the long admin UDS
    /// deadline because listing a large scope must not
    /// die on the short 10s generic POST timeout.
    PrefixInventory {
        #[arg(long)]
        json: bool,
    },
    /// Read or update sealed-home backup PUT concurrency on the live daemon.
    ///
    /// The setting is process-local and takes effect for subsequent work in
    /// the held cut; no daemon restart or cut rebuild occurs. A valid
    /// `LASTDB_BACKUP_UPLOAD_CONCURRENCY` environment value remains
    /// authoritative and is reported as the effective source.
    BackupConcurrency {
        /// Set the live override (1..=32). Omit with `--clear` absent to read.
        #[arg(long, value_name = "N", conflicts_with = "clear")]
        value: Option<usize>,
        /// Clear the live override and return to env/adaptive policy.
        #[arg(long)]
        clear: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Turn Cloud Sync **off** (intentional pause).
    ///
    /// Stamps the live grace clock and renames `cloud_sync.json` →
    /// `cloud_sync.json.paused` so reboots stay off. Prefer this over hand
    /// editing files. Alias: `pause`.
    #[command(alias = "pause")]
    Off {
        #[arg(long)]
        json: bool,
    },
    /// Turn Cloud Sync **on** after a pause in the same daemon process.
    /// A daemon that booted while Off refuses until peer reconciliation exists.
    #[command(alias = "resume")]
    On {
        #[arg(long)]
        json: bool,
    },
    /// Stage an active cloud config while the durable resume marker keeps
    /// cloud workers Off. A supervised daemon restart is then required.
    PrepareResumePrimary {
        #[arg(long)]
        json: bool,
    },
    /// Check a stopped copy, start the backup job, finish after a fresh restore,
    /// or read its durable receipt.
    ResumePrimary {
        #[command(subcommand)]
        action: CloudResumePrimaryCommand,
    },
    /// Prepare or publish one immutable S0 rescue from a stopped source copy.
    BackupWhileOff {
        #[arg(long)]
        json: bool,
        /// Read a bounded cloud writer summary from a Cloud Sync Off home.
        #[arg(long, conflicts_with = "execute")]
        inspect_writers: bool,
        /// Publish the saved plan through the rescue-only cloud protocol.
        #[arg(long)]
        execute: bool,
        /// Wait for old URLs to expire before upload and pointer commit.
        #[arg(long, requires = "execute")]
        wait: bool,
    },
    /// Clear the active cloud replay pin for one target+seq (owner socket).
    ///
    /// Take `target` and `seq` from `lastdb status` → `replay_blocker` (no
    /// wildcards). Behaviour depends on the pin's code:
    /// - `cloud_replay_corrupt_entry` — deletes that cloud log object + local
    ///   tombstone (safe: the object was unreadable for everyone).
    /// - `cloud_replay_apply_failed` — local skip only: advances this device's
    ///   cursor and writes a tombstone **without** deleting the cloud object
    ///   (peers / later builds can still apply it).
    #[command(alias = "quarantine-replay-blocker")]
    QuarantineReplay {
        /// Sync target label from `replay_blocker.target`.
        #[arg(long)]
        target: String,
        /// Cloud log sequence from `replay_blocker.seq`.
        #[arg(long)]
        seq: u64,
        #[arg(long)]
        json: bool,
    },
    /// Dev: force-seal a local LastStore snapshot and print a backup manifest.
    ///
    /// Local only: no chunk upload, no manifest upload, no latest CAS flip.
    #[command(hide = true)]
    CutManifest {
        /// Prior manifest JSON, used to validate/hash-chain this generation.
        #[arg(long)]
        previous: Option<PathBuf>,
        /// Write the manifest JSON to a file instead of only stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum CloudResumePrimaryCommand {
    /// Read the local writer frontier and every cloud log key from a stopped copy.
    Plan {
        #[arg(long)]
        copy_home: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Accept one durable owner job on a backup-only daemon.
    Start {
        /// Require an empty cloud and build the root from local files only.
        #[arg(long)]
        fresh_from_local: bool,
        /// Accept known gaps in local files. This choice stays in the job receipt.
        #[arg(long, requires = "fresh_from_local")]
        accept_local_damage: bool,
        #[arg(long)]
        json: bool,
    },
    /// Turn Cloud Sync on after a source-free restore of this exact backup.
    Finish {
        #[arg(long)]
        restore_manifest_sha256: String,
        /// Separate home produced by a completed remote-latest restore.
        #[arg(long)]
        restore_home: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Read the last durable job receipt without cloud work.
    Status {
        #[arg(long)]
        json: bool,
    },
}
