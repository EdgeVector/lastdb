//! Clap definitions for the `lastdb` CLI: the top-level `Cli`, every
//! subcommand enum, and the small value-enum argument types.

use super::*;

#[path = "cli_defs/app.rs"]
mod app;
#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/db.rs"]
mod db;
#[path = "cli_defs/schema.rs"]
mod schema;

pub(super) use app::*;
pub(super) use cloud::*;
pub(super) use db::*;
pub(super) use schema::*;

/// CLI spelling of [`laststore::HashGroupKey`].
///
/// Kept local so the storage crate does not grow a clap dependency.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub(super) enum HashGroupKeyArg {
    /// Hash the whole id — today's placement, and what every existing home uses.
    #[default]
    FullKey,
    /// Hash only the partition prefix, giving HashRange partitions physical locality.
    PartitionPrefix,
}

impl From<HashGroupKeyArg> for laststore::HashGroupKey {
    fn from(arg: HashGroupKeyArg) -> Self {
        match arg {
            HashGroupKeyArg::FullKey => Self::FullKey,
            HashGroupKeyArg::PartitionPrefix => Self::PartitionPrefix,
        }
    }
}

impl HashGroupKeyArg {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::FullKey => "full_key",
            Self::PartitionPrefix => "partition_prefix",
        }
    }
}

/// Target envelope policy for `db reseal-at-rest`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(super) enum ResealAtRestTargetArg {
    /// ENB without compression.
    Binary,
    /// ENB with optional deflate compression.
    BinaryCompress,
}

impl ResealAtRestTargetArg {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::BinaryCompress => "binary-compress",
        }
    }
}

/// Optional off-box publication policy for one durable Delete request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(super) enum CloudPublicationArg {
    /// Wait for every required cloud target to confirm the exact mutation frontier.
    Wait,
}

impl CloudPublicationArg {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Wait => "wait",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MutateRequestOptions {
    pub(super) must_exist: bool,
    pub(super) durable: bool,
    pub(super) cloud_publication: Option<CloudPublicationArg>,
}

/// Client headroom above the full exact-mutation server route budget.
pub(super) const MUTATION_CLOUD_PUBLICATION_CLIENT_HEADROOM_SECS: u64 = 20;

pub(super) fn mutation_cloud_publication_client_timeout() -> Duration {
    mutation_cloud_publication_client_timeout_for(lastdb_node::exec::exact_mutation_route_budget())
}

pub(super) fn mutation_cloud_publication_client_timeout_for(route_budget: Duration) -> Duration {
    route_budget.saturating_add(Duration::from_secs(
        MUTATION_CLOUD_PUBLICATION_CLIENT_HEADROOM_SECS,
    ))
}

/// Client-side deadline for admin scans, from the **same** env var the server
/// reads.
///
/// These two deadlines have to move together. Reading the const directly here
/// meant the client gave up at the default while the server was still working
/// under a raised `LASTDB_UDS_ADMIN_TIMEOUT_SECS`, so no amount of raising it
/// could let a long scan finish — the client hung up first and the work looked
/// like a timeout even when it went on to commit.
pub(super) fn admin_scan_client_timeout() -> Duration {
    lastdb_node::exec::admin_handler_timeout()
}

#[derive(Parser, Debug)]
#[command(
    name = "lastdb",
    about = "Tiny LastDB control CLI for LastDB Mini",
    version = env!("FOLDDB_BUILD_VERSION")
)]
pub(super) struct Cli {
    /// Node home directory (default: LASTDB_HOME / FOLDDB_HOME / service-home / ~/.lastdb).
    #[arg(long)]
    pub(super) data_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub(super) command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub(super) enum Command {
    /// Show whether the local daemon socket is reachable.
    ///
    /// Exit 0 when the owner socket answers `/health`. Exit 1 when it does
    /// not; the first human line is then `lastdbd: not reachable — <reason>`.
    /// `--json` adds `"reachable": false` and the reason, and also exits 1.
    /// A reachable-but-degraded node (QoS, sync_failing) still exits 0.
    ///
    /// Human lines by default. Pass `--json` for a machine-readable plane map
    /// (ideal storage roles + bytes) plus live dual-read counters when the
    /// daemon is up. Key counts are intentionally omitted — use
    /// `lastdb db inventory` for scanned key totals.
    ///
    /// Live `/api/status` is the **cheap health** path: process vitals, sync,
    /// backup, QoS, and request-ops *scalars* only. The forensic request-ops
    /// ring (256 recent samples + rankings) is opt-in via `lastdb ops` or
    /// `GET /api/status?recent=1`.
    Status {
        /// Emit machine-readable plane map + dual-read (and live sampler when up).
        #[arg(long)]
        json: bool,
        /// Print the additive gauge contract (unit + window + availability for
        /// every typed operator gauge). Does not change default status lines.
        /// Machine JSON: `GET /api/status` → `status.contract`.
        #[arg(long)]
        contract: bool,
        /// Seconds to wait for the daemon's status read (default: 30, or
        /// `LASTDB_UDS_ADMIN_TIMEOUT_SECS` when that is exported).
        /// Raise when the node is cold — `status_sync` costs tens of seconds
        /// shortly after a restart.
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Show worst request/mutation offenders (client, schema, latency).
    ///
    /// Clients self-identify with the `X-LastDB-Client` header (e.g. `kanban`).
    /// By default this shows the live in-process forensic ring (full
    /// `request_ops.recent` + rankings via `GET /api/status?recent=1`). Plain
    /// `lastdb status` stays on the cheap health path without that ring.
    /// Pass `--since` to include durable sampler rollups, or `--by-app` for a
    /// compact app/verb view.
    ///
    /// The default tables label process-lifetime and recent-ring populations.
    /// `--by-app` keeps the compact key-value contract for external parsers.
    Ops(OpsArgs),
    /// Run one proactive Mini health alert check.
    ///
    /// Two read-only probes, neither of which reads a DB record. `/health`
    /// drives the down path, its consecutive-failure threshold and its alert
    /// cooldown. `/api/status` — the node's own cheap health path — drives a
    /// DEGRADED verdict from signals the node already computes:
    /// `runtime_degraded`, `footprint_over_limit`,
    /// `deferred_lane_refuse_entries`, `durability.degraded` (with the node's
    /// own reasons), a slowest request at or above
    /// `--slowest-request-warn-ms`, and a `governor_state` of `purge-failed`
    /// held for at least `GOVERNOR_PURGE_FAILED_WARN_SECS` (not caller
    /// configurable; see `health_alert::probe_degradation`).
    ///
    /// Liveness alone is NOT health. Measured 2026-10-04 on the primary before
    /// this probe existed: `ok` in 0.08 s while a real read took 23.5 s, with
    /// `durability.degraded=true` on a backup 3.5 days past its 24 h threshold.
    ///
    /// A degradation probe that cannot be completed reports
    /// `reachable; degradation unknown — <why>`, never `ok`. Pass
    /// `--no-degradation-probe` to go back to liveness only.
    AlertCheck(AlertCheckArgs),
    /// Show or change the running daemon's tracing filter, without a restart.
    ///
    /// With no argument, prints the directive in force. With a directive, swaps
    /// it in place — the change takes effect on the next log event and is lost
    /// on restart, which is what makes it safe to raise verbosity on a live
    /// node and walk away.
    ///
    /// The directive is `EnvFilter` syntax, so it can target one module:
    ///
    ///   lastdb log-filter 'fold_db::fold_db_core::query::hash_range_query=debug,info'
    ///
    /// Pass `info` to restore the startup default. A malformed directive is
    /// refused and the active filter is left alone.
    LogFilter {
        /// The `EnvFilter` directive to install. Omit to read the current one.
        directive: Option<String>,
        /// Emit the raw `log_filter` object instead of a human line.
        #[arg(long)]
        json: bool,
    },
    /// Persist or inspect the home directory used by brew/launchd service starts.
    ServiceHome {
        #[command(subcommand)]
        action: ServiceHomeCommand,
    },
    /// Isolate a FRESH node home from macOS Time Machine and FSEvents.
    ///
    /// Spotlight exclusion already happens automatically on every boot and
    /// needs no attention here. Time Machine and FSEvents do not have that:
    /// `tmutil addexclusion` requires root, and FSEvents can only be turned
    /// off per APFS *volume*, not per directory — so isolating a home from
    /// FSEvents means giving it its own volume.
    ///
    /// Without `--execute`, prints the plan only; nothing on disk changes.
    /// With `--execute`, needs root (re-run with sudo) and:
    ///   1. excludes the home from Time Machine (`tmutil addexclusion`);
    ///   2. unless `--skip-volume`, creates a new APFS volume in the same
    ///      container as the reference path, mounts it at
    ///      `/Volumes/<volume-name>`, writes `.fseventsd/no_log` at its
    ///      root, and initializes the node home there instead of `--data-dir`.
    ///
    /// Refuses when the target home already holds a store: this is for a
    /// new install, not a migration of a live one.
    IsolateVolume {
        /// Name for the new APFS volume (ignored with --skip-volume).
        #[arg(long, default_value = "LastDB")]
        volume_name: String,
        /// Only apply the Time Machine exclusion; skip APFS volume creation.
        #[arg(long)]
        skip_volume: bool,
        /// Actually create the volume / set the exclusion. Without this,
        /// only the plan is printed and nothing changes.
        #[arg(long)]
        execute: bool,
        /// Emit machine-readable JSON instead of human lines.
        #[arg(long)]
        json: bool,
    },
    /// Connect cloud sync. Fresh homes with an invite create a new identity
    /// and print its recovery phrase; existing-account joins read the phrase.
    Connect {
        /// Exemem environment to register against (dev | prod).
        #[arg(long)]
        env: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        /// Invite code for first-device onboarding on a fresh data dir.
        #[arg(long)]
        invite_code: Option<String>,
        /// Read the first-device invite code from standard input.
        #[arg(long, conflicts_with = "invite_code")]
        invite_code_stdin: bool,
        /// Replace an existing different identity.key.
        #[arg(long)]
        force: bool,
        /// Register a copied identity only against compiled DEV for a safe-upgrade proof.
        #[arg(
            long,
            requires_all = ["env", "invite_code_stdin"],
            conflicts_with_all = ["api_url", "invite_code", "force"]
        )]
        use_existing_identity: bool,
    },
    /// Restore the latest LastStore cloud backup into a fresh home.
    Restore {
        /// Fresh target home to restore into. Refuses the current primary home.
        #[arg(long)]
        into: PathBuf,
        /// Exemem environment to restore from (dev | prod).
        #[arg(long)]
        env: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Emit secret-free progress JSON Lines on stderr; requires --json.
        #[arg(long, requires = "json")]
        progress_json: bool,
        /// Read verified chunk prefixes from an old home; the target must be fresh.
        /// A failed remote-latest destination can be used for a source-free retry.
        #[arg(long)]
        reuse_chunks_from: Option<PathBuf>,
        /// Recover an S0-only backup from cloud with a recovered identity and cloud config.
        #[arg(long, conflicts_with = "reuse_chunks_from")]
        remote_s0_only: bool,
        /// Restore normal backup/latest from cloud with only identity and cloud config.
        /// Discovery refuses more than 10,000 recovery descriptors.
        #[arg(long, conflicts_with = "remote_s0_only")]
        remote_latest: bool,
        /// Select one cloud database when an account has several backups.
        #[arg(long)]
        db_hash: Option<String>,
        /// Require one exact manifest; normal remote restore still checks latest.
        #[arg(long)]
        manifest_sha256: Option<String>,
    },
    /// Offline-copy a stopped LastStore home into hash-group layout.
    ///
    /// Accepts a legacy `segment_log` source (the original conversion) or an
    /// existing `hash_group` source (a *relayout*, which changes only physical
    /// placement). A relayout is how a populated home adopts
    /// `--hash-group-key partition-prefix`: placement is recorded in the durable
    /// layout descriptor, so it cannot change on a reopen.
    MigrateHashGroup {
        /// Stopped source LastDB home. The source is never modified.
        #[arg(long)]
        from: PathBuf,
        /// Fresh destination LastDB home. Never replaces or promotes automatically.
        #[arg(long)]
        into: PathBuf,
        /// Which part of a document id picks its hash group in the destination.
        ///
        /// `partition-prefix` hashes only through the first NUL, so one
        /// HashRange partition lands in a bounded set of groups and a partition
        /// read resolves them directly instead of sweeping every group.
        #[arg(long, value_enum, default_value_t = HashGroupKeyArg::FullKey)]
        hash_group_key: HashGroupKeyArg,
        /// How many groups one partition may occupy under `partition-prefix`.
        ///
        /// Power of two. Pure locality (1) makes group size follow partition
        /// size, so a large board becomes one oversized group; raising this
        /// trades a few extra group visits per read for a bounded max group.
        #[arg(long, default_value_t = 1)]
        partition_fanout: u32,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Cloud sync: paid signup, upgrade checkout, or subscription status.
    Cloud {
        #[command(subcommand)]
        action: CloudCommand,
    },
    /// Hard-erase one keyed record. Delete means gone (no trash, no undo).
    /// Re-delete of a missing key succeeds. Pass `--must-exist` to refuse a miss.
    ///
    /// Only `delete` is implemented. Create/update are refused until a fields
    /// file exists. Hidden `--type purge` is an alias of `--type delete --must-exist`.
    Mutate {
        /// Schema name (`App/Name` or descriptive).
        #[arg(long)]
        schema: String,
        /// Only `delete` is implemented. Hidden `purge` = `delete --must-exist`.
        #[arg(long = "type", default_value = "delete")]
        mutation_type: String,
        /// Hash half of the key.
        #[arg(long)]
        key_hash: Option<String>,
        /// Range half of the key.
        #[arg(long)]
        key_range: Option<String>,
        /// Delete rows whose range starts with this prefix under --key-hash.
        #[arg(long, conflicts_with = "key_range", requires = "key_hash")]
        key_range_prefix: Option<String>,
        /// Refuse if the target is already gone. Incompatible with --cloud-publication.
        #[arg(long)]
        must_exist: bool,
        /// Wait for the Delete to reach local durable storage before the response.
        #[arg(long)]
        durable: bool,
        /// Wait for exact off-box publication. Requires --durable.
        #[arg(long, value_enum, value_name = "MODE", requires = "durable")]
        cloud_publication: Option<CloudPublicationArg>,
        /// Emit the raw node JSON response.
        #[arg(long)]
        json: bool,
    },
    /// List live record keys for one schema (hash + range). No atom bodies.
    ///
    /// Walks the schema's key-field molecule tips. Tombstones are skipped.
    /// Default page is 100 keys; pass `--cursor` from the previous page to
    /// continue. This is not a census of field atoms and does not require
    /// `X-LastDB-Allow-Full-Scan`. Alias of `get-keys`.
    List {
        /// Schema identity hash, `app/Name`, or descriptive name.
        schema: String,
        /// Restrict the page to range keys under this hash (O(log M)).
        #[arg(long)]
        key_hash: Option<String>,
        /// Max keys to return (default 100, max 1000).
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Exclusive keyset cursor from the previous page's `next_cursor`.
        #[arg(long)]
        cursor: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// One page of live keys for a schema. No atom bodies.
    ///
    /// Prints the schema's hash/range field names in the header so the
    /// columns have meaning. Pass `--key-hash` to list only under one
    /// partition. `list` is an alias. Hydrate a row with `lastdb get`.
    GetKeys {
        /// Schema identity hash, `app/Name`, or descriptive name.
        schema: String,
        /// Restrict the page to range keys under this hash (O(log M)).
        #[arg(long)]
        key_hash: Option<String>,
        /// Max keys to return (default 100, max 1000).
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Exclusive keyset cursor from the previous page's `next_cursor`.
        #[arg(long)]
        cursor: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Point-get one record by exact hash (and range when the schema is HashRange).
    ///
    /// Wraps `POST /api/query` with a HashKey / HashRangeKey filter. Refuses a
    /// body that would scan. HashRange schemas need `--key-range`.
    Get {
        /// Schema identity hash, `app/Name`, or descriptive name.
        schema: String,
        /// Hash half of the key.
        #[arg(long)]
        key_hash: Option<String>,
        /// Range half of the key (required when the schema is HashRange).
        #[arg(long)]
        key_range: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Compact one HashRange key onto the schema's record molecule.
    ///
    /// OWNER socket. Talks to the running daemon (same as `lastdb get`).
    /// Requires `--key-range`. Not `lastdb db compact` (LastStore planes).
    CompactRecord {
        /// Schema identity hash, `app/Name`, or descriptive name.
        schema: String,
        /// Hash half of the key.
        #[arg(long)]
        key_hash: String,
        /// Range half of the key.
        #[arg(long)]
        key_range: String,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Inspect one installed schema's key layout and legal query shapes.
    Schema {
        #[command(subcommand)]
        action: SchemaInspectCommand,
    },
    /// Inspect active target liveness edges and reclaim blockers.
    Liveness {
        #[command(subcommand)]
        action: LivenessCommand,
    },
    /// Database admin.
    ///
    /// Most verbs talk to the running daemon over the owner socket.
    /// `retain-superseded-versions-offline` rewrites tip files while the daemon
    /// is stopped. Stop the daemon, run that verb, then start the same daemon.
    Db {
        #[command(subcommand)]
        action: DbCommand,
    },
    /// Read, set, or clear one installed schema's node-local retention policy.
    SchemaRetention {
        #[command(subcommand)]
        action: SchemaRetentionCommand,
    },
    /// Retire or restore one installed schema's claim on its `descriptive_name`.
    ///
    /// A rekey mints a new identity for the same product and leaves the
    /// predecessor Available under the SAME readable name, so that name
    /// resolves to two schemas and every lookup by it is a 409. Retiring the
    /// predecessor's claim drops it from descriptive-name resolution only. Its
    /// identity hash, its state and its data are untouched, so a reader that
    /// pins the predecessor by hash keeps reading it.
    ///
    /// Name the claimant by its canonical name or identity hash — naming it by
    /// the descriptive name it shares is exactly what is ambiguous.
    SchemaNameClaim {
        #[command(subcommand)]
        action: SchemaNameClaimCommand,
    },
    /// Rebuild Search app inbox by paging product records (off hot path).
    ///
    /// Exclusive offline open of `--data-dir` / LASTDB_HOME (stop `lastdbd` first
    /// for the primary home). Emits IndexChangeBatch JSON under
    /// `apps/search/inbox` for the Search app to drain into LastStore.
    /// After restore/cold home: run this, then `search rebuild` / drain.
    SearchRebuild {
        /// Records per page when walking schemas (default 32).
        #[arg(long, default_value_t = 32)]
        page_size: usize,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// App registry: check schema coverage, register novel schemas, and
    /// publish an app into the LastDB registry (design:
    /// brain `design-lastdb-app-registry`).
    App {
        #[command(subcommand)]
        action: AppCommand,
    },
}

#[derive(Args, Debug)]
pub(super) struct OpsArgs {
    /// Show one compact row per app/client and request verb.
    #[arg(long)]
    pub(super) by_app: bool,
    /// Include durable rollups for the trailing window (examples: 30m, 6h, 2d).
    #[arg(long)]
    pub(super) since: Option<String>,
    /// Seconds to wait for the daemon's status read (default: the shared
    /// admin-scan budget, `LASTDB_UDS_ADMIN_TIMEOUT_SECS` / 600s). This is a
    /// diagnostic for an already-slow node, so it waits rather than giving up.
    #[arg(long)]
    pub(super) timeout: Option<u64>,
}

#[derive(Args, Debug)]
pub(super) struct AlertCheckArgs {
    /// Persisted checker state (default: <home>/monitoring/lastdbd-health-alert-state.json).
    #[arg(long)]
    pub(super) state_file: Option<PathBuf>,
    /// Consecutive failed /health probes required before a down alert.
    #[arg(long, default_value_t = 3)]
    pub(super) failures_before_alert: u64,
    /// Minimum seconds between repeated down alerts during one incident.
    #[arg(long, default_value_t = 900)]
    pub(super) cooldown_secs: u64,
    /// Append notifications to this file instead of using macOS Notification Center.
    #[arg(long, conflicts_with = "no_notify")]
    pub(super) notification_log: Option<PathBuf>,
    /// Disable notification delivery; useful only for dry-run diagnostics.
    #[arg(long)]
    pub(super) no_notify: bool,
    /// Last Stack heartbeat helper path; when set, one routine-heartbeats line is appended.
    #[arg(long)]
    pub(super) heartbeat_command: Option<PathBuf>,
    /// If this file exists, down alerts are suppressed for an acknowledged incident.
    #[arg(long)]
    pub(super) acknowledged_incident_file: Option<PathBuf>,
    /// Routine name to write in heartbeat lines.
    #[arg(long, default_value = "lastdbd-mini-health-alert")]
    pub(super) routine_name: String,
    /// Slowest-request budget in ms for the degradation probe. A slowest
    /// request at or above this is reported as a degradation.
    #[arg(long, default_value_t = 5000)]
    pub(super) slowest_request_warn_ms: u64,
    /// Skip the `/api/status` degradation probe and report liveness only.
    /// Restores the pre-2026-10-04 behaviour, in which a node serving reads in
    /// 23 s answered `ok`.
    #[arg(long)]
    pub(super) no_degradation_probe: bool,
}

#[derive(Subcommand, Debug)]
pub(super) enum ServiceHomeCommand {
    /// Persist the home directory service starts should use.
    Set {
        /// Absolute path, or a path beginning with ~/; must not resolve to ~/.folddb.
        home: PathBuf,
    },
    /// Show the persisted service home, if one is configured.
    Show,
    /// Clear the persisted service home.
    Clear,
}
