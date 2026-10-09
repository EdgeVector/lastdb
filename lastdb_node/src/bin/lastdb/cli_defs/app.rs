//! Clap subcommands for `lastdb app` and `lastdb app index`.

use super::*;

// lint:file-size-ok one clap enum (a single type cannot span files); moved verbatim from cli_defs.rs

#[derive(Subcommand, Debug)]
pub(crate) enum AppIndexCommand {
    /// Sign an index file: writes `<index>.sig` (or `--out`).
    Sign {
        #[arg(long)]
        index: PathBuf,
        /// Ed25519 signing key file (base64 32-byte secret, `dev-init` format).
        /// Default: $LASTDB_REGISTRY_SIGNING_KEY, then <home>/registry-index-signing.key.
        #[arg(long)]
        key_file: Option<PathBuf>,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Verify an index file against its detached signature.
    Verify {
        #[arg(long)]
        index: PathBuf,
        /// Signature file (default: `<index>.sig`).
        #[arg(long)]
        sig: Option<PathBuf>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Print the pinned release verifying key this binary trusts.
    TrustKey,
}

#[derive(Subcommand, Debug)]
pub(crate) enum AppCommand {
    /// Generate the developer Ed25519 signing key used to publish apps.
    DevInit {
        /// Key file path (default: <home>/dev-signing.key).
        #[arg(long)]
        key_file: Option<PathBuf>,
    },
    /// Step 1: check every manifest schema against the target node's catalog.
    ///
    /// By default this is read-only. For each manifest schema it posts
    /// `/api/apps/declare-schema` with intent `check` to the node that
    /// `--data-dir` / LASTDB_HOME names. The node reports what a sync would do
    /// (reuse, compose, register, or expand) and any error a sync would hit,
    /// such as an invalid field mapper. It registers nothing, binds nothing,
    /// and writes no audit event.
    ///
    /// CAUTION: `--sync` is a WRITE. It uses intent `catalog_sync`: the node
    /// can register or expand schemas, bind them, and write schema-sync audit
    /// events. Only `--sync` reports audited, bind-eligible coverage.
    Check {
        /// App manifest (JSON: app_id, metadata, uses[], schemas[]).
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        json: bool,
        /// Write: synchronize the schemas (intent `catalog_sync`) instead of
        /// the read-only check. Can register or expand schemas on the node.
        #[arg(long)]
        sync: bool,
        /// Legacy compatibility option. Mini owns schema synchronization.
        #[arg(long)]
        schema_url: Option<String>,
    },
    /// Compatibility alias for the Mini-owned app schema synchronization path.
    /// This command never calls Schema Service directly.
    RegisterSchemas {
        #[arg(long)]
        manifest: PathBuf,
        /// Legacy compatibility option. Mini owns schema synchronization.
        #[arg(long)]
        env: Option<String>,
        /// Legacy compatibility option. Mini owns schema synchronization.
        #[arg(long)]
        schema_url: Option<String>,
        /// Legacy compatibility option. Mini owns schema synchronization.
        #[arg(long)]
        api_url: Option<String>,
        /// Legacy compatibility option. Mini owns schema synchronization.
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Legacy compatibility option. Mini owns schema synchronization.
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Step 3: reserve the app namespace as a sandbox registry row.
    Publish {
        #[arg(long)]
        manifest: PathBuf,
        /// Target environment (dev | prod). Default: dev.
        #[arg(long)]
        env: Option<String>,
        /// Explicit schema service base URL (overrides --env).
        #[arg(long)]
        schema_url: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Developer API key (default: $EXEMEM_DEV_API_KEY).
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Step 4: promote a sandbox app to live. REJECTS if any manifest schema
    /// is still novel, or if the registry row has neither `source` nor
    /// `artifact` (live shelf must be installable).
    Promote {
        #[arg(long)]
        manifest: PathBuf,
        /// Target environment (dev | prod). Default: dev.
        #[arg(long)]
        env: Option<String>,
        /// Explicit schema service base URL (overrides --env).
        #[arg(long)]
        schema_url: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Developer API key (default: $EXEMEM_DEV_API_KEY).
        #[arg(long)]
        api_key: Option<String>,
    },
    /// List apps on the signed public index (default), or in the live
    /// registry service when `--env` / `--schema-url` is given.
    List {
        /// Legacy: read the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory (default: the public tap,
        /// or $LASTDB_REGISTRY_INDEX).
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Live storage grouped by owning app (`owner_app_id`), read from the
    /// keep-small projection on the local node.
    ///
    /// Cheap: point reads of the write-path meters, not a store walk. Use
    /// this instead of `lastdb db inventory` for the daily "who is using the
    /// space" question. Reserved rows `system` (node-owned schemas) and
    /// `unattributed` (bytes the projection cannot attribute yet) are listed
    /// separately, and `complete` is false whenever `unattributed` is nonzero.
    Storage {
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Run one bounded reconciliation page, then print the storage report.
        #[arg(long)]
        reconcile: bool,
        /// Layouts visited by `--reconcile` (default 32, cap 256).
        #[arg(long)]
        page_size: Option<u64>,
    },
    /// Show one app's row on the signed public index (default), or its live
    /// registry record when `--env` / `--schema-url` is given.
    Info {
        app_id: String,
        /// Legacy: read the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
    },
    /// Resolve the one proved row for an app on this node: the newest compat
    /// row whose `lastdb_version` names the running node build. Anonymous.
    Resolve {
        app_id: String,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Node build string to resolve for (default: the running node, then
        /// this binary).
        #[arg(long)]
        lastdb_version: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Sign, verify, or show a static index file.
    #[command(subcommand)]
    Index(AppIndexCommand),
    /// Install one or more apps by proof: the newest compat row on the signed
    /// index proved with the running node. Legacy service path with
    /// `--env` / `--schema-url` (one app).
    Install {
        /// App ids (`brain kanban situations`).
        #[arg(required = true)]
        app_ids: Vec<String>,
        /// Legacy: install from the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Node build string to resolve for (default: the running node).
        #[arg(long)]
        lastdb_version: Option<String>,
        /// Install directory (default: <home>/apps/<app_id>). One app only.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Legacy: allow installing sandbox records.
        #[arg(long)]
        allow_sandbox: bool,
        /// Replace an existing install directory.
        #[arg(long)]
        force: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Upgrade installed apps to the newest proved row for the running node.
    /// Legacy service path with `--env` / `--schema-url` (one app).
    Upgrade {
        /// App ids (`brain kanban situations`).
        #[arg(required = true)]
        app_ids: Vec<String>,
        /// Legacy: upgrade from the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Node build string to resolve for (default: the running node).
        #[arg(long)]
        lastdb_version: Option<String>,
        /// Install directory (default: <home>/apps/<app_id>). One app only.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Legacy: allow installing/upgrading sandbox records.
        #[arg(long)]
        allow_sandbox: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Publish an immutable release: bind the locked schema identities, the
    /// source commit, and the signed artifact digest into one release id.
    ///
    /// Registers NO schema. Every identity comes from the app lockfile; a
    /// schema with no locked identity fails the release instead.
    ReleasePublish {
        #[arg(long)]
        manifest: PathBuf,
        /// Stable app UUID that anchors the release execution identity.
        #[arg(long)]
        app_uuid: String,
        /// The commit that produced the artifact.
        #[arg(long)]
        source_commit: String,
        /// The artifact file whose bytes the digest covers.
        #[arg(long)]
        artifact: PathBuf,
        /// Where installers fetch the artifact (`https://…` or `file://…`).
        #[arg(long)]
        artifact_url: String,
        /// Target environment (dev | prod). Default: dev.
        #[arg(long)]
        env: Option<String>,
        /// Explicit schema service base URL (overrides --env).
        #[arg(long)]
        schema_url: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Developer API key (default: $EXEMEM_DEV_API_KEY).
        #[arg(long)]
        api_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Point a channel at a release under a generation check. A stale
    /// generation fails with a conflict.
    ReleaseChannel {
        app_id: String,
        /// Channel name (`stable`, `beta`, …).
        #[arg(long)]
        channel: String,
        #[arg(long)]
        release_id: String,
        /// The generation the last channel read returned. Omit to read the
        /// channel first and use what it reports.
        #[arg(long)]
        generation: Option<u64>,
        /// Target environment (dev | prod). Default: dev.
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        schema_url: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        #[arg(long)]
        api_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Revoke a published release.
    ReleaseRevoke {
        app_id: String,
        #[arg(long)]
        release_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        schema_url: Option<String>,
        #[arg(long)]
        api_url: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        #[arg(long)]
        api_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Public install: resolve the channel, verify the release, and activate
    /// it through Host Track. Anonymous — no developer credential.
    ReleaseInstall {
        app_id: String,
        #[arg(long, default_value = "stable")]
        channel: String,
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        schema_url: Option<String>,
        /// Host Track root (default: $LASTDB_HOST_TRACK_ROOT or ~/.host-track).
        #[arg(long)]
        host_track_root: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Print the four release ids and the status of a published app.
    ReleaseStatus {
        app_id: String,
        #[arg(long, default_value = "stable")]
        channel: String,
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        schema_url: Option<String>,
        /// Skip the channel read and prove against a release id you supply.
        #[arg(long)]
        desired: Option<String>,
        #[arg(long)]
        host_track_root: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Run one drift check. On drift, restore the prior verified release
    /// and prove the four-way match again.
    ReleaseCheck {
        app_id: String,
        #[arg(long, default_value = "stable")]
        channel: String,
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        schema_url: Option<String>,
        /// Skip the channel read and check against a release id you supply.
        #[arg(long)]
        desired: Option<String>,
        #[arg(long)]
        host_track_root: Option<PathBuf>,
        #[arg(long)]
        json: bool,
        /// Keep running the check cycle instead of returning after one.
        /// Without this flag the command runs exactly one cycle.
        #[arg(long)]
        watch: bool,
        /// Seconds between cycles under `--watch`. Defaults to the operator
        /// setting, PROBE_INTERVAL (60 s).
        #[arg(long)]
        interval_secs: Option<u64>,
        /// Stop after this many cycles under `--watch`. Without it `--watch`
        /// runs until the process is stopped.
        #[arg(long)]
        cycles: Option<u64>,
    },
    /// Report the status of a DEVELOPMENT session. Prints DEV or UNMANAGED
    /// and never CURRENT.
    DevStatus {
        app_id: String,
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        dev_session_id: String,
        /// The mutable development workspace.
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Write this process's observation as a running release. The host reads
    /// the observation set to decide whether the app is CURRENT.
    ReleaseObserve {
        app_id: String,
        #[arg(long)]
        app_uuid: String,
        #[arg(long)]
        release_id: String,
        #[arg(long)]
        activation_epoch: u64,
        #[arg(long)]
        host_track_root: Option<PathBuf>,
        /// Hold the process open for this many seconds while observed. Used
        /// to prove that an old release process cannot report CURRENT.
        #[arg(long)]
        hold_secs: Option<u64>,
    },
    /// Run an installed source-backed app using its manifest `run` block.
    Run {
        app_id: String,
        /// Install directory (default: <home>/apps/<app_id>).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Extra arguments passed after the manifest-declared args.
        #[arg(last = true)]
        args: Vec<String>,
    },
}
