//! `lastdb app` release and dev-status subcommands.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppReleaseCommand {
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
}
