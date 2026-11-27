//! Command line of `lastdb upgrade` and `lastdb service`. Verbs and exit codes are
//! contract section 10.

use clap::{Args, Subcommand};
use std::path::PathBuf;

/// Switch this host to a newer LastDB program pair, safely.
///
/// With no verb it installs the newest release this install may take. Exit codes:
/// 0 upgraded or already current, 2 refused before any change, 3 rolled back,
/// 4 needs a human, 5 busy (another upgrade holds the lock).
#[derive(Args, Debug)]
pub(crate) struct UpgradeArgs {
    /// Say what would happen. Change nothing.
    #[arg(long)]
    pub check: bool,
    /// Install this version from the list. It ignores the rollout share.
    #[arg(long = "version", id = "target_version", value_name = "VERSION")]
    pub target_version: Option<String>,
    /// Change the configured channel (stable or nightly), then install its newest release.
    #[arg(long, value_parser = ["stable", "nightly"])]
    pub channel: Option<String>,
    /// Install a local folder holding `lastdb` and `lastdbd`, with the same safe switch.
    #[arg(long, value_name = "DIR")]
    pub pair: Option<PathBuf>,
    /// Go back to the previous program pair and reject the one that ran.
    #[arg(long)]
    pub rollback: bool,
    /// Print the current and previous pair, what is available, the last result and the rejected versions.
    #[arg(long)]
    pub status: bool,
    /// The hourly check: record releases, and roll back a withdrawn build. Applies a new
    /// release only inside the configured window.
    #[arg(long)]
    pub auto: bool,
    /// Remove a version from the rejected list.
    #[arg(long, value_name = "VERSION")]
    pub clear_rejected: Option<String>,
    /// Continue although the cloud backup is behind. The local home stays intact;
    /// the cloud recovery point is smaller.
    #[arg(long)]
    pub accept_backup_behind: bool,
    /// Print one JSON object on stdout.
    #[arg(long)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ServiceCommand {
    /// Install the service for a fresh home: a LaunchAgent (macOS) or a systemd user unit
    /// (Linux) that runs `<home>/current/lastdbd`.
    Install {
        #[arg(long)]
        json: bool,
    },
    /// Take over an existing job once: it runs `<home>/current/lastdbd` afterwards.
    /// This drains, stops the old daemon strictly, starts the new one and verifies it.
    /// Nothing inside the home moves.
    Adopt {
        /// The label of the existing LaunchAgent, or the name of the systemd user unit.
        #[arg(long)]
        label: String,
        /// Continue although the cloud backup is behind.
        #[arg(long)]
        accept_backup_behind: bool,
        #[arg(long)]
        json: bool,
    },
}
