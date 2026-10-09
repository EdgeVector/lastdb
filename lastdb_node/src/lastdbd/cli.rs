//! Command line of `lastdbd` and the one-shot subcommands.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lastdb_node::service_home;

#[derive(Parser, Debug)]
#[command(
    name = "lastdbd",
    about = "LastDB Mini semantic daemon: core DB + app-identity + cloud sync over the owner Unix socket",
    // Stamped by build.rs (tag -> git describe -> manifest), matching the
    // release gate's --version-equals-tag assertion.
    version = env!("FOLDDB_BUILD_VERSION")
)]
pub(crate) struct Cli {
    /// Node home directory (default: LASTDB_HOME / FOLDDB_HOME / ~/.lastdb;
    /// an existing ~/.folddb is honored in place).
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Worker socket path. The proxy owns the public socket when this option
    /// points at a private path. The default is `<home>/data/folddb.sock`.
    #[arg(long)]
    pub(crate) socket_path: Option<PathBuf>,

    /// Full-surface worker socket path. The default is
    /// `<home>/data/folddb-full.sock`.
    #[arg(long)]
    pub(crate) full_socket_path: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Persist or inspect the home directory used by brew/launchd service starts.
    ServiceHome {
        #[command(subcommand)]
        action: ServiceHomeCommand,
    },
    /// Connect cloud sync. On a fresh home with --invite-code, creates a new
    /// identity and prints its recovery phrase. Otherwise reads the 24-word
    /// phrase from stdin to join an existing account as another device. Run
    /// while the daemon is stopped; the next boot pulls the account's data.
    Connect {
        /// Exemem environment to register against (dev | prod). Defaults to
        /// the profile's environment resolution (EXEMEM_ENV / build profile).
        #[arg(long)]
        env: Option<String>,
        /// Explicit Exemem API URL (overrides --env).
        #[arg(long)]
        api_url: Option<String>,
        /// Invite code for first-device onboarding on a fresh data dir.
        #[arg(long)]
        invite_code: Option<String>,
        /// Replace an existing DIFFERENT identity.key (orphans data written
        /// under the old key — the old data dir will no longer decrypt).
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ServiceHomeCommand {
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

/// Run `lastdbd service-home ...` and return the line to print.
pub(crate) fn run_service_home(action: &ServiceHomeCommand) -> Result<String, String> {
    match action {
        ServiceHomeCommand::Set { home } => service_home::set_configured_home(home),
        ServiceHomeCommand::Show => service_home::show_configured_home(),
        ServiceHomeCommand::Clear => service_home::clear_configured_home(),
    }
}

/// Resolve the Exemem API URL for `lastdbd connect`.
pub(crate) fn connect_api_url(
    api_url: Option<String>,
    env: Option<&str>,
) -> Result<String, String> {
    use folddb_profile::endpoints::{exemem_api_url, exemem_api_url_for, Environment};

    match (api_url, env) {
        (Some(url), _) => Ok(url),
        (None, Some("dev")) => Ok(exemem_api_url_for(Environment::Dev).to_string()),
        (None, Some("prod")) => Ok(exemem_api_url_for(Environment::Prod).to_string()),
        (None, Some(other)) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
        (None, None) => Ok(exemem_api_url()),
    }
}
