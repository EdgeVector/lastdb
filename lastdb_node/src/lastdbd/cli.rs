//! Command line of `lastdbd` and the one-shot subcommands.

use std::io::Write;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lastdb_node::service_home;

#[derive(Parser, Debug)]
#[command(
    name = "lastdbd",
    about = "LastDB Mini semantic daemon: core DB + app-identity + explicit byte backups over the owner Unix socket",
    // Stamped by build.rs (tag -> git describe -> manifest), matching the
    // release gate's --version-equals-tag assertion.
    version = env!("FOLDDB_BUILD_VERSION")
)]
pub(crate) struct Cli {
    /// Node home directory (default: LASTDB_HOME / service-home / ~/.lastdb).
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

    /// Enable bounded change delivery to LastSeek/Search. Off by default.
    /// A new or interrupted Search corpus needs a full rebuild from LastDB.
    #[arg(long)]
    pub(crate) enable_search: bool,

    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

impl Cli {
    /// Preserve requested help/version output and argument-error exit status,
    /// without printing rejected paths or values into captured stderr.
    pub(crate) fn parse_private() -> Self {
        match Self::try_parse() {
            Ok(cli) => cli,
            Err(error) => {
                if matches!(
                    error.kind(),
                    clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
                ) {
                    error.exit();
                }
                let _ = writeln!(std::io::stderr().lock(), "lastdbd: invalid_arguments");
                std::process::exit(2);
            }
        }
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Persist or inspect the home directory used by brew/launchd service starts.
    ServiceHome {
        #[command(subcommand)]
        action: ServiceHomeCommand,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ServiceHomeCommand {
    /// Persist the home directory service starts should use.
    Set {
        /// Absolute path, or a path beginning with ~/; must not resolve to ~/.lastdb.
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
