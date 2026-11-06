//! Command line of `lastdbd`.

use std::io::Write;
use std::path::PathBuf;

use clap::Parser;

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

    /// Worker socket path. The default is `<home>/data/folddb.sock`.
    #[arg(long)]
    pub(crate) socket_path: Option<PathBuf>,

    /// Enable bounded change delivery to LastSeek/Search. Off by default.
    /// A new or interrupted Search corpus needs a full rebuild from LastDB.
    #[arg(long)]
    pub(crate) enable_search: bool,
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
