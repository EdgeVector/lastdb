use super::*;

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
