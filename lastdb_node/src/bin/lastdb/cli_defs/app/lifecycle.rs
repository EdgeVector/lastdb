//! `lastdb app` subcommand for publishing a signed app declaration.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppLifecycleCommand {
    /// Publish this clean git commit for the running node's exact API version.
    ///
    /// Publish refuses unless the manifest lists every schema the app declares,
    /// typed: `schemas` is present (an empty list when the running node holds
    /// no schema of the app), every field has a type, and every schema the
    /// running node holds for the app is listed by name and identity hash.
    Publish {
        #[arg(long)]
        manifest: PathBuf,
        /// Target registry environment (dev | prod). Default: prod.
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Developer API key (default: $EXEMEM_DEV_API_KEY).
        #[arg(long)]
        api_key: Option<String>,
        /// Registry HTTP API base URL (default: $LASTDB_APP_REGISTRY_URL).
        #[arg(long)]
        registry_url: Option<String>,
        /// Signed-list channel. Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Validate this checkout against the running node and write an unsigned
        /// declaration for signing elsewhere. No key lookup or publication.
        #[arg(long, conflicts_with_all = ["key_file", "api_key", "registry_url"])]
        prepare: Option<PathBuf>,
    },
}
