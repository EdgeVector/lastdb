//! `lastdb app` subcommands for dev init, schema check, publish, promote and run.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppLifecycleCommand {
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
    ///
    /// A schema entry may carry `retention: {ttl_days | ttl_seconds}`. The read-only
    /// check prints the policy it would apply, every change or removal of a
    /// manifest TTL, who shares the records (`shared_with`), and whether the
    /// TTL would delete existing records. `--sync` applies it. The node refuses
    /// a TTL on a catalog schema that another app owns or another schema is
    /// bound to. A TTL deletes records for good: read
    /// docs/app-manifest-retention.md first.
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
        /// Confirm that records which already exist may expire. A manifest
        /// `retention` is refused without it when it would delete existing
        /// records: a new TTL on a Range or HashRange schema that holds data,
        /// or a TTL shorter than the manifest TTL running.
        #[arg(long)]
        confirm_expire_existing: bool,
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
