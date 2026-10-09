use clap::Parser;
use schema_service_server_http::{SchemaServiceServer, DEFAULT_DEV_SCHEMA_PORT};

/// Schema service dev binary.
///
/// Production deploys schema_service as the schema-infra Lambda (Phase 1).
/// This binary is intended for local development and `./run.sh` workflows.
///
/// **Snapshot hydration (`--hydrate-from`).** Pull a `SnapshotEnvelope`
/// from a remote schema service (e.g. `https://schema.folddb.com`) and
/// import it into the local Sled instance, so the dev binary boots with
/// production-like data. Authentication is via an exemem.com API key
/// (issued at https://www.exemem.com/developer) supplied through the
/// `EXEMEM_API_KEY` environment variable; the key is intentionally not
/// a CLI flag so it never ends up in shell history or `ps`. By default
/// the import only runs when the local registry is "seeds-only" — pass
/// `--rehydrate` to overwrite an existing local registry.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Port for the schema service.
    #[arg(long, default_value_t = DEFAULT_DEV_SCHEMA_PORT)]
    port: u16,

    /// Path to the Last Store home for local schema registry storage.
    #[arg(long, default_value = "schema_registry")]
    db_path: String,

    /// Base URL of a remote schema service to hydrate from
    /// (e.g. `https://schema.folddb.com`). The binary fetches
    /// `<url>/v1/snapshot` and imports the envelope into local Sled.
    /// Requires the `EXEMEM_API_KEY` environment variable.
    #[arg(long)]
    hydrate_from: Option<String>,

    /// Force re-hydration even when the local registry already contains
    /// user-authored artifacts. Without this flag, `--hydrate-from` is
    /// a no-op once the user has added schemas/views/transforms.
    #[arg(long)]
    rehydrate: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Phase 2 observability wiring (B6 follow-up to PR #65). Installs the
    // global tracing subscriber + W3C `TraceContextPropagator` so the
    // `W3CParentContext` middleware's `set_parent` call flows trace ids
    // through the FMT JSON layer + RING buffer. Reads `OBS_FILE_PATH` for
    // the JSON sink (defaults to `~/.folddb/observability.jsonl`).
    //
    // Held for the binary lifetime — dropping it would stop the
    // tracing-appender worker mid-flush. See
    // `docs/observability/init-wiring-notes.md` for the rationale and
    // the deferred Lambda binary path (handled by the cohort B5 sweep).
    let _obs_guard = observability::init_node("schema_service", env!("CARGO_PKG_VERSION"))?;

    let Cli {
        port,
        db_path,
        hydrate_from,
        rehydrate,
    } = Cli::parse();
    let bind_address = format!("127.0.0.1:{port}");

    println!("Schema service starting with local Last Store storage");
    println!("   Database path: {db_path}");

    let server = SchemaServiceServer::new_with_builtins(&db_path, &bind_address).await?;

    if let Some(remote) = hydrate_from.as_deref() {
        let api_key =
            std::env::var("EXEMEM_API_KEY").map_err(|_| -> Box<dyn std::error::Error> {
                "--hydrate-from requires the EXEMEM_API_KEY environment variable. \
             Get a key at https://www.exemem.com/developer (API Keys tab)."
                    .into()
            })?;
        match server.hydrate_from(remote, &api_key, rehydrate).await? {
            Some(report) => {
                println!(
                    "Hydrated from {} (captured_at={}, schemas={}, \
                     canonical_fields={}, embeddings={})",
                    remote,
                    report.captured_at,
                    report.schemas,
                    report.canonical_fields,
                    report.embeddings,
                );
            }
            None => {
                println!(
                    "Skipped hydrate from {remote}: local registry has user-authored \
                     artifacts. Pass --rehydrate to overwrite.",
                );
            }
        }
    }

    println!("Schema service listening on {bind_address}");

    server
        .run()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}
