//! In-repo reference relay for CI signed-history proofs.
//!
//! `reference_relay --listen 127.0.0.1:0 --data-dir DIR` prints the bound
//! address, then serves the relay OpenAPI surface and writes one line per request to stderr. HTTP `nodeSignature` is
//! verified, then access is gated by a per-database grant index built from
//! publisher-signed `birth` / `grant` entries (rebuilt from the stored logs on
//! open). No `AcceptEntry`: record payloads are never decrypted or applied.

mod auth;
mod blob_http;
mod blob_reclaim;
mod blob_store;
mod error;
mod gate;
mod grant_index;
mod grant_load;
mod grant_verify;
mod handlers;
mod heads;
mod presign;
mod refusal;
mod request_log;
mod segment;
mod store;
mod store_read;
mod trim;
mod trim_plan;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::middleware;
use axum::routing::{get, post, put};
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use blob_http::{blob_bytes, blob_upload};
use blob_reclaim::{blob_drop, blob_list};
use handlers::{blob_get, blob_put, log_append, log_read, log_streams, AppState};
use store::Store;
use trim::log_trim;

/// The hosted relay runs on AWS Lambda, which takes at most 6 MB in one request and returns at
/// most 6 MB in one response. That is the protocol's only request and response size
/// (`docs/designs/relay-protocol.md`, "Sizes"), and this relay holds both to it so a node that
/// works here works there. Blob bodies are not buffered and not bound by it; each upload is bound
/// by the size its `blob.put` begin declared.
pub(crate) const PLATFORM_PAYLOAD_MAX_BYTES: usize = 6 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "reference_relay")]
struct Cli {
    /// Bind address (`127.0.0.1:0` lets the OS pick a free port).
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,
    /// Durable store root for logs, blobs, and the presign secret.
    #[arg(long)]
    data_dir: PathBuf,
    /// Refuse a `blob.put` begin for a blob larger than this many bytes, the way the Exemem
    /// relay does (HTTP 400, `invalid_request`, field `bytes`). No limit when absent.
    #[arg(long)]
    max_blob_bytes: Option<u64>,
    /// `ROUTE=FIELD`, for example `log/streams=provider.dynamo.conflict`: answer the first request
    /// of that route with HTTP 429 `rate_limited` and that field, as the Exemem relay does when a
    /// transaction collides (`quota.bytes` for a quota refusal). Later requests are served. May be
    /// repeated. With this option the relay also refuses a nonce it has seen, as Exemem does. Only
    /// the retry proof uses it.
    #[arg(long, value_name = "ROUTE=FIELD")]
    refuse_first: Vec<String>,
    /// Read an upload body at most this many bytes a second (a slow uplink). Only the proof uses it.
    #[arg(long)]
    upload_bytes_per_second: Option<u64>,
    /// After this many bytes of an upload body, stop reading it and hold the connection (a stalled
    /// uplink). Only the proof uses it.
    #[arg(long)]
    upload_stall_after: Option<u64>,
    /// `close-once`: drop the connection of the first large upload after part of its body (the
    /// object store closed it). `400`: answer every large upload with HTTP 400 (BadDigest). Only
    /// the proof uses it.
    #[arg(long, value_parser = ["close-once", "400"])]
    upload_fault: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("reference_relay: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let store = Store::open(cli.data_dir.clone())
        .await
        .map_err(|e| format!("open data-dir: {e:?}"))?;
    let secret = load_or_create_secret(store.root()).await?;
    let listener = TcpListener::bind(cli.listen)
        .await
        .map_err(|e| format!("bind {}: {e}", cli.listen))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("local_addr: {e}"))?;
    println!("{addr}");
    let grants = grant_load::rebuild(store.root()).await;
    let state = Arc::new(AppState {
        store,
        public_base: format!("http://{addr}"),
        secret,
        grants: Mutex::new(grants),
        trim_lock: Mutex::new(()),
        blob_lock: Mutex::new(()),
        max_blob_bytes: cli.max_blob_bytes,
        upload_bytes_per_second: cli.upload_bytes_per_second,
        upload_stall_after: cli.upload_stall_after,
        upload_fault: cli.upload_fault,
        upload_fault_done: std::sync::atomic::AtomicBool::new(false),
    });
    let mut app = Router::new()
        .route("/relay/v1/log/append", post(log_append))
        .route("/relay/v1/log/read", post(log_read))
        .route("/relay/v1/log/streams", post(log_streams))
        .route("/relay/v1/log/trim", post(log_trim))
        .route("/relay/v1/blob/put", post(blob_put))
        .route("/relay/v1/blob/get", post(blob_get))
        .route("/relay/v1/blob/list", post(blob_list))
        .route("/relay/v1/blob/drop", post(blob_drop))
        .route(
            "/relay/v1/blob/upload/{database_id}/{token}",
            put(blob_upload),
        )
        .route(
            "/relay/v1/blob/bytes/{database_id}/{sha256}",
            get(blob_bytes),
        )
        .layer(DefaultBodyLimit::max(PLATFORM_PAYLOAD_MAX_BYTES));
    if let Some(refusals) = refusal::Refusals::parse(&cli.refuse_first)? {
        app = app.layer(middleware::from_fn_with_state(refusals, refusal::refuse));
    }
    let app = app
        .layer(middleware::from_fn(request_log::log_request))
        .with_state(state);
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("serve: {e}"))
}

async fn load_or_create_secret(root: &std::path::Path) -> Result<Vec<u8>, String> {
    let path = root.join("presign.secret");
    if path.exists() {
        let hex = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| format!("read presign.secret: {e}"))?;
        return presign::secret_from_hex(hex.trim()).map_err(|e| format!("presign.secret: {e:?}"));
    }
    let hex = presign::new_secret_hex();
    tokio::fs::write(&path, &hex)
        .await
        .map_err(|e| format!("write presign.secret: {e}"))?;
    presign::secret_from_hex(&hex).map_err(|e| format!("presign.secret: {e:?}"))
}
