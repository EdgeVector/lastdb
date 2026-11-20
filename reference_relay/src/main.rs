//! In-repo reference relay for CI signed-history proofs.
//!
//! `reference_relay --listen 127.0.0.1:0 --data-dir DIR` prints the bound
//! address, then serves the relay OpenAPI surface. HTTP `nodeSignature` is
//! verified, then access is gated by a per-database grant index built from
//! publisher-signed `birth` / `grant` entries (rebuilt from the stored logs on
//! open). No `AcceptEntry`: record payloads are never decrypted or applied.

mod auth;
mod error;
mod gate;
mod grant_index;
mod grant_load;
mod grant_verify;
mod handlers;
mod presign;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use handlers::{blob_bytes, blob_get, blob_put, blob_upload, log_append, log_read, AppState};
use store::Store;

/// CI blob_perf uploads ~300 MiB; leave headroom above axum's 2 MiB default.
const MAX_BODY_BYTES: usize = 512 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "reference_relay")]
struct Cli {
    /// Bind address (`127.0.0.1:0` lets the OS pick a free port).
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,
    /// Durable store root for logs, blobs, and the presign secret.
    #[arg(long)]
    data_dir: PathBuf,
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
    });
    let app = Router::new()
        .route("/relay/v1/log/append", post(log_append))
        .route("/relay/v1/log/read", post(log_read))
        .route("/relay/v1/blob/put", post(blob_put))
        .route("/relay/v1/blob/get", post(blob_get))
        .route("/relay/v1/blob/upload/{token}", put(blob_upload))
        .route("/relay/v1/blob/bytes/{sha256}", get(blob_bytes))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
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
