//! In-repo grant-blind reference relay for CI signed-history proofs.
//!
//! `reference_relay --listen 127.0.0.1:0 --data-dir DIR` prints the bound
//! address, then serves the relay OpenAPI surface. No grant checks and no
//! `AcceptEntry` — HTTP `nodeSignature` only.

mod auth;
mod error;
mod handlers;
mod presign;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::{get, post, put};
use axum::Router;
use clap::Parser;
use tokio::net::TcpListener;

use handlers::{blob_bytes, blob_get, blob_put, blob_upload, log_append, log_read, AppState};
use store::Store;

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
    let state = Arc::new(AppState {
        store,
        public_base: format!("http://{addr}"),
        secret,
    });
    let app = Router::new()
        .route("/relay/v1/log/append", post(log_append))
        .route("/relay/v1/log/read", post(log_read))
        .route("/relay/v1/blob/put", post(blob_put))
        .route("/relay/v1/blob/get", post(blob_get))
        .route("/relay/v1/blob/upload/{token}", put(blob_upload))
        .route("/relay/v1/blob/bytes/{sha256}", get(blob_bytes))
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
