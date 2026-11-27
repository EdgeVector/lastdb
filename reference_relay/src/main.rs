//! In-repo reference relay for CI signed-history proofs.
//!
//! `reference_relay --listen 127.0.0.1:0 --data-dir DIR` prints the bound
//! address, then serves every `relay_protocol::Route` and writes one line per request to stderr. HTTP `nodeSignature` is
//! verified, then access is gated by a per-database grant index built from
//! publisher-signed `birth` / `grant` entries (rebuilt from the stored logs on
//! open). No `AcceptEntry`: record payloads are never decrypted or applied.

mod auth;
mod blob_api;
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
mod segment_api;
mod store;
mod store_read;
mod trim;
mod trim_plan;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::HeaderValue;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::Router;
use clap::Parser;
use relay_protocol::Route;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use blob_api::{blob_get, blob_put};
use blob_http::{blob_bytes, blob_upload};
use blob_reclaim::{blob_drop, blob_list};
use handlers::{log_append, log_read, log_streams, AppState};
use presign::{BLOB_BYTES_PATH, BLOB_UPLOAD_PATH};
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
    /// The revision this relay names in `X-LastDB-Relay-Revision`: `1.<n>` at most the revision
    /// of this build, to act as a relay of that older revision, or `none` to name none, as a relay
    /// from before revisions. The default is the revision of this build. Only the proof uses it.
    #[arg(long)]
    announce_revision: Option<String>,
}

/// The value of `X-LastDB-Relay-Revision` this relay sends, `None` to send none.
fn announced_revision(option: Option<&str>) -> Result<Option<HeaderValue>, String> {
    let value = match option {
        None => relay_protocol::revision(),
        Some("none") => return Ok(None),
        Some(value) => match relay_protocol::parse_revision(value) {
            Some(revision) if revision <= relay_protocol::REVISION => value.trim().to_owned(),
            _ => {
                return Err(format!(
                    "--announce-revision {value:?}: expected none or {}.<n> up to {}",
                    relay_protocol::PROTOCOL,
                    relay_protocol::revision()
                ))
            }
        },
    };
    HeaderValue::from_str(&value)
        .map(Some)
        .map_err(|e| format!("--announce-revision: {e}"))
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
    let announced = announced_revision(cli.announce_revision.as_deref())?;
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
        .route(
            Route::LogAppend.path(),
            post(log_append).layer(middleware::from_fn(run_detached)),
        )
        .route(Route::LogRead.path(), post(log_read))
        .route(Route::LogStreams.path(), post(log_streams))
        .route(
            Route::LogTrim.path(),
            post(log_trim).layer(middleware::from_fn(run_detached)),
        )
        .route(
            Route::BlobPut.path(),
            post(blob_put).layer(middleware::from_fn(run_detached)),
        )
        .route(Route::BlobGet.path(), post(blob_get))
        .route(Route::BlobList.path(), post(blob_list))
        .route(
            Route::BlobDrop.path(),
            post(blob_drop).layer(middleware::from_fn(run_detached)),
        )
        // The object-store stand-in behind the presigned URLs; not protocol routes.
        .route(
            &format!("{BLOB_UPLOAD_PATH}/{{database_id}}/{{token}}"),
            put(blob_upload),
        )
        .route(
            &format!("{BLOB_BYTES_PATH}/{{database_id}}/{{sha256}}"),
            get(blob_bytes),
        )
        .layer(DefaultBodyLimit::max(PLATFORM_PAYLOAD_MAX_BYTES));
    if let Some(refusals) = refusal::Refusals::parse(&cli.refuse_first)? {
        app = app.layer(middleware::from_fn_with_state(refusals, refusal::refuse));
    }
    let app = app
        .layer(middleware::from_fn_with_state(announced, announce_revision))
        .layer(middleware::from_fn(request_log::log_request))
        .with_state(state);
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("serve: {e}"))
}

/// A route that changes the store runs to its end even when the caller goes away. The server
/// drops the future of a request whose connection closed, and such a handler can stop between two
/// of its steps: a `log.append` of a segment that wrote the item and did not yet move the head
/// left an item above the head, and every later append of that stream was a conflict
/// (`segment.first_seq`) until the relay restarted. A node killed in the middle of a call closes
/// its connection that way. The spawned task finishes the handler, and its answer is dropped
/// when nobody waits for it any more.
async fn run_detached(request: Request, next: Next) -> Response {
    tokio::spawn(next.run(request))
        .await
        .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Every answer of a protocol route, a refusal included, names the contract revision this relay
/// serves (`relay_protocol::answer_headers::REVISION`), or none when `--announce-revision none`.
/// The object-store stand-in routes are not protocol routes and do not.
async fn announce_revision(
    State(announced): State<Option<HeaderValue>>,
    request: Request,
    next: Next,
) -> Response {
    let protocol = Route::from_path(request.uri().path()).is_some();
    let mut response = next.run(request).await;
    if let Some(value) = announced.filter(|_| protocol) {
        response
            .headers_mut()
            .insert(relay_protocol::answer_headers::REVISION, value);
    }
    response
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
