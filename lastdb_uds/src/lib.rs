//! Unix-domain-socket transport for the LastDB Mini daemon (`lastdb_node`).
//!
//! The daemon (`lastdbd`) uses these modules for its owner and app sockets.
//! The former desktop node (`fold_db_node`) was removed in the Mini-only
//! cutover; it no longer re-exports this crate.
//!
//! What lives here is transport + routing only:
//! - [`worker_pool`] — **primary** concurrency control: bounded workers + queue
//!   with backpressure (CPU-shaped; not “max connections”).
//! - [`uds`] — socket bind, `0o600` perms, same-user peer-credential gate,
//!   EMFILE-aware accept classification.
//! - [`uds_http`] — the minimal HTTP/1.1 framing used on the
//!   socket (no actix, no TLS — kernel peer creds are the transport auth).
//! - [`uds_router`] — the control-socket route classifier and dispatcher with
//!   its hardcoded data-route allowlist ([`uds_router::DataRoute`]).
//!
//! The data-route executor lives in `lastdb_node`, which supplies
//! [`uds_router::dispatch`]'s `execute_data` callback. The daemon passes
//! [`uds_router::no_pairing_mint`] for the browser-pairing route.

pub mod uds;
pub mod uds_http;
pub mod uds_router;
pub mod worker_pool;

pub use worker_pool::{SubmitError, UdsPoolSnapshot, UdsWorkerPool};
