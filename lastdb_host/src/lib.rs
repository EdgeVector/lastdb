//! Framework-agnostic LastDB owner-socket **host core** — the single home for the
//! wire-shape contract that both socket route executors speak.
//!
//! `lastdbd` (the minimal daemon, `lastdb_node::exec`) and the full desktop node
//! (`fold_db_node::server::uds_exec`) serve the **same** owner-socket wire
//! surface: the [`lastdb_uds::uds_router::DataRoute`] allowlist with identical
//! request/response JSON shapes. Historically each re-implemented the request
//! parsing, pagination math, response envelope, and I4 content-free error
//! mapping against `fold_db` core in lockstep — two copies kept identical by
//! discipline, with drift risk.
//!
//! This crate holds that shared, framework-agnostic layer so the contract is
//! enforced by the compiler instead of by convention:
//!
//! - [`wire`] — request-body / query-string parsing helpers (all pure, all
//!   **I4**: never echo a caller byte into an error).
//! - [`envelope`] — the `{ ok, ...data, user_hash }` success envelope, the
//!   `json_ok` serializer, and the content-free / owner-visible error mapping.
//! - [`reject`] — the typed request-shape rejection contract: one
//!   `{ok:false, kind, error, key?, try?}` body, built only from compile-time
//!   constants, replacing the discriminator-free `400 "Bad Request"`.
//! - [`catalog`] — the internal-index schema allow-list hidden from
//!   native-index search unless `include_internal=true`.
//! - [`pagination`] — the query page constants and `has_more` computation.
//!
//! It depends only on `fold_db` core and `lastdb_uds` (the socket transport),
//! never on either node crate, so both binaries consume it without a cycle.

pub mod catalog;
pub mod envelope;
pub mod handlers;
pub mod host_node;
pub mod pagination;
pub mod qos;
pub mod reject;
pub mod wire;

pub use handlers::HostError;
pub use host_node::{HostNode, ReadBusy, ReadPermit};
pub use qos::{Lane, QosConfig, QosGate, QosPermit, QosSnapshot};
