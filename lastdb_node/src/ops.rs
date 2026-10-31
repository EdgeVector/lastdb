//! Mini daemon operational telemetry and status surfaces.
//!
//! These modules are grouped here so crash attribution, crash telemetry,
//! self-metrics status, health alerts, and session accounting evolve as one
//! operations surface. The crate root re-exports each module to keep existing
//! binary call sites stable while new code can use `lastdb_node::ops`.
//!
//! The host holds this state, so `ops` sits below the host. Work that takes
//! `&Host` (the sampler, TTL sweep, blob dwell, footprint relief) lives in
//! [`crate::jobs`].

pub mod crash_attribution;
pub mod crash_telemetry;
pub mod gauge;
pub mod health_alert;
pub mod log_filter;
pub mod request_telemetry;
pub mod self_metrics;
pub mod session_ledger;
pub mod status_gauge_contract;
