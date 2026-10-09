//! Mini daemon operational telemetry and status surfaces.
//!
//! These modules are grouped here so crash attribution, crash telemetry,
//! self-metrics, health alerts, and session accounting evolve as one
//! operations surface. The crate root re-exports each module to keep existing
//! binary and test call sites stable while new code can use `lastdb_node::ops`.

pub mod atom_ref_backfill;
pub mod crash_attribution;
pub mod crash_telemetry;
pub mod footprint;
pub mod gauge;
pub mod health_alert;
pub mod log_filter;
pub mod request_telemetry;
pub mod self_metrics;
pub mod session_ledger;
pub mod status_gauge_contract;
pub mod status_gauge_gate;
pub mod ttl_sweep;
