//! Host jobs: background and on-demand work over a booted [`Host`].
//!
//! Each job here takes `&Host`, so this module sits above the host in the
//! layer order (`lastdb_node/layers.toml`). The state and status types that
//! these jobs fill (the sampler state, the status snapshot, request telemetry)
//! stay in [`crate::ops`], below the host, because the host holds them.
//!
//! The crate root re-exports `blob_dwell`, `footprint` and `ttl_sweep` so the
//! existing `lastdb_node::` paths stay stable.
//!
//! [`Host`]: crate::host::Host

pub mod blob_dwell;
pub mod footprint;
mod paced_rest;
pub mod self_metrics;
pub mod ttl_sweep;
