//! Host jobs: background and on-demand work over a booted [`Host`].
//!
//! [`Host`]: crate::host::Host

mod paced_rest;
pub mod ttl_sweep;
