//! Pieces of the `lastdbd` binary, split out of `main.rs`.

pub(crate) mod cli;
pub(crate) mod conflict_fold;
pub(crate) mod footprint_proof;
pub(crate) mod serve;
pub(crate) mod session;
pub(crate) mod shutdown;
pub(crate) mod tracing_init;
