//! Cross-user sharing and consent-gated delivery support.
//!
//! This module is compiled only with the `sharing` feature. Default local
//! kernel builds omit it; product hosts opt in explicitly.

pub mod blob_cas;
pub mod delivery_wire;
pub mod file_blob_seal;
pub mod query_slice;
pub mod signing;
pub mod store;
pub mod types;
