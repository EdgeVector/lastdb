//! Main-key classification. Always compiled; no cloud-sync gate.
//!
//! It lives here so it builds under default `fold_db` features.

pub mod main_keys;

pub use main_keys::classify_main_key;
