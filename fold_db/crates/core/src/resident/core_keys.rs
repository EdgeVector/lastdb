//! A separate byte allowance for typed core memory and write-model metadata.
//!
//! The name stays from the retired complete key membership. A format 7 home
//! keeps no field molecules, so no key membership loads into memory.

mod budget;

pub(crate) use budget::{CoreKeyBudget, CoreKeyReservation};

pub(crate) const DEFAULT_SCHEMA_KEY_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const SCHEMA_KEY_BYTES_ENV: &str = "LASTDB_SCHEMA_KEY_BYTES";

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum CoreKeyError {
    #[error("complete key byte allowance refused the allocation")]
    Capacity,
}
