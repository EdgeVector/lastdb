//! A separate byte allowance for typed core memory and write-model metadata.
//!
//! No key membership loads into memory.

mod budget;

pub(crate) use budget::{CoreKeyBudget, CoreKeyReservation};

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum CoreKeyError {
    #[error("complete key byte allowance refused the allocation")]
    Capacity,
}
