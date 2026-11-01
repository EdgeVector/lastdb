//! On-disk record formats of the typed stores: values and pure rules.
//!
//! The operations layer writes these records. The storage layer reads them in
//! restore proofs and file dependency checks, so they sit in the model layer
//! below both. Nothing here opens a store or starts a task.

pub(crate) mod atom_history;
pub(crate) mod key_format;
