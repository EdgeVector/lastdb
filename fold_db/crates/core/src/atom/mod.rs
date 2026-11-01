//! The key codec and value limits live in `lastdb_model::atom`. This module
//! keeps the old `crate::atom` path and adds the behavior the model must not
//! hold: the env-resolved content limit.

pub use lastdb_model::atom::*;

pub mod size_limit;

pub use size_limit::{
    atom_content_byte_len, enforce_atom_content_limit, is_over_default_limit,
    max_atom_content_bytes, observe_atom_content_over_default, parse_max_atom_content_bytes,
    ABSOLUTE_MAX_ATOM_CONTENT_BYTES, ATOM_CONTENT_TOO_LARGE_HINT, DEFAULT_MAX_ATOM_CONTENT_BYTES,
    HEADROOM_ALARM_FRACTION, MAX_ATOM_CONTENT_BYTES_ENV, MIN_MAX_ATOM_CONTENT_BYTES,
};
