//! Stateful route handlers for `dispatch_with_state`, grouped by theme.
//! The dispatcher in `main.rs` matches method and path, then calls one
//! function here.

pub(crate) mod admin;
pub(crate) mod apps;
pub(crate) mod fields;
pub(crate) mod schemas;
