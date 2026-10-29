//! Random ids for the registry core.
//!
//! Declaration ids, near-miss registration ids, and mutation-gate challenge
//! ids are version-4 UUIDs so a retry cannot collide with a previous mint.
//! Challenge nonces are the same mint: they are HMAC inputs and must stay
//! unpredictable. No existing port supplies entropy. Clock, config, the
//! blocking runtime, the embedder, and persistence do not. This function is
//! the only allowed `Uuid::new_v4` call.

/// Fresh random UUID. Callers format it with `to_string` or `simple`.
#[allow(
    clippy::disallowed_methods,
    reason = "OS entropy for v4 ids; no id port exists, and challenge nonces must stay unpredictable"
)]
pub(crate) fn fresh_uuid_v4() -> uuid::Uuid {
    uuid::Uuid::new_v4()
}
