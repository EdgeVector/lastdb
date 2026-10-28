//! Bounded properties of the real registry APIs. No network or primary home.
//!
//! Explicitly requested by Tom on 2026-10-28. Only this target is enabled.

#[path = "canonical/concurrency.rs"]
mod concurrency;
#[path = "canonical/failure.rs"]
mod failure;
#[path = "canonical_properties/identity.rs"]
mod identity;
#[path = "canonical/schemas.rs"]
mod schemas;
#[path = "canonical/support.rs"]
mod support;
#[path = "canonical/vocabulary.rs"]
mod vocabulary;
