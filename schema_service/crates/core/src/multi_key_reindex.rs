//! Background membership rebuild for same-product multi-key schemas.
//!
//! Field mappers share **payload** field keys across keyed siblings. Hash
//! membership under a new/repaired key layout must still be built by walking source
//! records and emitting keyed membership rows — async/reindex job class
//! (preference-schema-expand-same-product-different-keys).
//!
//! # Shipped entry point
//!
//! [`KeyedMembershipIndex::reindex`] is the operator Mini / fkanban heal /
//! background workers call: plan membership rows from source records, persist them into
//! a partition-queryable index. Tests and callers query via
//! [`KeyedMembershipIndex::query_partition`] — not by re-planning.
