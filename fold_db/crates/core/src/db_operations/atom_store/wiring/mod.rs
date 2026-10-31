//! Wiring: `AtomStore` implements the ports and keeps thin wrappers.
//!
//! Converted concept code names only ports (see `ports.rs`). This module is
//! the one place that joins the concepts to the store: each port impl
//! delegates to a concept struct or to an inherent `AtomStore` primitive, and
//! each wrapper keeps the old inherent method for callers that the trial did
//! not convert. A wrapper adds no read, no lock and no allocation: it builds a
//! concept struct of one reference and awaits the same future.

mod delete;
mod filter;
mod schema_keys;
mod store;
