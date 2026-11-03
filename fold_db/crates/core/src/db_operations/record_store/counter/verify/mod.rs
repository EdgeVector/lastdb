//! The verifier: reports faults in the retention queue. It never changes one.
//!
//! `verify_refs_step` reads in pages with a cursor. It writes nothing and keeps
//! its result in memory only. It takes no write gate, so it never stalls a
//! writer. The phases, in order:
//!
//! - `queue`: the retention queue of the conflict rows (`time_queue/survey.rs`).
//!   A bucket that holds entries and has no directory row is a fault: the
//!   janitor never finds it.
//!
//! The survey of the retention queue reads its rows again before it reports a
//! bucket.
//!
//! A run that no step touched for ten minutes is dropped.

mod queue;
mod report;
mod run;
mod state;
mod summary;

pub use report::{VerifyReport, VerifyRequest, VerifySummary};
pub(crate) use state::VerifyState;
