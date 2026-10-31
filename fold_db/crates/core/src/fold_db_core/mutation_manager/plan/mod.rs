//! Write decisions as pure functions: the functional core of the write path.
//!
//! A write runs in three phases:
//!
//! 1. gather: the caller reads every input with the existing batch reads;
//! 2. decide: a function here takes model values and the gathered snapshot
//!    and returns a plan value (accept, the exact refusal, replay, the keys
//!    to load or persist);
//! 3. execute: the unchanged commit path applies that plan.
//!
//! This module names no service type and has no effect: no async, locks,
//! atomics, clock, ids, environment, task-local, or file reads, and no
//! logging. `scripts/ci/plan-purity-lint.sh` enforces the rule in CI.
//! A decision that needs an effect mid-way (the content-addressed atom of a
//! replay check) takes that effect as a closure argument, so the order of
//! effects stays with the caller.

pub(super) mod aggregate;
pub(super) mod cas;
pub(super) mod idempotency;
pub(super) mod key_fields;
pub(super) mod molecules;
