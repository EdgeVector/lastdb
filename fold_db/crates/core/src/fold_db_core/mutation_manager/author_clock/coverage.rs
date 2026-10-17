//! A durable high-water reservation covers a bounded native request window.
//!
//! Clock values in live mutations remain their actual allocated values. The
//! stored maximum reserves unused values too. A restart skips that remainder,
//! so an acknowledged allocation cannot recur after a wall-clock rollback.

use super::{AuthorClockPersistBarrier, MutationAuthorClockState};
use std::sync::Mutex;

const PHYSICAL_RESERVATION_NANOS: u64 = 1_000_000_000;
const LOGICAL_RESERVATION_COUNT: u64 = 1_024;

#[derive(Default)]
pub(super) struct ClockCoverage {
    current: Mutex<Option<(MutationAuthorClockState, AuthorClockPersistBarrier)>>,
}

impl ClockCoverage {
    pub(super) fn barrier(
        &self,
        state: MutationAuthorClockState,
        submit: impl FnOnce(MutationAuthorClockState) -> AuthorClockPersistBarrier,
    ) -> AuthorClockPersistBarrier {
        let mut current = self.current.lock().expect("native author-clock coverage");
        if let Some((reserved, barrier)) = &*current {
            if state.physical_nanos <= reserved.physical_nanos
                && state.logical_counter <= reserved.logical_counter
            {
                return barrier.clone();
            }
        }
        let reserved = MutationAuthorClockState {
            physical_nanos: state
                .physical_nanos
                .saturating_add(PHYSICAL_RESERVATION_NANOS),
            logical_counter: state
                .logical_counter
                .saturating_add(LOGICAL_RESERVATION_COUNT),
        };
        // Publish the shared barrier only after the bounded queue owns the
        // reservation. Covered requests must await this same durable result.
        let barrier = submit(reserved);
        *current = Some((reserved, barrier.clone()));
        barrier
    }
}
