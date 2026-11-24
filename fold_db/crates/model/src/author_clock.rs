//! Monotonic per-device author clock and signed-log ordering bound.

const NANOS_PER_MILLI: u64 = 1_000_000;

/// Exclusive upper bound on signed-log `ordering_ms` (`2100-01-01T00:00:00Z`),
/// matching `mutation_log::LAMPORT_LIMIT`.
pub const MAX_ORDERING_MS: u64 = 4_102_444_800_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationAuthorClockState {
    pub physical_nanos: u64,
    pub logical_counter: u64,
}

impl MutationAuthorClockState {
    pub fn advance_local_batch(
        &mut self,
        now_nanos: u64,
        count: usize,
    ) -> Option<(std::ops::RangeInclusive<u64>, u64)> {
        let add = u64::try_from(count.checked_sub(1)?).ok()?;
        let counter = self.logical_counter.checked_add(1)?;
        let first = self.physical_nanos.max(now_nanos);
        let last = first.checked_add(add)?;
        self.physical_nanos = last;
        self.logical_counter = counter;
        Some((first..=last, counter))
    }

    /// Reserve `count` times, one millisecond apart, the first in a later
    /// millisecond than every time this clock gave before. Returns the first
    /// time and the batch counter.
    ///
    /// A write that is captured for the signed log takes its order value from
    /// the millisecond of its time. The order values of one device must rise
    /// with every entry, so each captured write gets a millisecond of its own.
    pub fn advance_local_batch_spaced(
        &mut self,
        now_nanos: u64,
        count: usize,
    ) -> Option<(u64, u64)> {
        let add = u64::try_from(count.checked_sub(1)?)
            .ok()?
            .checked_mul(NANOS_PER_MILLI)?;
        let counter = self.logical_counter.checked_add(1)?;
        let after = (self.physical_nanos / NANOS_PER_MILLI)
            .checked_add(1)?
            .checked_mul(NANOS_PER_MILLI)?;
        let first = after.max(now_nanos);
        let last = first.checked_add(add)?;
        if last / NANOS_PER_MILLI >= MAX_ORDERING_MS {
            return None;
        }
        self.physical_nanos = last;
        self.logical_counter = counter;
        Some((first, counter))
    }

    /// Ensure the next spaced local write sits strictly above `prev_order_ms`.
    pub fn floor_above_order(&mut self, prev_order_ms: u64) -> Option<()> {
        if prev_order_ms.checked_add(1)? >= MAX_ORDERING_MS {
            return None;
        }
        let prev_nanos = prev_order_ms.checked_mul(NANOS_PER_MILLI)?;
        self.physical_nanos = self.physical_nanos.max(prev_nanos);
        Some(())
    }

    pub fn observe_remote(&mut self, physical_nanos: u64, logical_counter: u64) {
        self.physical_nanos = self.physical_nanos.max(physical_nanos);
        self.logical_counter = self.logical_counter.max(logical_counter);
    }
}

#[must_use]
pub fn mutation_author_clock_key(device_id: &str) -> String {
    format!(
        "mutation_author_clock:{}",
        crate::keys::canonical::hex::sha256_hex(device_id)
    )
}
