use super::{SnapshotQuote, MAX_PENDING_BATCHES, MAX_PENDING_BYTES};
use crate::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

struct Entry {
    quote: SnapshotQuote,
    reserved_bytes: u64,
    created: Instant,
    uncertain: bool,
}

#[derive(Default)]
struct State {
    next: u64,
    entries: BTreeMap<u64, Entry>,
    bytes: u64,
}

#[derive(Default)]
pub(super) struct AdmissionLedger(Mutex<State>);

/// Owned, non-cloneable capacity for one complete batch on one writer instance.
/// Dropping an unused ticket returns its quota. A failed append retains the
/// uncertain charge while that poisoned writer exists.
pub struct SnapshotReservation {
    ledger: Arc<AdmissionLedger>,
    id: u64,
    quote: SnapshotQuote,
    created: Instant,
}

/// Finite snapshot work, including reserved batches not yet appended.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotPressure {
    /// Quota held by owned tickets, including an uncertain append.
    pub reserved_bytes: u64,
    /// Number of tickets awaiting a complete snapshot commit.
    pub reserved_batches: usize,
    /// Age of the oldest ticket, measured with a monotonic clock.
    pub oldest_reservation_ms: u64,
    /// Caller should drain at 384 MiB or two seconds. This is not a timer.
    pub should_flush: bool,
}

impl AdmissionLedger {
    fn state(&self) -> Result<MutexGuard<'_, State>> {
        self.0
            .lock()
            .map_err(|_| super::refuse("snapshot admission lock is poisoned"))
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        quote: SnapshotQuote,
        created: Instant,
        previous: Option<&SnapshotReservation>,
    ) -> Result<SnapshotReservation> {
        let mut state = self.state()?;
        let added_bytes = self.additional_bytes(&state, quote, previous)?;
        let oldest = state.entries.values().map(|entry| entry.created).min();
        if oldest.is_some_and(|time| time.elapsed().as_millis() >= 5000) {
            return Err(super::capacity(
                "snapshot age admission limit; flush pending work",
            ));
        }
        let total = state.bytes.checked_add(added_bytes);
        let pending_batches = state.entries.len() - usize::from(previous.is_some());
        if total.map_or(true, |bytes| bytes > MAX_PENDING_BYTES)
            || pending_batches as u64 >= MAX_PENDING_BATCHES
        {
            return Err(super::capacity("snapshot queue admission limit"));
        }
        let id = state
            .next
            .checked_add(1)
            .ok_or_else(|| super::refuse("snapshot reservation identity exhaustion"))?;
        state.next = id;
        state.bytes += added_bytes;
        state.entries.insert(
            id,
            Entry {
                quote,
                reserved_bytes: added_bytes,
                created,
                uncertain: false,
            },
        );
        Ok(SnapshotReservation {
            ledger: Arc::clone(self),
            id,
            quote,
            created,
        })
    }

    fn additional_bytes(
        self: &Arc<Self>,
        state: &State,
        quote: SnapshotQuote,
        previous: Option<&SnapshotReservation>,
    ) -> Result<u64> {
        let Some(previous) = previous else {
            return Ok(quote.physical_reserve);
        };
        let entry = state
            .entries
            .get(&previous.id)
            .ok_or_else(|| super::refuse("snapshot extension prefix is no longer held"))?;
        if !Arc::ptr_eq(self, &previous.ledger)
            || entry.uncertain
            || entry.quote != previous.quote
            || entry.reserved_bytes != previous.quote.physical_reserve
            || quote.operations < previous.quote.operations
            || quote.logical_bytes < previous.quote.logical_bytes
        {
            return Err(super::refuse(
                "snapshot extension prefix or dimensions differ",
            ));
        }
        quote
            .physical_reserve
            .checked_sub(entry.reserved_bytes)
            .ok_or_else(|| super::refuse("snapshot extension shrank its prefix"))
    }

    pub(super) fn pressure(&self) -> Result<SnapshotPressure> {
        let state = self.state()?;
        let oldest = state.entries.values().map(|entry| entry.created).min();
        let age = oldest.map_or(0, |time| {
            u64::try_from(time.elapsed().as_millis()).unwrap_or(u64::MAX)
        });
        Ok(SnapshotPressure {
            reserved_bytes: state.bytes,
            reserved_batches: state.entries.len(),
            oldest_reservation_ms: age,
            should_flush: state.bytes >= 384 * 1024 * 1024 || age >= 2000,
        })
    }
}

impl SnapshotReservation {
    pub(super) fn created(&self) -> Instant {
        self.created
    }
    /// Capacity calculated before this ticket was issued.
    pub fn quote(&self) -> SnapshotQuote {
        self.quote
    }

    pub(super) fn validate(
        &self,
        ledger: &Arc<AdmissionLedger>,
        actual: SnapshotQuote,
    ) -> Result<()> {
        if !Arc::ptr_eq(ledger, &self.ledger)
            || actual.operations > self.quote.operations
            || actual.logical_bytes > self.quote.logical_bytes
            || actual.physical_reserve > self.quote.physical_reserve
        {
            return Err(super::refuse(
                "snapshot reservation owner or dimensions differ",
            ));
        }
        let state = self.ledger.state()?;
        let entry = state
            .entries
            .get(&self.id)
            .ok_or_else(|| super::refuse("snapshot reservation is no longer held"))?;
        if entry.uncertain
            || entry.quote != self.quote
            || entry.reserved_bytes != self.quote.physical_reserve
        {
            return Err(super::refuse("snapshot reservation cannot be reused"));
        }
        Ok(())
    }

    /// Finalize the checked extension without changing its aggregate charge.
    /// Admission retains both tickets until the caller transfers this prefix.
    pub(in crate::disk_pack) fn absorb(&mut self, previous: SnapshotReservation) {
        assert!(Arc::ptr_eq(&self.ledger, &previous.ledger));
        assert_ne!(self.id, previous.id);
        let mut state = self.ledger.0.lock().expect("snapshot admission ownership");
        let prefix = state
            .entries
            .get(&previous.id)
            .expect("owned snapshot prefix");
        let incoming = state
            .entries
            .get(&self.id)
            .expect("owned snapshot extension");
        assert!(!prefix.uncertain && !incoming.uncertain);
        assert_eq!(prefix.quote, previous.quote);
        assert_eq!(incoming.quote, self.quote);
        assert_eq!(prefix.reserved_bytes, prefix.quote.physical_reserve);
        assert_eq!(
            prefix.reserved_bytes.checked_add(incoming.reserved_bytes),
            Some(self.quote.physical_reserve)
        );
        state.entries.remove(&previous.id);
        state
            .entries
            .get_mut(&self.id)
            .expect("owned snapshot extension")
            .reserved_bytes = self.quote.physical_reserve;
        drop(state);
    }

    pub(super) fn begin(&mut self) -> Result<()> {
        let mut state = self.ledger.state()?;
        let entry = state
            .entries
            .get_mut(&self.id)
            .ok_or_else(|| super::refuse("snapshot reservation is no longer held"))?;
        entry.uncertain = true;
        Ok(())
    }

    /// The complete snapshot publication is durable before this runs.
    pub(super) fn complete(&mut self) -> Result<()> {
        let mut state = self.ledger.state()?;
        let entry = state
            .entries
            .remove(&self.id)
            .ok_or_else(|| super::refuse("snapshot reservation is no longer held"))?;
        state.bytes -= entry.reserved_bytes;
        Ok(())
    }
}

impl Drop for SnapshotReservation {
    fn drop(&mut self) {
        let Ok(mut state) = self.ledger.0.lock() else {
            return;
        };
        if state
            .entries
            .get(&self.id)
            .is_some_and(|entry| !entry.uncertain)
        {
            let entry = state
                .entries
                .remove(&self.id)
                .expect("known unused reservation");
            state.bytes -= entry.reserved_bytes;
        }
    }
}
