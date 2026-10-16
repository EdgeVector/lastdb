use super::{JournalQuote, MAX_SUFFIX_BATCHES, MAX_SUFFIX_BYTES};
use crate::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

struct Entry {
    quote: JournalQuote,
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
pub struct JournalReservation {
    ledger: Arc<AdmissionLedger>,
    id: u64,
    quote: JournalQuote,
    created: Instant,
}

/// Finite journal work, including reserved batches not yet appended.
#[derive(Clone, Copy, Debug)]
pub struct JournalPressure {
    /// Quota held by owned tickets, including an uncertain append.
    pub reserved_bytes: u64,
    /// Appended worst-case quota after CURRENT.
    pub durable_suffix_bytes: u64,
    /// Number of tickets not yet transferred to the durable suffix.
    pub reserved_batches: usize,
    /// Age of the oldest ticket, measured with a monotonic clock.
    pub oldest_reservation_ms: u64,
    /// Age of all unmaterialized work, including complete durable batches.
    pub oldest_unpublished_ms: u64,
    /// Caller should drain at 384 MiB or two seconds. This is not a timer.
    pub should_materialize: bool,
}

impl AdmissionLedger {
    fn state(&self) -> Result<MutexGuard<'_, State>> {
        self.0
            .lock()
            .map_err(|_| super::refuse("journal admission lock is poisoned"))
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        quote: JournalQuote,
        durable: u64,
        batches: u64,
        suffix_oldest: Option<Instant>,
    ) -> Result<JournalReservation> {
        let mut state = self.state()?;
        let oldest = state
            .entries
            .values()
            .map(|entry| entry.created)
            .chain(suffix_oldest)
            .min();
        if oldest.is_some_and(|time| time.elapsed().as_millis() >= 5000) {
            return Err(super::refuse(
                "journal age admission limit; materialize pending work",
            ));
        }
        let total = state
            .bytes
            .checked_add(durable)
            .and_then(|bytes| bytes.checked_add(quote.physical_reserve))
            .and_then(|bytes| bytes.checked_add(8192));
        if total.map_or(true, |bytes| bytes > MAX_SUFFIX_BYTES)
            || batches.saturating_add(state.entries.len() as u64) >= MAX_SUFFIX_BATCHES
        {
            return Err(super::refuse("journal suffix admission limit"));
        }
        let id = state
            .next
            .checked_add(1)
            .ok_or_else(|| super::refuse("journal reservation identity exhaustion"))?;
        state.next = id;
        state.bytes += quote.physical_reserve;
        let created = Instant::now();
        state.entries.insert(
            id,
            Entry {
                quote,
                created,
                uncertain: false,
            },
        );
        Ok(JournalReservation {
            ledger: Arc::clone(self),
            id,
            quote,
            created,
        })
    }

    pub(super) fn pressure(
        &self,
        durable: u64,
        suffix_oldest: Option<Instant>,
    ) -> Result<JournalPressure> {
        let state = self.state()?;
        let oldest = state.entries.values().map(|entry| entry.created).min();
        let age = oldest.map_or(0, |time| {
            u64::try_from(time.elapsed().as_millis()).unwrap_or(u64::MAX)
        });
        let aggregate_oldest = oldest.into_iter().chain(suffix_oldest).min();
        let aggregate_age = aggregate_oldest.map_or(0, |time| {
            u64::try_from(time.elapsed().as_millis()).unwrap_or(u64::MAX)
        });
        Ok(JournalPressure {
            reserved_bytes: state.bytes,
            durable_suffix_bytes: durable,
            reserved_batches: state.entries.len(),
            oldest_reservation_ms: age,
            oldest_unpublished_ms: aggregate_age,
            should_materialize: state.bytes.saturating_add(durable) >= 384 * 1024 * 1024
                || aggregate_age >= 2000,
        })
    }
}

impl JournalReservation {
    pub(super) fn created(&self) -> Instant {
        self.created
    }
    /// Capacity calculated before this ticket was issued.
    pub fn quote(&self) -> JournalQuote {
        self.quote
    }

    pub(super) fn validate(
        &self,
        ledger: &Arc<AdmissionLedger>,
        actual: JournalQuote,
    ) -> Result<()> {
        if !Arc::ptr_eq(ledger, &self.ledger)
            || actual.operations > self.quote.operations
            || actual.logical_bytes > self.quote.logical_bytes
            || actual.physical_reserve > self.quote.physical_reserve
        {
            return Err(super::refuse(
                "journal reservation owner or dimensions differ",
            ));
        }
        let state = self.ledger.state()?;
        let entry = state
            .entries
            .get(&self.id)
            .ok_or_else(|| super::refuse("journal reservation is no longer held"))?;
        if entry.uncertain || entry.quote != self.quote {
            return Err(super::refuse("journal reservation cannot be reused"));
        }
        Ok(())
    }

    pub(super) fn begin(&mut self) -> Result<()> {
        let mut state = self.ledger.state()?;
        let entry = state
            .entries
            .get_mut(&self.id)
            .ok_or_else(|| super::refuse("journal reservation is no longer held"))?;
        entry.uncertain = true;
        Ok(())
    }

    /// The writer already owns the durable suffix charge when this runs.
    pub(super) fn complete(&mut self) -> Result<()> {
        let mut state = self.ledger.state()?;
        let entry = state
            .entries
            .remove(&self.id)
            .ok_or_else(|| super::refuse("journal reservation is no longer held"))?;
        state.bytes -= entry.quote.physical_reserve;
        Ok(())
    }
}

impl Drop for JournalReservation {
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
            state.bytes -= entry.quote.physical_reserve;
        }
    }
}
