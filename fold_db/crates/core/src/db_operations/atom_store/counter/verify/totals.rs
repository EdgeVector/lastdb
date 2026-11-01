//! The totals of a verify run and the watch of writers: memory the run keeps
//! between pages. Nothing here is a counter and nothing is stored on disk.

use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// The most atoms the touched set holds. Beyond it every difference is a candidate.
const MAX_TOUCHED: usize = 1_000_000;

pub(super) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A short key for an atom id: the first 128 bits of its SHA-256.
pub(super) fn digest(atom: &str) -> u128 {
    let hash = Sha256::digest(atom.as_bytes());
    let mut head = [0u8; 16];
    head.copy_from_slice(&hash[..16]);
    u128::from_be_bytes(head)
}

/// Holders per atom over the tips and records pages.
pub(super) struct Tally {
    counts: HashMap<u128, u32>,
    max_atoms: usize,
    /// False once an atom did not fit. Then no total is exact.
    pub complete: bool,
}

impl Tally {
    pub(super) fn new(max_atoms: usize) -> Self {
        Self {
            counts: HashMap::new(),
            max_atoms,
            complete: true,
        }
    }

    pub(super) fn add(&mut self, atom: u128, holders: u32) {
        if let Some(count) = self.counts.get_mut(&atom) {
            *count = count.saturating_add(holders);
        } else if self.counts.len() < self.max_atoms {
            self.counts.insert(atom, holders);
        } else {
            self.complete = false;
        }
    }

    pub(super) fn holders(&self, atom: u128) -> u64 {
        self.counts.get(&atom).map_or(0, |count| u64::from(*count))
    }

    pub(super) fn len(&self) -> usize {
        self.counts.len()
    }
}

/// The atoms that a planner batch changed while a run reads.
///
/// A tip or a record that names an atom can come or go between two pages of a
/// run. The planner tells the watch which atoms it touched, and the verifier
/// takes a difference for such an atom as a candidate, not a fault. The watch
/// is off when no run reads, so a write pays one atomic load.
#[derive(Default)]
pub(crate) struct Watch {
    active: AtomicBool,
    touched: Mutex<(HashSet<u128>, bool)>,
}

impl Watch {
    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Called by the planner inside the write gate, with every atom of a tip
    /// or record change. Call it only when [`Self::is_active`] is true.
    pub(crate) fn note<'a>(&self, atoms: impl IntoIterator<Item = &'a str>) {
        let mut touched = lock(&self.touched);
        for atom in atoms {
            if touched.0.len() >= MAX_TOUCHED {
                touched.1 = true;
                return;
            }
            touched.0.insert(digest(atom));
        }
    }

    pub(super) fn start(&self) {
        *lock(&self.touched) = (HashSet::new(), false);
        self.active.store(true, Ordering::Release);
    }

    pub(super) fn stop(&self) {
        self.active.store(false, Ordering::Release);
        *lock(&self.touched) = (HashSet::new(), false);
    }

    /// True when a writer touched the atom, or when the set overflowed.
    pub(super) fn touched(&self, atom: u128) -> bool {
        let touched = lock(&self.touched);
        touched.1 || touched.0.contains(&atom)
    }

    pub(super) fn count(&self) -> u64 {
        lock(&self.touched).0.len() as u64
    }
}
