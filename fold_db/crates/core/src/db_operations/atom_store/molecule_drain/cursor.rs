//! The in-memory cursor of the drain task: the candidate list, the molecule in
//! work, and the molecules it parked.
//!
//! Nothing here is the state of the drain. The state is the candidate rows, the
//! counters, and the rows of the molecule, all in the store. The cursor only
//! saves reads: it remembers where a range walk stopped, so a molecule of any
//! size drains in batches of a fixed size without a read from the start of its
//! range. A restart of the node loses the cursor, and the drain starts again from
//! the candidate list. The deleted keys it passes again cost reads, never writes.

use super::candidate::{list_bounds, Candidate};
use super::family::{records_prefix, row_prefixes, tip_prefix};
use super::judge::Refusal;
use crate::db_operations::atom_store::RangeWalk;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Candidates one read of the list asks for.
const LIST_PAGE: usize = 64;
/// A candidate that waits for kept conflict records is looked at again after this.
pub(super) const PARK_RECORDS: Duration = Duration::from_secs(60);
/// A candidate that a counter or a record keeps from a drain is looked at again
/// after this. The refusal needs an operator; the retry only finds a repair.
pub(super) const PARK_REFUSED: Duration = Duration::from_secs(600);

/// Why a candidate is parked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParkWhy {
    /// `slots` is zero and `records` is above zero.
    Records,
    Refused(Refusal),
}

#[derive(Debug)]
struct Parked {
    until: Instant,
    why: ParkWhy,
}

/// The phase of the molecule in work.
#[derive(Debug)]
pub(super) enum Phase {
    /// Erase the tip records, a page at a time.
    Tips(RangeWalk),
    /// Release the kept conflict records that the slot directory of the molecule
    /// still names, a page at a time.
    Records(RangeWalk),
    /// Delete the rows of the family that carry no count, one prefix at a time.
    Rows { index: usize, walk: RangeWalk },
    /// The final batch.
    Finish,
}

/// The molecule in work.
#[derive(Debug)]
pub(super) struct Active {
    pub(super) candidate: Candidate,
    pub(super) phase: Phase,
    /// Tip keys the current pass of the tip phase has found. A pass that found
    /// none, followed by a gate that still counts `slots`, is a counter fault.
    pub(super) found_tips: u64,
}

impl Active {
    pub(super) fn new(candidate: Candidate, rows: usize) -> Self {
        let phase = Phase::Tips(tip_walk(&candidate, rows));
        Self {
            candidate,
            phase,
            found_tips: 0,
        }
    }
}

/// A walk over the tip records of the molecule.
pub(super) fn tip_walk(candidate: &Candidate, rows: usize) -> RangeWalk {
    prefix_walk(&tip_prefix(candidate), rows)
}

/// A walk over the slot directory of the molecule.
pub(super) fn records_walk(candidate: &Candidate, rows: usize) -> RangeWalk {
    prefix_walk(&records_prefix(candidate), rows)
}

/// A walk over the rows of the `index`th prefix of the family.
pub(super) fn row_walk(candidate: &Candidate, index: usize, rows: usize) -> Option<RangeWalk> {
    row_prefixes(candidate)
        .get(index)
        .map(|prefix| prefix_walk(prefix, rows))
}

fn prefix_walk(prefix: &str, rows: usize) -> RangeWalk {
    let (start, end) = crate::kind_partition::exact_prefix_bounds(prefix);
    RangeWalk::new(start.as_bytes(), end.as_bytes(), rows)
}

/// The cursor of one drain task.
#[derive(Debug)]
pub(crate) struct DrainCursor {
    list: RangeWalk,
    queue: VecDeque<Candidate>,
    pub(super) active: Option<Active>,
    parked: HashMap<Candidate, Parked>,
}

impl Default for DrainCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl DrainCursor {
    pub(crate) fn new() -> Self {
        Self {
            list: new_list_walk(),
            queue: VecDeque::new(),
            active: None,
            parked: HashMap::new(),
        }
    }

    /// Read the candidate list again from its start. The molecule in work keeps
    /// its walk. The candidates read before and not started are read again.
    pub(crate) fn restart(&mut self) {
        self.list = new_list_walk();
        self.queue.clear();
    }

    pub(super) fn list_done(&self) -> bool {
        self.list.done()
    }

    pub(super) fn list_walk(&mut self) -> &mut RangeWalk {
        &mut self.list
    }

    /// Add a candidate that a page of the list named. A candidate that is parked,
    /// in work or already waiting is not added again.
    pub(super) fn offer(&mut self, candidate: Candidate) {
        if let Some(parked) = self.parked.get(&candidate) {
            if parked.until > Instant::now() {
                return;
            }
            self.parked.remove(&candidate);
        }
        let in_work = self
            .active
            .as_ref()
            .is_some_and(|active| active.candidate == candidate);
        if !in_work && !self.queue.contains(&candidate) {
            self.queue.push_back(candidate);
        }
    }

    pub(super) fn pop(&mut self) -> Option<Candidate> {
        self.queue.pop_front()
    }

    /// Put a candidate back after a failed step, behind the others, so one bad
    /// molecule cannot hold back the rest.
    pub(super) fn rotate(&mut self, candidate: Candidate) {
        self.queue.push_back(candidate);
    }

    /// Park a candidate for a while.
    pub(super) fn park(&mut self, candidate: Candidate, why: ParkWhy) {
        let wait = match why {
            ParkWhy::Records => PARK_RECORDS,
            ParkWhy::Refused(_) => PARK_REFUSED,
        };
        self.parked.insert(
            candidate,
            Parked {
                until: Instant::now() + wait,
                why,
            },
        );
    }

    /// Forget a candidate that is finished or closed.
    pub(super) fn unpark(&mut self, candidate: &Candidate) {
        self.parked.remove(candidate);
    }

    /// The numbers for the report: pending, waiting for records, refused. A
    /// parked candidate whose time is over counts as pending again.
    pub(crate) fn gauges(&self) -> (u64, u64, u64) {
        let now = Instant::now();
        let mut waiting = 0u64;
        let mut refused = 0u64;
        for parked in self.parked.values().filter(|parked| parked.until > now) {
            match parked.why {
                ParkWhy::Records => waiting += 1,
                ParkWhy::Refused(_) => refused += 1,
            }
        }
        let pending = self.queue.len() as u64 + u64::from(self.active.is_some());
        (pending, waiting, refused)
    }

    /// The refused candidates now, for the report, sorted so the sample is stable.
    pub(crate) fn refused(&self) -> Vec<(String, String, Refusal)> {
        let now = Instant::now();
        let mut seen = HashSet::new();
        let mut out: Vec<(String, String, Refusal)> = self
            .parked
            .iter()
            .filter(|(_, parked)| parked.until > now)
            .filter_map(|(candidate, parked)| match parked.why {
                ParkWhy::Refused(why) => Some((
                    candidate.scope.clone().unwrap_or_default(),
                    candidate.molecule.clone(),
                    why,
                )),
                ParkWhy::Records => None,
            })
            .filter(|item| seen.insert((item.0.clone(), item.1.clone())))
            .collect();
        out.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        out
    }
}

fn new_list_walk() -> RangeWalk {
    let (start, end) = list_bounds();
    RangeWalk::new(&start, &end, LIST_PAGE)
}
