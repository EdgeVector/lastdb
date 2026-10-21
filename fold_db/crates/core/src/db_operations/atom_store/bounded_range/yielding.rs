//! The continuation of a walk whose reads name where to go on.

use super::keys::successor;
use super::RangeWalk;

impl RangeWalk {
    /// A read returned `rows` rows and dropped `unsealed` rows that had no
    /// at-rest envelope. `resume` is the last key the read examined: the walk
    /// goes on at the least key above it, in the span it read. `None` means the
    /// span ended.
    ///
    /// The read, not the page length, says where to go on. A read that found no
    /// row, because the rows of its page were dropped or because it used its
    /// whole key budget in a run of deleted keys, still moves the walk forward
    /// with no failed read and no cut of the key range.
    pub(crate) fn finish_yield(&mut self, rows: usize, unsealed: u64, resume: Option<&[u8]>) {
        self.stats.calls += 1;
        self.stats.unsealed_rows += unsealed;
        let Some((_, end)) = self.todo.pop() else {
            return;
        };
        if let Some(last) = resume {
            let next = successor(last);
            if next < end {
                self.todo.push((next, end));
            }
            if rows == 0 {
                self.stats.skips += 1;
            } else {
                self.asked = (self.asked * 2).min(self.limit);
            }
        }
    }
}
