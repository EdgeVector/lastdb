//! The continuation of a walk whose reads name where to go on.

use super::keys::successor;
use super::RangeWalk;

impl RangeWalk {
    /// A read returned `rows` rows. `resume` is the last key the read examined:
    /// the walk goes on at the least key above it, in the span it read. `None`
    /// means the span ended.
    ///
    /// The read, not the page length, says where to go on. A read that found no
    /// row, because it used its key budget in a run of deleted keys, still moves
    /// the walk forward with no failed read and no cut of the key range.
    pub(crate) fn finish_yield(&mut self, rows: usize, resume: Option<&[u8]>) {
        self.stats.calls += 1;
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
