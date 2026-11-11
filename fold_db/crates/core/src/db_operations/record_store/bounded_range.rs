//! A range walk that always makes progress under the read budget.

use super::RecordStore;
use crate::schema::SchemaError;

/// The least page the walk asks for after a read hit the limit.
const MIN_ROWS: usize = 16;

/// One bounded piece of a walk.
#[derive(Debug, Default)]
pub(crate) struct Step {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The state of a bounded walk over `start..end`.
#[derive(Debug, Clone)]
pub(crate) struct RangeWalk {
    /// Key spans not read yet. The last one is the lowest.
    todo: Vec<(Vec<u8>, Vec<u8>)>,
    limit: usize,
    asked: usize,
}

impl RangeWalk {
    /// A walk over `start..end` (inclusive start, exclusive end) that asks for
    /// `limit` rows in each read.
    pub(crate) fn new(start: &[u8], end: &[u8], limit: usize) -> Self {
        let limit = limit.max(1);
        let todo = if start < end {
            vec![(start.to_vec(), end.to_vec())]
        } else {
            Vec::new()
        };
        Self {
            todo,
            limit,
            asked: limit,
        }
    }

    /// Ask for up to `rows` rows in each read from now on. A read that hit the
    /// limit keeps its smaller page until reads succeed again.
    pub(crate) fn set_limit(&mut self, rows: usize) {
        self.limit = rows.max(1);
        self.asked = self.asked.min(self.limit);
    }

    /// True when every key of the range was read.
    pub(crate) fn done(&self) -> bool {
        self.todo.is_empty()
    }

    /// The next read to make: start, end and the number of rows to ask for.
    pub(crate) fn request(&self) -> Option<(Vec<u8>, Vec<u8>, usize)> {
        self.todo
            .last()
            .map(|(start, end)| (start.clone(), end.clone(), self.asked))
    }

    /// Cut the lowest span in two and read the lower half first. Returns false
    /// when no key lies between the ends of the span.
    fn cut_top(&mut self) -> bool {
        let Some((start, end)) = self.todo.pop() else {
            return false;
        };
        let Some(middle) = midpoint(&start, &end) else {
            self.todo.push((start, end));
            return false;
        };
        self.todo.push((middle.clone(), end));
        self.todo.push((start, middle));
        true
    }

    /// The read hit the read limit. Ask for fewer rows, or cut the span in two.
    /// Returns false when the span cannot be cut: the read cannot be bounded.
    pub(crate) fn read_too_big(&mut self) -> bool {
        if self.asked > MIN_ROWS {
            self.asked = MIN_ROWS;
            return true;
        }
        self.cut_top()
    }

    /// A read returned `rows` rows. `resume` is the last key the read examined:
    /// the walk goes on at the least key above it, in the span it read. `None`
    /// means the span ended.
    pub(crate) fn finish_yield(&mut self, rows: usize, resume: Option<&[u8]>) {
        let Some((_, end)) = self.todo.pop() else {
            return;
        };
        if let Some(last) = resume {
            let next = successor(last);
            if next < end {
                self.todo.push((next, end));
            }
            if rows > 0 {
                self.asked = (self.asked * 2).min(self.limit);
            }
        }
    }
}

impl RecordStore {
    /// Read the next bounded piece of a walk, outside any captured read scope.
    pub(crate) async fn walk_step(&self, walk: &mut RangeWalk) -> Result<Step, SchemaError> {
        loop {
            let Some((start, end, asked)) = walk.request() else {
                return Ok(Step::default());
            };
            let result = self
                .reader()
                .scan_range_yielding(&start, &end, asked, 0)
                .await;
            match result.map_err(SchemaError::from) {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    return Ok(Step { rows: scan.rows });
                }
                Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                    if !walk.read_too_big() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|byte| *byte >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let digits = |key: &[u8]| -> Vec<u32> {
        (0..width)
            .map(|at| key.get(at).map_or(0, |byte| u32::from(*byte)))
            .collect()
    };
    let (a, b) = (digits(low), digits(high));
    let mut sum = vec![0u32; width + 1];
    let mut carry = 0;
    for at in (0..width).rev() {
        let total = a[at] + b[at] + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let mut middle = Vec::with_capacity(width);
    let mut remainder = 0;
    for (at, digit) in sum.into_iter().enumerate() {
        let value = remainder * 128 + digit;
        remainder = value % 2;
        if at > 0 {
            middle.push(u8::try_from(value / 2).ok()?);
        }
    }
    while middle.len() > 1 && middle.last() == Some(&0) && middle[..middle.len() - 1] > *low {
        middle.pop();
    }
    (middle.as_slice() > low && middle.as_slice() < high).then_some(middle)
}
