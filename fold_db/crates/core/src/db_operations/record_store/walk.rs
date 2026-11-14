//! Adaptive range-bisection walks, schema row release, and delete audit ledger scans.

use super::reads::MIN_ROWS;
use super::{invalid, RecordDeleteLedgerEntry, RecordStore};
use crate::schema::types::{Schema, SchemaError};
use crate::storage::format::records::{release_families, ReleaseFamily, ReleaseKind};
use lastdb_model::kind_partition::exact_prefix_bounds;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SchemaRelease {
    pub(crate) records: u64,
    pub(crate) alternates: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RangeWalk {
    todo: Vec<(Vec<u8>, Vec<u8>)>,
    limit: usize,
    asked: usize,
}

impl RangeWalk {
    pub(crate) fn new(start: &[u8], end: &[u8], limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            todo: (start < end)
                .then(|| (start.to_vec(), end.to_vec()))
                .into_iter()
                .collect(),
            limit,
            asked: limit,
        }
    }

    pub(crate) fn set_limit(&mut self, rows: usize) {
        self.limit = rows.max(1);
        self.asked = self.asked.min(self.limit);
    }

    pub(crate) fn done(&self) -> bool {
        self.todo.is_empty()
    }

    pub(crate) fn request(&self) -> Option<(Vec<u8>, Vec<u8>, usize)> {
        self.todo
            .last()
            .map(|(s, e)| (s.clone(), e.clone(), self.asked))
    }

    pub(crate) fn read_too_big(&mut self) -> bool {
        if self.asked > MIN_ROWS {
            self.asked = MIN_ROWS;
            return true;
        }
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

    pub(crate) fn finish_yield(&mut self, rows: usize, resume: Option<&[u8]>) {
        let Some((_, end)) = self.todo.pop() else {
            return;
        };
        if let Some(last) = resume {
            let mut next = last.to_vec();
            next.push(0);
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
    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, SchemaError> {
        let prefix =
            lastdb_model::kind_partition::anchored(crate::storage::format::DELETE_AUDIT_FAMILY, "");
        let mut rows = self
            .reader()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| invalid(format!("delete ledger scan: {e}")))?;
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let take = if limit > 0 { limit } else { rows.len() };
        rows.into_iter()
            .take(take)
            .map(|(k, v)| {
                serde_json::from_slice(&v).map_err(|e| {
                    invalid(format!(
                        "delete ledger row {} is unreadable: {e}",
                        String::from_utf8_lossy(&k)
                    ))
                })
            })
            .collect()
    }

    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let mut out = SchemaRelease::default();
        let families: Vec<ReleaseFamily> = release_families(schema)?;
        for family in families {
            let (start, end) = exact_prefix_bounds(family.prefix());
            let mut walk = RangeWalk::new(
                start.as_bytes(),
                end.as_bytes(),
                crate::storage::format::SCHEMA_RELEASE_ROWS,
            );
            let mut count = 0;
            while !walk.done() {
                let rows = self.walk_step(&mut walk).await?;
                if !rows.is_empty() {
                    let keys = rows.into_iter().map(|(k, _)| k).collect();
                    count += self
                        .in_write_scope(self.format.release_schema_batch(keys, &family))
                        .await?;
                }
            }
            match family.kind() {
                ReleaseKind::Records => out.records += count,
                ReleaseKind::Alternates => out.alternates += count,
                ReleaseKind::AppliedDeletes | ReleaseKind::InlineFiles => {}
            }
        }
        Ok(out)
    }

    pub(crate) async fn walk_step(
        &self,
        walk: &mut RangeWalk,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SchemaError> {
        loop {
            let Some((start, end, asked)) = walk.request() else {
                return Ok(Vec::new());
            };
            match self
                .reader()
                .scan_range_yielding(&start, &end, asked, 0)
                .await
                .map_err(SchemaError::from)
            {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    return Ok(scan.rows);
                }
                Err(SchemaError::ReadResourceLimit { .. }) if walk.read_too_big() => {}
                Err(error) => return Err(error),
            }
        }
    }
}

fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|&b| b >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let (mut sum, mut carry) = (vec![0u32; width + 1], 0u32);
    for at in (0..width).rev() {
        let total = low.get(at).map_or(0, |&b| u32::from(b))
            + high.get(at).map_or(0, |&b| u32::from(b))
            + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let (mut middle, mut rem) = (Vec::with_capacity(width), 0);
    for (at, digit) in sum.into_iter().enumerate() {
        let val = rem * 128 + digit;
        rem = val % 2;
        if at > 0 {
            middle.push(u8::try_from(val / 2).ok()?);
        }
    }
    while middle.len() > 1 && middle.last() == Some(&0) && middle[..middle.len() - 1] > *low {
        middle.pop();
    }
    (middle.as_slice() > low && middle.as_slice() < high).then_some(middle)
}
