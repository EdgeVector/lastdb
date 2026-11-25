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
        let (start, end) = exact_prefix_bounds(&prefix);
        let target = if limit > 0 { limit.min(10_000) } else { 10_000 };
        let (mut cursor, mut out) = (start.into_bytes(), Vec::new());
        while out.len() < target && cursor.as_slice() < end.as_bytes() {
            let ask = (target - out.len()).min(crate::storage::format::SCHEMA_RELEASE_ROWS);
            let rows = self
                .reader()
                .scan_range_paged(&cursor, end.as_bytes(), ask)
                .await
                .map_err(|e| invalid(format!("delete ledger scan: {e}")))?;
            if rows.is_empty() {
                break;
            }
            let full = rows.len() == ask;
            let mut next = rows.last().expect("non-empty page").0.clone();
            next.push(0);
            for (k, v) in rows {
                let entry = serde_json::from_slice(&v).map_err(|e| {
                    invalid(format!(
                        "delete ledger row {} is unreadable: {e}",
                        String::from_utf8_lossy(&k)
                    ))
                })?;
                out.push(entry);
            }
            if !full {
                break;
            }
            cursor = next;
        }
        Ok(out)
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
    const SCALARS: u32 = 0x10_f800;
    let (Ok(lo), Ok(hi)) = (std::str::from_utf8(low), std::str::from_utf8(high)) else {
        return None;
    };
    if lo >= hi {
        return None;
    }
    let rank = |c: char| {
        let u = c as u32;
        if u < 0xd800 {
            u
        } else {
            u - 0x800
        }
    };
    let unrank = |r: u32| char::from_u32(if r < 0xd800 { r } else { r + 0x800 });
    let lo_r: Vec<u32> = lo.chars().map(rank).collect();
    let hi_r: Vec<u32> = hi.chars().map(rank).collect();
    if hi_r.starts_with(&lo_r) && hi_r[lo_r.len()..].iter().all(|&r| r == 0) {
        let extra = hi_r.len() - lo_r.len();
        if extra < 2 {
            return None;
        }
        let mut middle = lo.to_owned();
        for _ in 0..extra / 2 {
            middle.push('\0');
        }
        return (middle.as_bytes() > low
            && middle.as_bytes() < high
            && !middle.starts_with("b64:"))
        .then(|| middle.into_bytes());
    }
    let width = lo_r.len().max(hi_r.len()) + 1;
    let (mut sum, mut carry) = (vec![0u32; width + 1], 0u32);
    for at in (0..width).rev() {
        let total = lo_r.get(at).copied().unwrap_or(0) + hi_r.get(at).copied().unwrap_or(0) + carry;
        sum[at + 1] = total % SCALARS;
        carry = total / SCALARS;
    }
    sum[0] = carry;
    let (mut mid_r, mut rem) = (Vec::with_capacity(width), 0u32);
    for (at, digit) in sum.into_iter().enumerate() {
        let val = rem * SCALARS + digit;
        rem = val % 2;
        if at > 0 {
            mid_r.push(val / 2);
        }
    }
    while mid_r.len() > 1 && mid_r.last() == Some(&0) && mid_r[..mid_r.len() - 1] > lo_r[..] {
        mid_r.pop();
    }
    let middle: String = mid_r.into_iter().map(unrank).collect::<Option<_>>()?;
    (middle.as_bytes() > low && middle.as_bytes() < high && !middle.starts_with("b64:"))
        .then(|| middle.into_bytes())
}
