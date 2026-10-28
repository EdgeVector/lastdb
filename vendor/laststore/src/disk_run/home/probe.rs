//! Conditional exact reads share index and data decode without a retained cache.

use super::*;
use crate::disk_run::reader::Selected;

/// Read the primary value, then check the fallback key only when it is absent.
pub struct ReadProbe {
    pub primary: (String, String),
    pub fallback: (String, String),
}

/// A present primary leaves `fallback_present` false.
pub struct ProbedValue {
    pub value: Option<Vec<u8>>,
    pub fallback_present: bool,
}

impl RunRead {
    /// Share block decode for primary reads and conditional absence probes.
    /// Positive fallbacks use a normal batch read to validate stored values.
    /// Negative probes need no second pass. Only primary bodies are retained.
    /// None selects the original two-pass path before any budget is consumed.
    pub fn get_many_or_probe(&mut self, pairs: &[ReadProbe]) -> Result<Option<Vec<ProbedValue>>> {
        let Some(wanted) = self.probe_keys(pairs)? else {
            return Ok(None);
        };
        self.examine(pairs.len() as u64)?;
        for pair in pairs {
            self.charge((pair.primary.0.len() + pair.primary.1.len()) as u64)?;
        }
        let mut found = self.probe_records(&wanted)?;
        let mut results = Vec::with_capacity(pairs.len());
        // Accept every primary before charging any conditional probe, just as
        // the original primary batch followed by the missing-key batch does.
        for slots in found.chunks_exact_mut(2) {
            let value = match slots[0].take() {
                Some(Selected::Record(Found::Value(body))) => Some(self.accept(body, u64::MAX)?),
                Some(Selected::Record(Found::Tombstone)) | None => None,
                _ => return Err(super::super::corrupt("primary probe selector differs")),
            };
            let fallback_present = match slots[1].take() {
                Some(Selected::Presence(present)) => present,
                None => false,
                _ => return Err(super::super::corrupt("fallback probe selector differs")),
            };
            results.push(ProbedValue {
                value,
                fallback_present,
            });
        }
        self.finish_probes(pairs, &mut results)?;
        Ok(Some(results))
    }

    fn finish_probes(&mut self, pairs: &[ReadProbe], results: &mut [ProbedValue]) -> Result<()> {
        let mut positive = Vec::new();
        let mut coordinates = Vec::new();
        for (position, (pair, result)) in pairs.iter().zip(results.iter_mut()).enumerate() {
            if result.value.is_some() {
                result.fallback_present = false;
            } else if result.fallback_present {
                positive.push(position);
                coordinates.push(pair.fallback.clone());
            } else {
                self.examine(1)?;
                self.charge((pair.fallback.0.len() + pair.fallback.1.len()) as u64)?;
            }
        }
        // Preserve value decode errors and the original body-byte charge for
        // orphan bodies. Validation remains one batch, with the same held root.
        for (position, body) in positive.into_iter().zip(self.get_many(&coordinates)?) {
            results[position].fallback_present = body?.is_some();
        }
        Ok(())
    }

    fn probe_keys(&self, pairs: &[ReadProbe]) -> Result<Option<Vec<Vec<u8>>>> {
        let count = pairs.len().saturating_mul(2);
        let bytes = pairs.iter().fold(0u64, |sum, pair| {
            sum.saturating_add(
                (pair.primary.0.len()
                    + pair.primary.1.len()
                    + pair.fallback.0.len()
                    + pair.fallback.1.len()) as u64,
            )
        });
        // Reserve the worst-case key shape. The final charge still covers
        // primary keys plus only the fallbacks whose primary was absent.
        if self.bounded_streams
            || count > MAX_BATCH_KEYS
            || self.costs.examined_keys.saturating_add(count as u64) > self.budget.keys
            || self.costs.budget_bytes.saturating_add(bytes) > self.budget.bytes
        {
            return Ok(None);
        }
        pairs
            .iter()
            .flat_map(|pair| [&pair.primary, &pair.fallback])
            .map(|(collection, key)| run_key(collection, key.as_bytes()))
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }

    fn probe_records(&mut self, wanted: &[Vec<u8>]) -> Result<Vec<Option<Selected>>> {
        let mut found: Vec<Option<Selected>> = wanted.iter().map(|_| None).collect();
        for slot in 0..self.runs.len() {
            let open: Vec<_> = (0..wanted.len())
                .filter(|&at| found[at].is_none())
                .filter(|&at| {
                    at % 2 == 0
                        || !matches!(&found[at - 1], Some(Selected::Record(Found::Value(_))))
                })
                .filter(|&at| self.runs[slot].0.meta.covers(&wanted[at]))
                .collect();
            if open.is_empty() {
                continue;
            }
            let keys: Vec<_> = open.iter().map(|&at| wanted[at].as_slice()).collect();
            let presence: Vec<_> = open.iter().map(|at| at % 2 == 1).collect();
            let parents: Vec<_> = open
                .iter()
                .enumerate()
                .map(|(local, &at)| {
                    local
                        .checked_sub(1)
                        .filter(|&parent| at % 2 == 1 && open[parent] == at - 1)
                })
                .collect();
            let hits = self.reader(slot)?.select_many(&keys, &presence, &parents)?;
            for (at, hit) in open.into_iter().zip(hits) {
                found[at] = hit;
            }
        }
        Ok(found)
    }
}
