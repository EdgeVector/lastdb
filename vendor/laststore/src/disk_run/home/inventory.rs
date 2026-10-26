//! Complete snapshot pages for explicit offline conversion and verification.

use super::{HomeMutation, ReadBudget, RunRead, RunReadCosts, MAX_VALUE_BYTES};
use crate::disk_run::{value, Found, MergeIter, MergeSource, RunSequence};
use crate::{Error, Result};
use std::ops::Bound;

/// One bounded page of a held snapshot, including every collection.
pub struct AdministrativePage {
    /// Live values in physical key order. Tombstones remain absent.
    pub records: Vec<HomeMutation>,
    /// Last examined physical key. Continue strictly after it at the same root.
    pub resume: Option<Vec<u8>>,
    /// Every remaining physical key was examined.
    pub exhausted: bool,
}

impl RunRead {
    /// Enumerate a complete held snapshot for an offline operation. This is
    /// not a molecule query. Every collection participates without a registry.
    /// A fresh fork supplies a new finite budget for the next page.
    pub fn administrative_snapshot_page(
        &mut self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<AdministrativePage> {
        if limit == 0 || limit > 10_000 || self.bounded_streams {
            return Err(Error::Config(
                "invalid offline snapshot page request".into(),
            ));
        }
        let base_from = self.runs.len() - self.manifest.base.len();
        let overlaps = |file: &super::manifest::RunFile| {
            after.is_none_or(|lower| file.meta.overlaps(lower, None))
        };
        for slot in 0..base_from {
            if overlaps(&self.runs[slot].0) {
                self.reader(slot)?;
            }
        }
        let base: Vec<_> = self.runs[base_from..]
            .iter()
            .filter(|(file, _)| overlaps(file))
            .map(|(file, _)| file.path.clone())
            .collect();
        self.costs.runs_opened += base.len() as u64;
        let RunRead {
            runs,
            key,
            costs,
            budget,
            ..
        } = self;
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut sources: Vec<_> = runs[..base_from]
            .iter()
            .filter(|(file, _)| overlaps(file))
            .filter_map(|(_, reader)| reader.as_ref())
            .map(|reader| MergeSource::Run(reader.range(lower, Bound::Unbounded)))
            .collect();
        if !base.is_empty() {
            sources.push(MergeSource::Sequence(RunSequence::new(
                base,
                key,
                after.map_or(Bound::Unbounded, |key| Bound::Excluded(key.to_vec())),
                Bound::Unbounded,
            )));
        }
        page(MergeIter::from_sources(sources), costs, *budget, limit)
    }
}

fn page(
    mut input: MergeIter<'_>,
    costs: &mut RunReadCosts,
    budget: ReadBudget,
    limit: usize,
) -> Result<AdministrativePage> {
    let mut output = AdministrativePage {
        records: Vec::new(),
        resume: None,
        exhausted: false,
    };
    while output.records.len() < limit && costs.examined_keys < budget.keys {
        let Some((key, found)) = input.next().transpose()? else {
            output.exhausted = true;
            break;
        };
        costs.examined_keys += 1;
        let body = match found {
            Found::Value(stored) => Some(value::decode(stored, MAX_VALUE_BYTES)?),
            Found::Tombstone => None,
        };
        costs.budget_bytes = costs
            .budget_bytes
            .saturating_add(key.len().saturating_add(body.as_ref().map_or(0, Vec::len)) as u64);
        if costs.budget_bytes > budget.bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        if let Some(body) = body {
            let (collection, storage_key) = split(&key)?;
            output.records.push(HomeMutation {
                collection,
                key: storage_key,
                body: Some(body),
            });
        }
        output.resume = Some(key);
    }
    if !output.exhausted && output.resume.is_none() {
        return Err(Error::ReadBudgetExceeded);
    }
    Ok(output)
}

fn split(bytes: &[u8]) -> Result<(String, String)> {
    let invalid = || Error::Config("invalid collection framing in offline snapshot".into());
    let head: [u8; 2] = bytes
        .get(..2)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let at = 2 + usize::from(u16::from_be_bytes(head));
    let collection = std::str::from_utf8(bytes.get(2..at).ok_or_else(invalid)?)
        .map_err(|_| invalid())?
        .to_owned();
    let key = std::str::from_utf8(bytes.get(at..).ok_or_else(invalid)?)
        .map_err(|_| invalid())?
        .to_owned();
    Ok((collection, key))
}
