//! The reads of the molecule phases. One page is one captured read scope: the
//! rows of the range and every point read or range read for them see one
//! committed root. Nothing here takes the write gate and nothing here writes.

use super::pages::Request;
use crate::crypto::count_unsealed_discards;
use crate::db_operations::atom_store::{AtomStore, MoleculeBindings};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use std::collections::BTreeSet;

/// A binding counter never asks for more records than this above its own count,
/// so a counter that is far too low still ends the read.
const RECORD_SLACK: usize = 8;
/// The most binding records one counter reads.
const MAX_RECORD_READ: usize = 9_000;

/// One molecule header row.
pub(super) struct HeaderRow {
    pub molecule: String,
    /// The decoded header, or `None` when the row does not decode.
    pub value: Option<serde_json::Value>,
}

/// A page of the headers phase.
pub(super) struct HeadersPage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    pub scan_unsealed: u64,
    pub headers: Vec<HeaderRow>,
}

/// One `bindings` counter row with the binding records read at the same root.
pub(super) struct CounterRow {
    pub molecule_token: String,
    /// The decoded record, or `None` when the row does not decode.
    pub record: Option<MoleculeBindings>,
    pub binding_records: u64,
}

/// A page of the bindings phase.
pub(super) struct BindingsPage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    pub scan_unsealed: u64,
    pub point_unsealed: u64,
    pub counters: Vec<CounterRow>,
}

/// A page of the binding records phase.
pub(super) struct BindingRecordsPage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    pub scan_unsealed: u64,
    pub point_unsealed: u64,
    /// The molecule tokens of the page that have no `bindings` counter.
    pub without_counter: Vec<String>,
}

/// The part of `key` after `marker`, if the key holds it.
fn after<'a>(key: &'a str, marker: &str) -> Option<&'a str> {
    key.find(marker).map(|at| &key[at + marker.len()..])
}

impl AtomStore {
    /// Read one page of molecule headers at one root.
    pub(super) async fn read_headers_page(
        &self,
        (start, end, asked): Request,
    ) -> Result<HeadersPage, SchemaError> {
        self.atom_read_scope(async {
            let (scan, scan_unsealed) = count_unsealed_discards(
                self.raw().inner().scan_range_yielding(&start, &end, asked),
            )
            .await;
            let scan = scan.map_err(SchemaError::from)?;
            let (rows, resume) = (scan.rows, scan.resume);
            let mut headers = Vec::with_capacity(rows.len());
            for (key, value) in &rows {
                let key = String::from_utf8_lossy(key);
                if let Some(molecule) =
                    crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(&key)
                {
                    headers.push(HeaderRow {
                        molecule: molecule.to_string(),
                        value: serde_json::from_slice(value).ok(),
                    });
                }
            }
            Ok(HeadersPage {
                rows: rows.len(),
                resume,
                scan_unsealed,
                headers,
            })
        })
        .await
    }

    /// Read one page of `bindings` counters, and for each counter the binding
    /// records of its molecule, at one root. A bind and an unbind write the
    /// records and the counter in one batch, so the two agree at every root.
    pub(super) async fn read_bindings_page(
        &self,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<BindingsPage, SchemaError> {
        self.atom_read_scope(async {
            let (scan, scan_unsealed) = count_unsealed_discards(
                self.raw().inner().scan_range_yielding(&start, &end, asked),
            )
            .await;
            let scan = scan.map_err(SchemaError::from)?;
            let (rows, resume) = (scan.rows, scan.resume);
            let mut counters = Vec::with_capacity(rows.len());
            let mut point_unsealed = 0;
            for (key, value) in &rows {
                let key = String::from_utf8_lossy(key);
                let Some(token) = after(&key, "mref:v1:n:") else {
                    continue;
                };
                let record: Option<MoleculeBindings> = serde_json::from_slice(value).ok();
                let ask = record
                    .as_ref()
                    .map_or(RECORD_SLACK, |record| {
                        usize::try_from(record.bindings).unwrap_or(MAX_RECORD_READ)
                    })
                    .saturating_add(RECORD_SLACK)
                    .min(MAX_RECORD_READ);
                let prefix = build_storage_key(scope, &format!("mref:v1:e:{token}\0"));
                let upper = FilterUtils::create_prefix_end(&prefix)?;
                let (found, unsealed) = count_unsealed_discards(
                    self.raw()
                        .inner()
                        .scan_range_paged(prefix.as_bytes(), upper.as_bytes(), ask),
                )
                .await;
                point_unsealed += unsealed;
                counters.push(CounterRow {
                    molecule_token: token.to_string(),
                    record,
                    binding_records: found.map_err(SchemaError::from)?.len() as u64,
                });
            }
            Ok(BindingsPage {
                rows: rows.len(),
                resume,
                scan_unsealed,
                point_unsealed,
                counters,
            })
        })
        .await
    }

    /// Read one page of binding records, and the `bindings` counter of each
    /// molecule they name, at one root.
    pub(super) async fn read_binding_records_page(
        &self,
        scope: Option<&str>,
        (start, end, asked): Request,
    ) -> Result<BindingRecordsPage, SchemaError> {
        self.atom_read_scope(async {
            let (scan, scan_unsealed) = count_unsealed_discards(
                self.raw().inner().scan_range_yielding(&start, &end, asked),
            )
            .await;
            let scan = scan.map_err(SchemaError::from)?;
            let (rows, resume) = (scan.rows, scan.resume);
            let tokens: BTreeSet<String> = rows
                .iter()
                .filter_map(|(key, _)| {
                    let key = String::from_utf8_lossy(key);
                    let rest = after(&key, "mref:v1:e:")?;
                    rest.split('\0').next().map(str::to_string)
                })
                .collect();
            let tokens: Vec<String> = tokens.into_iter().collect();
            let keys = tokens
                .iter()
                .map(|token| build_storage_key(scope, &format!("mref:v1:n:{token}")).into_bytes())
                .collect();
            let (counters, point_unsealed) =
                count_unsealed_discards(self.raw().inner().exists_many(keys)).await;
            let without_counter = tokens
                .into_iter()
                .zip(counters.map_err(SchemaError::from)?)
                .filter(|(_, exists)| !exists)
                .map(|(token, _)| token)
                .collect();
            Ok(BindingRecordsPage {
                rows: rows.len(),
                resume,
                scan_unsealed,
                point_unsealed,
                without_counter,
            })
        })
        .await
    }
}
