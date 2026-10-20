//! The read verb of the molecule counters.
//!
//! A molecule keeps three counters in one scope: `slots` and `records` in its
//! header, and `bindings` beside its binding records. This file joins them in
//! one view. A molecule may go only when all three are zero and none is
//! `suspect`. The drain that removes a dead molecule is a later step. This view
//! is its input and its completion test.

use super::counter::{counts_of_header, MoleculeCounts};
use super::molecule_bindings::MoleculeBindings;
use super::AtomStore;
use crate::atom::molecule_key_codec::header_key;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

/// The three counters of one molecule in one scope, for operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeCountersView {
    pub molecule: String,
    /// A header record exists. The molecule lives at this prefix.
    pub header_present: bool,
    /// Tip records of the molecule in this scope.
    pub slots: u64,
    /// Conflict records that name the molecule in this scope.
    pub records: u64,
    /// Binding records (schema field, protein member, database catalog).
    pub bindings: u64,
    /// A counter disagreed with a change, or the header carries no counters.
    pub suspect: bool,
    /// `live`, `droppable`, `suspect` or `absent`.
    pub state: String,
}

impl MoleculeCountersView {
    /// All three counters are zero and none is `suspect`, and a header exists.
    /// The molecule holds nothing and nothing names it.
    #[must_use]
    pub fn droppable(&self) -> bool {
        self.state == "droppable"
    }
}

/// Join a header and a bindings record into one view. Pure.
#[must_use]
pub fn molecule_view(
    molecule: &str,
    header: Option<&Value>,
    bindings: Option<&MoleculeBindings>,
) -> MoleculeCountersView {
    let counts = header.and_then(counts_of_header);
    let unset = header.is_some() && counts.is_none();
    let MoleculeCounts {
        slots,
        records,
        suspect,
    } = counts.unwrap_or_default();
    let (bound, bound_suspect) =
        bindings.map_or((0, false), |record| (record.bindings, record.suspect));
    let suspect = suspect || bound_suspect || unset;
    let state = if suspect {
        "suspect"
    } else if slots + records + bound > 0 {
        "live"
    } else if header.is_some() {
        "droppable"
    } else {
        "absent"
    };
    MoleculeCountersView {
        molecule: molecule.to_string(),
        header_present: header.is_some(),
        slots,
        records,
        bindings: bound,
        suspect,
        state: state.to_string(),
    }
}

impl AtomStore {
    /// The counters of each molecule in `storage_prefix`, in the order given.
    /// Two reads by key, in batches.
    pub async fn molecule_counter_views(
        &self,
        molecules: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<MoleculeCountersView>, SchemaError> {
        self.atom_read_scope(self.molecule_counter_views_captured(molecules, storage_prefix))
            .await
    }

    async fn molecule_counter_views_captured(
        &self,
        molecules: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<MoleculeCountersView>, SchemaError> {
        let keys: Vec<String> = molecules
            .iter()
            .map(|molecule| build_storage_key(storage_prefix, &header_key(molecule)))
            .collect();
        let mut headers: Vec<Option<Value>> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            headers.extend(
                self.main_store
                    .get_items::<Value>(chunk)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read molecule headers: {error}"))
                    })?,
            );
        }
        let bindings = self
            .read_binding_counters(molecules, storage_prefix)
            .await?;
        Ok(molecules
            .iter()
            .zip(headers.iter().zip(bindings.iter()))
            .map(|(molecule, (header, bindings))| {
                molecule_view(molecule, header.as_ref(), bindings.as_ref())
            })
            .collect())
    }
}
