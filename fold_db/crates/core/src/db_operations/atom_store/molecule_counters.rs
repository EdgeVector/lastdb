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

/// Molecules in one call that get the read of a binding record. Each is a range
/// read, and one call spends a budget of 4,096 metadata pages.
const RECORD_CHECKS: usize = 256;

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
    /// `live`, `droppable`, `suspect`, `mismatch` or `absent`. `mismatch` is a
    /// molecule whose counters say it is dead and that still has a binding
    /// record with no counter: a fault, never a drain.
    pub state: String,
    /// A binding record exists. `None` when the read did not look for one
    /// (only a molecule whose counters are all zero is checked).
    #[serde(default)]
    pub binding_records: Option<bool>,
}

impl MoleculeCountersView {
    /// All three counters are zero, none is `suspect`, a header exists, and no
    /// binding record exists where the read looked for one. The molecule holds
    /// nothing and nothing names it.
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
        binding_records: None,
    }
}

impl MoleculeCountersView {
    /// Record whether a binding record exists. A binding record next to a zero
    /// counter is a fault, so a view that looked `droppable` becomes `mismatch`.
    #[must_use]
    pub fn with_binding_records(mut self, present: bool) -> Self {
        self.binding_records = Some(present);
        if present && self.state == "droppable" {
            self.state = "mismatch".to_string();
        }
        self
    }
}

impl AtomStore {
    /// The counters of each molecule in `storage_prefix`, in the order given.
    /// Two reads by key, in batches. A molecule whose counters are all zero gets
    /// one more read, of at most one binding record, up to [`RECORD_CHECKS`]
    /// molecules in one call.
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
        let mut views: Vec<MoleculeCountersView> = molecules
            .iter()
            .zip(headers.iter().zip(bindings.iter()))
            .map(|(molecule, (header, bindings))| {
                molecule_view(molecule, header.as_ref(), bindings.as_ref())
            })
            .collect();
        for view in views
            .iter_mut()
            .filter(|view| view.droppable())
            .take(RECORD_CHECKS)
        {
            let present = self
                .binding_record_present(&view.molecule, storage_prefix)
                .await?;
            *view = view.clone().with_binding_records(present);
        }
        Ok(views)
    }
}
