//! Slot observations retained from before a read through apply-gate validation.

use std::collections::{HashMap, HashSet};

use crate::db_operations::{ChangedKey, MoleculeData};

pub(in crate::fold_db_core::mutation_manager) type SiblingUpdate =
    (String, MoleculeData, HashSet<ChangedKey>);
pub(in crate::fold_db_core::mutation_manager) type SourceId = (usize, String, String);
pub(in crate::fold_db_core::mutation_manager) type AcceptedSources =
    HashMap<SourceId, crate::atom::AtomEntry>;

pub(in crate::fold_db_core::mutation_manager) struct SiblingOrigin {
    pub source: SourceId,
    pub base: Option<(crate::atom::AtomEntry, Option<crate::atom::KeyMetadata>)>,
    pub replacement: Option<(String, Option<crate::atom::KeyMetadata>)>,
}

#[derive(Clone)]
pub(in crate::fold_db_core::mutation_manager) struct SiblingMove {
    pub source: SourceId,
    pub molecule_uuid: String,
    pub old_key: crate::schema::types::KeyValue,
    pub new_key: crate::schema::types::KeyValue,
    pub expected_old: crate::db_operations::atom_store::PerKeyRecord,
}
pub(in crate::fold_db_core::mutation_manager) type PreparedSiblingUpdates =
    (Vec<SiblingUpdate>, Vec<SiblingOrigin>, Vec<SiblingMove>);
