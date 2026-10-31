//! A prepared atom keeps its source mutation position through publication.
pub(in crate::fold_db_core::mutation_manager) struct PreparedFieldAtom {
    pub mutation_index: usize,
    pub field_name: String,
    pub atom: crate::atom::Atom,
}
