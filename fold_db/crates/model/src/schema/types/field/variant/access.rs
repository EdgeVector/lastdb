use super::FieldVariant;

impl FieldVariant {
    /// Drop the in-memory materialized molecule. See prior docs.
    pub fn clear_molecule(&mut self) {
        self.molecule = None;
    }
}
