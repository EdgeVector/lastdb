use super::{atoms, budget, files, map, read_budget, rows::Rows, Proof, StorageResult};

struct Source<'a, 'b>(&'a Proof<'b>);

impl files::MetadataRead for Source<'_, '_> {
    fn get(&mut self, collection: &str, key: &str) -> StorageResult<Option<Vec<u8>>> {
        let mut read = super::fork_mapped(
            self.0.main,
            read_budget(budget::ATOM_INVENTORY_READ_BYTES, 1),
        )?;
        read.get_bounded(collection, key, budget::ATOM_BODY_MAX_BYTES)
            .map_err(map)
    }

    fn open_atom(&mut self, stored: &[u8]) -> StorageResult<crate::atom::Atom> {
        atoms::open(stored)
    }

    fn directories(
        &mut self,
        scope: Option<&str>,
        atom_id: &str,
        dependencies: &[files::FileDependency],
    ) -> StorageResult<()> {
        atoms::directories(self.0, scope, atom_id, dependencies)
    }
}

pub(super) fn verify(proof: &Proof<'_>) -> StorageResult<()> {
    let mut metadata = files::MetadataProof::new(Source(proof));
    let mut read = super::rows::read(proof.main, proof.key_heads)?;
    let mut rows = Rows::new(proof.main, read.as_mut(), "tips")?;
    while let Some((key, body)) = rows.next()? {
        metadata.row(&key, body)?;
    }
    Ok(())
}
