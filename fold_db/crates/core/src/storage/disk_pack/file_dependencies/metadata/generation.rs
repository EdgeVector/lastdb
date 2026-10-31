//! Use the core's existing selector and merge law; inactive bases hold no live metadata.
use super::*;
use crate::atom::molecule_key_codec;
use crate::records::molecule_record::{
    apply_generation_delete, merge_generation_slot, MoleculeGenerationDelete,
    MoleculeGenerationPointer, MoleculeGenerationSlot,
};

impl<R: MetadataRead> MetadataProof<R> {
    pub(super) fn live(
        &mut self,
        scope: Option<&str>,
        bare: &str,
        body: Vec<u8>,
    ) -> StorageResult<Option<PerKeyRecord>> {
        let record = narrow(decode(body)?)?;
        let molecule = molecule_key_codec::molecule_uuid_from_storage_key(bare)
            .ok_or_else(|| invalid("metadata molecule identity"))?;
        let pointer: Option<MoleculeGenerationPointer> = self.get(
            scope,
            &molecule_key_codec::molecule_generation_pointer_key(molecule),
        )?;
        let Some(pointer) = pointer else {
            return Ok(Some(record));
        };
        if pointer.generation.len() > 1024 {
            return Err(invalid("metadata generation identity"));
        }
        let key = molecule_key_codec::molecule_generation_bound_for_record_bound(
            molecule,
            &pointer.generation,
            bare,
        )
        .ok_or_else(|| invalid("metadata generation coordinate"))?;
        drop(pointer);
        let slot = self
            .get::<MoleculeGenerationSlot>(scope, &key)?
            .map(narrow_slot)
            .transpose()?;
        self.select(scope, molecule, bare, Some(record), slot)
    }

    pub(super) fn generation(
        &mut self,
        scope: Option<&str>,
        bare: &str,
        body: Vec<u8>,
    ) -> StorageResult<Option<PerKeyRecord>> {
        let rest = bare
            .strip_prefix("mgr:v1:")
            .ok_or_else(|| invalid("generation metadata key"))?;
        let (molecule, rest) = rest
            .split_once(':')
            .ok_or_else(|| invalid("generation metadata molecule"))?;
        let (generation, _) = rest
            .split_once(':')
            .ok_or_else(|| invalid("generation metadata identity"))?;
        let pointer: Option<MoleculeGenerationPointer> = self.get(
            scope,
            &molecule_key_codec::molecule_generation_pointer_key(molecule),
        )?;
        if pointer
            .as_ref()
            .is_none_or(|pointer| pointer.generation != generation)
        {
            return Ok(None);
        }
        drop(pointer);
        let slot = narrow_slot(decode(body)?)?;
        let base =
            molecule_key_codec::molecule_record_key_from_generation_key(molecule, generation, bare)
                .ok_or_else(|| invalid("generation metadata coordinate"))?;
        let live: Option<PerKeyRecord> = self
            .get::<PerKeyRecord>(scope, &base)?
            .map(narrow)
            .transpose()?;
        self.select(scope, molecule, &base, live, Some(slot))
    }

    fn select(
        &mut self,
        scope: Option<&str>,
        molecule: &str,
        base: &str,
        live: Option<PerKeyRecord>,
        slot: Option<MoleculeGenerationSlot>,
    ) -> StorageResult<Option<PerKeyRecord>> {
        let record = merge_generation_slot(live, slot);
        let delete =
            molecule_key_codec::molecule_generation_delete_bound_for_record_bound(molecule, base)
                .ok_or_else(|| invalid("generation metadata delete coordinate"))?;
        let deletion: Option<MoleculeGenerationDelete> = self.get(scope, &delete)?;
        if let Some(deletion) = &deletion {
            entry_bound(&deletion.shadow)?;
        }
        Ok(apply_generation_delete(record, deletion))
    }
}

fn narrow_slot(mut slot: MoleculeGenerationSlot) -> StorageResult<MoleculeGenerationSlot> {
    slot.record = slot.record.map(narrow).transpose()?;
    if let Some(shadow) = &slot.shadow {
        entry_bound(shadow)?;
    }
    Ok(slot)
}
