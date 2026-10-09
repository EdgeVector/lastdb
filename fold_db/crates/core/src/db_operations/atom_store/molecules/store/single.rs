use super::*;

impl AtomStore {
    /// Explode a molecule into the `(storage key, record)` pairs a full rewrite
    /// persists. The single source of truth for that derivation: the write path
    /// and [`Self::molecule_rewrite_blocked`] must agree exactly, or the
    /// pre-flight would clear a rewrite the write then rejects.
    ///
    /// `domain` says whether the molecule's slots still need encoding; see
    /// [`MoleculeKeyDomain`].
    #[cfg(any(feature = "sharing", test))]
    pub(in super::super) fn per_key_storage_records(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        domain: MoleculeKeyDomain,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        data.per_key_records()
            .into_iter()
            .map(|(hash, range, entry, meta)| {
                let (storage_hash, storage_range) = match domain {
                    MoleculeKeyDomain::Storage => (hash, range),
                };
                Ok((
                    molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        &storage_hash,
                        &storage_range,
                    ),
                    PerKeyRecord { entry, meta },
                ))
            })
            .collect()
    }
}
