use super::*;

impl AtomStore {
    pub(crate) async fn history_key_codec(
        &self,
        molecule_uuid: &str,
    ) -> Result<crate::atom::MoleculeKeyCodec, SchemaError> {
        self.load_molecule_key_bundle(molecule_uuid).await?;
        self.key_codec_for_molecule(molecule_uuid)
    }

    /// Load all mutation events for a molecule, sorted chronologically.
    ///
    /// The exact prefix is `history\0{molecule}:`, with the requested storage
    /// scope when present. No alternate key spelling is read.
    ///
    /// This is a history inspection and purge API, not a historical query view.
    pub async fn get_mutation_events(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<MutationEvent>, SchemaError> {
        self.load_molecule_key_bundle(molecule_uuid).await?;
        self.get_mutation_event_rows(molecule_uuid, storage_prefix)
            .await?
            .into_iter()
            .map(|(_, mut event)| {
                event.field_key =
                    self.canonical_history_field_key(molecule_uuid, &event.field_key)?;
                Ok(event)
            })
            .collect()
    }

    /// Canonical API coordinates for a history response. Stored history and
    /// signed provenance retain their original bytes.
    pub fn canonical_history_field_key(
        &self,
        molecule_uuid: &str,
        key: &crate::atom::FieldKey,
    ) -> Result<crate::atom::FieldKey, SchemaError> {
        let mut key = key.clone();
        if let Some(range) = &mut key.range {
            *range = self
                .key_codec_for_molecule(molecule_uuid)?
                .canonical_api_range(range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        }
        Ok(key)
    }

    /// Load mutation history with each authoritative storage key intact.
    pub async fn get_mutation_event_rows(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, MutationEvent)>, SchemaError> {
        let base_prefix = molecule_key_codec::history_molecule_prefix(molecule_uuid);

        // Reuse a confirmed empty result for this opened store. Current event
        // writers invalidate the exact prefix after their successful batch.
        let scan_key = build_storage_key(storage_prefix, &base_prefix);
        if crate::atom::history_event_memo::is_known_empty(self.store_id(), &scan_key) {
            return Ok(Vec::new());
        }

        let items: Vec<(String, MutationEvent)> = self
            .scan_exact_storage_prefix(
                &base_prefix,
                storage_prefix,
                "Failed to load mutation events",
            )
            .await?;

        // A nonempty prefix always reads storage; no event values are cached.
        if items.is_empty() {
            crate::atom::history_event_memo::mark_empty(self.store_id(), &scan_key);
        }

        // Rows are already in lexicographic order, which is chronological for
        // the zero-padded history key.
        Ok(items)
    }
}
