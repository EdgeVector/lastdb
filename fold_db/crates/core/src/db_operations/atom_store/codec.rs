//! Atom body encode/decode and storage-key reads for [`AtomStore`].

use super::*;

impl AtomStore {
    /// Warm an existing molecule key bundle without creating storage state.
    /// Missing bundles are legacy molecules and keep the node-level codec.
    pub(crate) async fn load_molecule_key_bundle(
        &self,
        molecule_uuid: &str,
    ) -> Result<(), crate::schema::SchemaError> {
        if let Some(store) = self.molecule_keys.as_ref() {
            store.load(molecule_uuid).await.map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "load molecule key bundle for {molecule_uuid}: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Open atom JSON from durable storage (content field dual-read).
    pub(crate) async fn open_atom_value(
        &self,
        mut atom_value: serde_json::Value,
    ) -> Result<serde_json::Value, crate::schema::SchemaError> {
        let bundle_key = self.take_atom_bundle_key(&mut atom_value).await?;
        if let Some(key) = bundle_key.as_ref().or(self.content_key.as_ref()) {
            crate::atom::open_atom_json(key, &mut atom_value).map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("open atom content: {e}"))
            })?;
        }
        Ok(atom_value)
    }

    async fn take_atom_bundle_key(
        &self,
        atom_header: &mut serde_json::Value,
    ) -> Result<Option<[u8; 32]>, crate::schema::SchemaError> {
        let bundle_molecule = atom_header
            .as_object_mut()
            .and_then(|object| object.remove("molecule_key_bundle"))
            .and_then(|value| value.as_str().map(str::to_owned));
        let (Some(store), Some(molecule_uuid)) =
            (self.molecule_keys.as_ref(), bundle_molecule.as_deref())
        else {
            return Ok(None);
        };
        Ok(Some(
            store
                .load(molecule_uuid)
                .await
                .map_err(|error| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "load molecule key bundle {molecule_uuid}: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "atom names missing molecule key bundle {molecule_uuid}"
                    ))
                })?
                .content_dek(),
        ))
    }

    pub(crate) async fn decode_atom(
        &self,
        atom_value: serde_json::Value,
    ) -> Result<crate::atom::Atom, crate::schema::SchemaError> {
        let opened = self.open_atom_value(atom_value).await?;
        serde_json::from_value(opened)
            .map_err(|e| crate::schema::SchemaError::InvalidData(format!("decode atom: {e}")))
    }

    /// Decode either a legacy JSON atom row or the binary `ATB:` container.
    pub(crate) async fn decode_atom_bytes(
        &self,
        stored: &[u8],
    ) -> Result<crate::atom::Atom, crate::schema::SchemaError> {
        if let Some((mut header, _)) = crate::atom::parse_atom_binary_row(stored).map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("decode binary atom row: {e}"))
        })? {
            let bundle_key = self.take_atom_bundle_key(&mut header).await?;
            let key = bundle_key
                .as_ref()
                .or(self.content_key.as_ref())
                .ok_or_else(|| {
                    crate::schema::SchemaError::InvalidData(
                        "binary atom row requires an atom content key".into(),
                    )
                })?;
            let mut opened = crate::atom::open_atom_binary_row(key, stored)
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "open binary atom content: {e}"
                    ))
                })?
                .expect("binary parser already matched");
            if let Some(object) = opened.as_object_mut() {
                object.remove("molecule_key_bundle");
            }
            return serde_json::from_value(opened)
                .map_err(|e| crate::schema::SchemaError::InvalidData(format!("decode atom: {e}")));
        }
        let value = serde_json::from_slice(stored).map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("decode atom row JSON: {e}"))
        })?;
        self.decode_atom(value).await
    }

    /// Encode an atom for durable storage, using the binary content container
    /// when its rollout switch is on.
    pub(crate) async fn encode_atom_bytes(
        &self,
        atom: &crate::atom::Atom,
        molecule_uuid: Option<&str>,
    ) -> Result<Vec<u8>, crate::schema::SchemaError> {
        self.encode_atom_bytes_with_binary(atom, molecule_uuid, self.atom_content_binary)
            .await
    }

    pub(crate) async fn encode_atom_bytes_with_binary(
        &self,
        atom: &crate::atom::Atom,
        molecule_uuid: Option<&str>,
        binary: bool,
    ) -> Result<Vec<u8>, crate::schema::SchemaError> {
        let mut value = serde_json::to_value(atom)
            .map_err(|e| crate::schema::SchemaError::InvalidData(format!("serialize atom: {e}")))?;
        let bundle_key = if let (Some(store), Some(molecule_uuid)) =
            (self.molecule_keys.as_ref(), molecule_uuid)
        {
            if store.is_enabled() {
                let bundle = store.ensure(molecule_uuid).await.map_err(|error| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "ensure molecule key bundle {molecule_uuid}: {error}"
                    ))
                })?;
                value
                    .as_object_mut()
                    .expect("Atom serializes as an object")
                    .insert(
                        "molecule_key_bundle".to_string(),
                        serde_json::Value::String(molecule_uuid.to_string()),
                    );
                Some(bundle.content_dek())
            } else {
                None
            }
        } else {
            None
        };
        let key = bundle_key.as_ref().or(self.content_key.as_ref());
        if binary {
            let key = key.ok_or_else(|| {
                crate::schema::SchemaError::InvalidData(
                    "binary atom row requires an atom content key".into(),
                )
            })?;
            return crate::atom::seal_atom_binary_row(key, &value).map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "seal binary atom content: {error}"
                ))
            });
        }
        if let Some(key) = key {
            crate::atom::seal_atom_json(key, &mut value).map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!("seal atom content: {error}"))
            })?;
        }
        serde_json::to_vec(&value).map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("serialize stored atom: {e}"))
        })
    }

    /// Batch-load atoms by full storage keys (`atom:{uuid}` or
    /// `{prefix}:atom:{uuid}`), opening content-seal when configured.
    ///
    /// **Must** be used instead of `raw().get_items::<Atom>(…)` — deserializing
    /// sealed rows straight into [`Atom`] leaves `content` as `ENC:…` ciphertext
    /// and leaks into query / hash-key paths.
    pub(super) async fn get_atoms_by_storage_keys_captured(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<crate::atom::Atom>>, crate::schema::SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let captured = runtime
            .as_ref()
            .map(|runtime| runtime.captured_atoms(keys))
            .transpose()?
            .unwrap_or_else(|| vec![None; keys.len()]);
        let raw = self.load_cold_atom_bytes(keys, &captured).await?;
        let mut out = Vec::with_capacity(keys.len());
        for ((requested, raw), captured) in keys.iter().zip(raw).zip(captured) {
            let atom = match captured {
                Some(atom) => Some(atom.as_ref().clone()),
                None => match raw {
                    Some(value) => Some(self.decode_atom_bytes(&value).await?),
                    None => None,
                },
            };
            if let Some(atom) = &atom {
                crate::storage::disk_pack::logical_paths::source::note_atom(
                    runtime.clone(),
                    requested,
                    atom,
                )?;
            }
            out.push(atom);
        }
        Ok(out)
    }

    /// Read exact current keys first; only a missed flat key needs its alias.
    async fn load_cold_atom_bytes(
        &self,
        keys: &[String],
        captured: &[Option<Arc<crate::atom::Atom>>],
    ) -> Result<Vec<Option<Vec<u8>>>, crate::schema::SchemaError> {
        let cold: Vec<_> = captured
            .iter()
            .enumerate()
            .filter(|(_, atom)| atom.is_none())
            .map(|(index, _)| index)
            .collect();
        let lookup = cold
            .iter()
            .map(|&index| keys[index].as_bytes().to_vec())
            .collect();
        let values = self.main_store.inner().get_many(lookup).await?;
        let mut raw = vec![None; keys.len()];
        let mut missing = Vec::new();
        for (index, value) in cold.into_iter().zip(values) {
            if value.is_none() {
                if let Some(alias) = crate::kind_partition::form_twin(&keys[index]) {
                    missing.push((index, alias.into_bytes()));
                }
            }
            raw[index] = value;
        }
        if !missing.is_empty() {
            let lookup = missing.iter().map(|(_, alias)| alias.clone()).collect();
            let values = self.main_store.inner().get_many(lookup).await?;
            for ((index, _), value) in missing.into_iter().zip(values) {
                raw[index] = value;
            }
        }
        Ok(raw)
    }

    pub(crate) fn primary_atom_key(
        &self,
        atom: &str,
        hint: Option<&crate::atom::AtomPartition>,
        scope: Option<&str>,
    ) -> String {
        crate::schema::types::field::build_storage_key(
            scope,
            &crate::atom::atom_key_codec::storage_key(self.atom_key_encoding(), hint, atom),
        )
    }

    pub(super) async fn load_atom_at_source(
        &self,
        key: &str,
        scope: Option<&str>,
        hint: Option<&crate::atom::AtomPartition>,
        primary: &str,
    ) -> Result<Option<crate::atom::Atom>, crate::schema::SchemaError> {
        let atom = self
            .get_atoms_by_storage_keys(&[key.to_owned()])
            .await?
            .into_iter()
            .next()
            .flatten();
        if let Some(atom) = &atom {
            let runtime = self
                .namespaced_store
                .as_ref()
                .and_then(|store| store.disk_pack_runtime());
            crate::storage::disk_pack::logical_paths::source::bind_atom(
                runtime, scope, key, atom, hint, primary,
            )?;
        }
        Ok(atom)
    }
}
