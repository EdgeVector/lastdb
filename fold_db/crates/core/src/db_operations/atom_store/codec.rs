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
    pub async fn get_atoms_by_storage_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<crate::atom::Atom>>, crate::schema::SchemaError> {
        let forms: Vec<Vec<String>> = keys
            .iter()
            .map(|key| crate::kind_partition::read_forms(key))
            .collect();
        let lookup: Vec<Vec<u8>> = forms
            .iter()
            .flatten()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        let raw = self
            .main_store
            .inner()
            .get_many(lookup)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("Failed to fetch atom batch: {e}"))
            })?;
        let mut raw_iter = raw.into_iter();
        let mut out = Vec::with_capacity(keys.len());
        for key_forms in forms {
            let mut hit = None;
            for _ in &key_forms {
                let item = raw_iter.next().flatten();
                if hit.is_none() {
                    hit = item;
                }
            }
            out.push(match hit {
                Some(v) => Some(self.decode_atom_bytes(&v).await?),
                None => None,
            });
        }
        Ok(out)
    }
}
