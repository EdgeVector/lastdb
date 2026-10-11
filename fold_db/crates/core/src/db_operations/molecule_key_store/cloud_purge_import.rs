//! Import an org-authorized bundle without changing an existing molecule's keys.

use super::*;

impl MoleculeKeyStore {
    /// Preserve keys from this same node's stopped old store. Never replace a
    /// canonical bundle used by the adopted personal data or another org.
    pub async fn preserve_local_node_bundles(
        &self,
        source: &dyn KvStore,
        molecules: &std::collections::BTreeSet<String>,
    ) -> StorageResult<()> {
        let node_key = self.node_wrap_key.as_ref().ok_or_else(|| {
            StorageError::InvalidOperation("local share preservation has no node key".into())
        })?;
        let _guard = self.mint_lock.lock().await;
        let keys: Vec<_> = molecules
            .iter()
            .flat_map(|molecule| {
                [
                    metadata_key(molecule).into_bytes(),
                    wrap_key_for(molecule, NODE_DOMAIN).into_bytes(),
                ]
            })
            .collect();
        let (previous, current) = futures::join!(
            source.get_many(keys.clone()),
            self.entries.inner().get_many(keys.clone())
        );
        let previous = previous?;
        let current = current?;
        if previous.len() != keys.len() || current.len() != keys.len() {
            return Err(StorageError::InvalidOperation(
                "local molecule bundle batch is incomplete".into(),
            ));
        }
        let mut puts = Vec::new();
        let mut imported = Vec::new();
        for (((molecule, previous), current), keys) in molecules
            .iter()
            .zip(previous.chunks_exact(2))
            .zip(current.chunks_exact(2))
            .zip(keys.chunks_exact(2))
        {
            validate_identity(molecule, NODE_DOMAIN)?;
            let open = |rows: &[Option<Vec<u8>>]| -> StorageResult<Option<MoleculeKeyBundle>> {
                match (&rows[0], &rows[1]) {
                    (None, None) => Ok(None),
                    (Some(metadata), Some(wrap)) => {
                        let metadata: BundleMetadata =
                            serde_json::from_slice(metadata).map_err(|_| {
                                StorageError::InvalidOperation(
                                    "local molecule metadata is invalid".into(),
                                )
                            })?;
                        if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != *molecule
                        {
                            return Err(StorageError::InvalidOperation(
                                "local molecule identity differs".into(),
                            ));
                        }
                        let wrap: BundleWrap = serde_json::from_slice(wrap).map_err(|_| {
                            StorageError::InvalidOperation("local molecule wrap is invalid".into())
                        })?;
                        open_wrap(&wrap, molecule, NODE_DOMAIN, node_key).map(Some)
                    }
                    _ => Err(StorageError::InvalidOperation(
                        "local molecule bundle is incomplete".into(),
                    )),
                }
            };
            let old = open(previous)?;
            let existing = open(current)?;
            if let (Some(old), Some(existing)) = (&old, &existing) {
                if old.content_dek != existing.content_dek
                    || old.blind_key != existing.blind_key
                    || old.ope_key != existing.ope_key
                {
                    return Err(StorageError::InvalidOperation(
                        "prior inbound share keys conflict with adopted data".into(),
                    ));
                }
            }
            if let Some(old) = old {
                if existing.is_none() {
                    for (key, value) in keys.iter().zip(previous) {
                        puts.push((
                            key.clone(),
                            value.clone().ok_or_else(|| {
                                StorageError::InvalidOperation(
                                    "local molecule wrap disappeared".into(),
                                )
                            })?,
                        ));
                    }
                }
                imported.push((molecule.clone(), old));
            }
        }
        self.entries.inner().batch_put(puts).await?;
        self.entries.inner().flush().await?;
        for (molecule, bundle) in imported {
            self.remember(&molecule, bundle);
        }
        Ok(())
    }

    /// Preflight all source domain wraps, compare existing bundles, then add
    /// this node's wrap. Only call on an inactive store: catalog publication
    /// and its durable switch receipt happen after this completes.
    pub async fn import_domain_for_schema(
        &self,
        source: &dyn KvStore,
        schema: &Schema,
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<usize> {
        let node_wrap_key = self.node_wrap_key.as_ref().ok_or_else(|| {
            StorageError::InvalidOperation("org adoption requires a node wrapping key".into())
        })?;
        validate_identity(&schema.name, domain)?;
        if domain == NODE_DOMAIN {
            return Err(StorageError::InvalidOperation(
                "org adoption requires an org access domain".into(),
            ));
        }
        let _guard = self.mint_lock.lock().await;
        let molecules = schema_molecule_uuids(schema);
        let keys = molecules
            .iter()
            .flat_map(|molecule| {
                [
                    metadata_key(molecule).into_bytes(),
                    wrap_key_for(molecule, domain).into_bytes(),
                ]
            })
            .collect();
        let rows = source.get_many(keys).await?;
        if rows.len() != molecules.len() * 2 {
            return Err(StorageError::InvalidOperation(
                "org bundle source returned an incomplete batch".into(),
            ));
        }
        let mut imported = Vec::with_capacity(molecules.len());
        for (molecule, rows) in molecules.iter().zip(rows.chunks_exact(2)) {
            let metadata: BundleMetadata =
                serde_json::from_slice(rows[0].as_deref().ok_or_else(|| {
                    StorageError::InvalidOperation(
                        "org replacement has a missing molecule bundle".into(),
                    )
                })?)
                .map_err(|_| {
                    StorageError::InvalidOperation(
                        "org replacement has invalid molecule metadata".into(),
                    )
                })?;
            if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != *molecule {
                return Err(StorageError::InvalidOperation(
                    "org molecule metadata identity differs".into(),
                ));
            }
            let wrap: BundleWrap = serde_json::from_slice(rows[1].as_deref().ok_or_else(|| {
                StorageError::InvalidOperation(
                    "org replacement lacks the authorized domain wrap".into(),
                )
            })?)
            .map_err(|_| {
                StorageError::InvalidOperation(
                    "org replacement has invalid domain wrap metadata".into(),
                )
            })?;
            let bundle = open_wrap(&wrap, molecule, domain, domain_wrap_key)?;
            if let Some(existing) = self.load(molecule).await? {
                if existing.content_dek != bundle.content_dek
                    || existing.blind_key != bundle.blind_key
                    || existing.ope_key != bundle.ope_key
                {
                    return Err(StorageError::InvalidOperation(
                        "org replacement conflicts with an existing molecule key bundle".into(),
                    ));
                }
            }
            imported.push((
                molecule.clone(),
                bundle,
                rows[1].clone().ok_or_else(|| {
                    StorageError::InvalidOperation("org source wrap disappeared".into())
                })?,
            ));
        }
        let mut puts = Vec::with_capacity(imported.len() * 3);
        for (molecule, bundle, source_wrap) in &imported {
            let plaintext = serde_json::to_vec(bundle).map_err(|_| {
                StorageError::SerializationError("serialize org molecule bundle".into())
            })?;
            let sealed = seal_at_rest_utf8(node_wrap_key, &plaintext).map_err(|_| {
                StorageError::InvalidOperation("org molecule node wrap failed".into())
            })?;
            let wrap = BundleWrap {
                version: BUNDLE_VERSION,
                molecule_uuid: molecule.clone(),
                domain: NODE_DOMAIN.into(),
                sealed_bundle: String::from_utf8(sealed).map_err(|_| {
                    StorageError::InvalidOperation(
                        "org molecule node wrap has invalid encoding".into(),
                    )
                })?,
            };
            puts.push((
                wrap_key_for(molecule, NODE_DOMAIN).into_bytes(),
                serde_json::to_vec(&wrap).map_err(|_| {
                    StorageError::SerializationError("serialize org node wrap".into())
                })?,
            ));
            puts.push((
                metadata_key(molecule).into_bytes(),
                serde_json::to_vec(&BundleMetadata {
                    version: BUNDLE_VERSION,
                    molecule_uuid: molecule.clone(),
                })
                .map_err(|_| {
                    StorageError::SerializationError("serialize molecule metadata".into())
                })?,
            ));
            let source_key = wrap_key_for(molecule, domain);
            puts.push((source_key.into_bytes(), source_wrap.clone()));
        }
        self.entries.inner().batch_put(puts).await?;
        self.entries.inner().flush().await?;
        for (molecule, bundle, _) in imported {
            self.remember(&molecule, bundle);
        }
        Ok(molecules.len())
    }
}
