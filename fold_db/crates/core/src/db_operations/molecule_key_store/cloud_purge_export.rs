//! Prepare isolated per-molecule keys without exporting a node-wide key.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct ExportedMoleculeKeys {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub bundles: BTreeMap<String, MoleculeKeyBundle>,
    pub legacy: BTreeSet<String>,
}

impl MoleculeKeyStore {
    pub(crate) async fn export_domain_keys(
        &self,
        molecules: &BTreeSet<String>,
        domain: &str,
        domain_key: &[u8; 32],
    ) -> StorageResult<ExportedMoleculeKeys> {
        let node_key = self
            .node_wrap_key
            .as_ref()
            .ok_or_else(|| StorageError::InvalidOperation("org source has no node key".into()))?;
        if domain == NODE_DOMAIN {
            return Err(StorageError::InvalidOperation(
                "org source requires an org domain".into(),
            ));
        }
        let lookup = molecules
            .iter()
            .flat_map(|id| {
                [
                    metadata_key(id).into_bytes(),
                    wrap_key_for(id, NODE_DOMAIN).into_bytes(),
                ]
            })
            .collect();
        let values = self.entries.inner().get_many(lookup).await?;
        if values.len() != molecules.len() * 2 {
            return Err(StorageError::InvalidOperation(
                "incomplete molecule key batch".into(),
            ));
        }
        let mut output = ExportedMoleculeKeys {
            rows: Vec::new(),
            bundles: BTreeMap::new(),
            legacy: BTreeSet::new(),
        };
        for (id, pair) in molecules.iter().zip(values.chunks_exact(2)) {
            let bundle = match (&pair[0], &pair[1]) {
                (None, None) => {
                    output.legacy.insert(id.clone());
                    MoleculeKeyBundle::generate()
                }
                (Some(metadata), Some(wrap)) => {
                    let metadata: BundleMetadata =
                        serde_json::from_slice(metadata).map_err(|_| {
                            StorageError::InvalidOperation("invalid molecule metadata".into())
                        })?;
                    if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != *id {
                        return Err(StorageError::InvalidOperation(
                            "molecule metadata differs".into(),
                        ));
                    }
                    let wrap: BundleWrap = serde_json::from_slice(wrap).map_err(|_| {
                        StorageError::InvalidOperation("invalid node molecule wrap".into())
                    })?;
                    open_wrap(&wrap, id, NODE_DOMAIN, node_key)?
                }
                _ => {
                    return Err(StorageError::InvalidOperation(
                        "incomplete molecule key identity".into(),
                    ))
                }
            };
            let plaintext = zeroize::Zeroizing::new(serde_json::to_vec(&bundle).map_err(|_| {
                StorageError::SerializationError("serialize org molecule key".into())
            })?);
            let ciphertext = seal_at_rest_utf8(domain_key, &plaintext)
                .map_err(|_| StorageError::InvalidOperation("org molecule wrap failed".into()))?;
            let wrap = BundleWrap {
                version: BUNDLE_VERSION,
                molecule_uuid: id.clone(),
                domain: domain.into(),
                sealed_bundle: String::from_utf8(ciphertext).map_err(|_| {
                    StorageError::InvalidOperation("invalid org wrap encoding".into())
                })?,
            };
            let metadata = BundleMetadata {
                version: BUNDLE_VERSION,
                molecule_uuid: id.clone(),
            };
            output.rows.push((
                metadata_key(id).into_bytes(),
                serde_json::to_vec(&metadata).map_err(|_| {
                    StorageError::SerializationError("serialize molecule metadata".into())
                })?,
            ));
            output.rows.push((
                wrap_key_for(id, domain).into_bytes(),
                serde_json::to_vec(&wrap)
                    .map_err(|_| StorageError::SerializationError("serialize org wrap".into()))?,
            ));
            output.bundles.insert(id.clone(), bundle);
        }
        Ok(output)
    }
}
