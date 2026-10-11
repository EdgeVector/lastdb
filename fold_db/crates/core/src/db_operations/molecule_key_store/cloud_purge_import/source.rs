//! Exact source bundle preflight before any candidate key writes.

use super::*;

impl MoleculeKeyStore {
    pub(super) async fn read_domain_import(
        &self,
        molecules: &[String],
        rows: &[Option<Vec<u8>>],
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<Vec<(String, MoleculeKeyBundle, Vec<u8>)>> {
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
        Ok(imported)
    }
}

pub(super) fn open_local_bundle(
    rows: &[Option<Vec<u8>>],
    molecule: &str,
    node_key: &[u8; 32],
) -> StorageResult<Option<MoleculeKeyBundle>> {
    match (&rows[0], &rows[1]) {
        (None, None) => Ok(None),
        (Some(metadata), Some(wrap)) => {
            let metadata: BundleMetadata = serde_json::from_slice(metadata).map_err(|_| {
                StorageError::InvalidOperation("local molecule metadata is invalid".into())
            })?;
            if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != molecule {
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
}
