//! Versioned offline rebuild. Completeness is durable only after all source
//! classes, exact edge values, and their file barriers succeed.

use super::{AtomStore, MoleculeRefCompleteness, MoleculeRefEdge};
use super::{MOLECULE_REF_COMPLETE_KEY, MOLECULE_REF_PREFIX};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

const WRITE_BATCH: usize = 4096;

impl AtomStore {
    /// Rebuild schema-field, schema-record R, and protein-member edges from
    /// every canonical source on a stopped home under exclusive maintenance.
    /// A known-empty home may use the same protocol before it exposes writers.
    /// Old v1 markers never authorize the expanded source set or memory ack.
    pub async fn bootstrap_molecule_ref_edges_on_isolated_copy(
        &self,
        edges: &[MoleculeRefEdge],
        schema_fields: u64,
        protein_members: u64,
        storage_prefix: Option<&str>,
    ) -> Result<MoleculeRefCompleteness, SchemaError> {
        let version = self.molecule_ref_bootstrap_version()?;
        if version == 2 && self.database_catalog_refs_present(storage_prefix).await? {
            return Err(SchemaError::InvalidData(
                "native v2 bootstrap cannot omit named database-catalog sources".into(),
            ));
        }
        let expected = validate_sources(edges, schema_fields, protein_members, storage_prefix)?;
        let complete_key = build_storage_key(storage_prefix, MOLECULE_REF_COMPLETE_KEY);
        self.main_store
            .delete_item(&complete_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("clear molecule reference completeness: {error}"))
            })?;
        self.flush_molecule_ref_bootstrap("clear completeness")
            .await?;
        let prefix = build_storage_key(storage_prefix, MOLECULE_REF_PREFIX);
        self.replace_molecule_ref_bootstrap(&prefix, &expected)
            .await?;
        self.flush_molecule_ref_bootstrap("write complete edges")
            .await?;
        let written: Vec<(String, MoleculeRefEdge)> = self
            .main_store
            .scan_items_with_prefix(&prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("audit molecule reference bootstrap: {error}"))
            })?;
        let actual: BTreeMap<_, _> = written.into_iter().collect();
        if actual != expected {
            return Err(SchemaError::InvalidData(format!(
                "molecule reference bootstrap audit failed: expected {} edges, found {}",
                expected.len(),
                actual.len()
            )));
        }
        let proof = MoleculeRefCompleteness {
            version,
            schema_fields,
            protein_members,
        };
        self.main_store
            .put_item(&complete_key, &proof)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("write molecule reference completeness: {error}"))
            })?;
        self.flush_molecule_ref_bootstrap("publish completeness")
            .await?;
        Ok(proof)
    }

    async fn replace_molecule_ref_bootstrap(
        &self,
        prefix: &str,
        expected: &BTreeMap<String, MoleculeRefEdge>,
    ) -> Result<(), SchemaError> {
        let old_rows: Vec<(String, serde_json::Value)> = self
            .main_store
            .scan_items_with_prefix(prefix)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read old molecule reference edges: {error}"))
            })?;
        let keys: Vec<_> = old_rows.into_iter().map(|(key, _)| key).collect();
        for chunk in keys.chunks(WRITE_BATCH) {
            self.main_store
                .batch_delete_keys(chunk.to_vec())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "clear molecule reference edge batch: {error}"
                    ))
                })?;
        }
        let rows: Vec<_> = expected
            .iter()
            .map(|(key, edge)| (key.clone(), edge.clone()))
            .collect();
        for chunk in rows.chunks(WRITE_BATCH) {
            self.main_store
                .batch_put_items(chunk.to_vec())
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "write molecule reference edge batch: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    async fn flush_molecule_ref_bootstrap(&self, phase: &str) -> Result<(), SchemaError> {
        self.main_store.inner().flush().await.map_err(|error| {
            SchemaError::InvalidData(format!(
                "flush molecule reference bootstrap {phase}: {error}"
            ))
        })
    }

    fn molecule_ref_bootstrap_version(&self) -> Result<u8, SchemaError> {
        let native = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        if let Some(runtime) = native {
            if !runtime.schema_record_refs_supported() {
                return Err(SchemaError::InvalidData(
                    "native v2 bootstrap requires a durable CURRENT bit16 writer fence".into(),
                ));
            }
            Ok(2)
        } else {
            // Older legacy binaries do not maintain R edges. Never leave a v2
            // proof they could stale and later copy through native conversion.
            Ok(1)
        }
    }
}

fn validate_sources(
    edges: &[MoleculeRefEdge],
    schema_fields: u64,
    protein_members: u64,
    storage_prefix: Option<&str>,
) -> Result<BTreeMap<String, MoleculeRefEdge>, SchemaError> {
    let (mut schemas, mut proteins) = (0u64, 0u64);
    let mut expected = BTreeMap::new();
    for edge in edges {
        match edge.edge_type.as_str() {
            "schema-field" | "schema-record" => schemas += 1,
            "protein-member" => proteins += 1,
            _ => {
                return Err(SchemaError::InvalidData(
                    "molecule reference bootstrap contains an unsupported source class".into(),
                ))
            }
        }
        expected.insert(edge.storage_key(storage_prefix), edge.clone());
    }
    if (schemas, proteins) != (schema_fields, protein_members) {
        return Err(SchemaError::InvalidData(
            "molecule reference bootstrap source counts differ from complete input".into(),
        ));
    }
    Ok(expected)
}
