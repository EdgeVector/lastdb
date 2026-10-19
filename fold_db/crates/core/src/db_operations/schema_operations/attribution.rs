//! Schema-root, root-tip and retention attribution walks.

use super::super::core::DbOperations;
use super::attribution_reports::{SchemaRetentionAttributionReport, SchemaRootAttributionReport};
use super::schema_molecule_ref_edges;
use crate::db_operations::{
    classification_for_roots, AttributionObjectKind, AttributionPath, AttributionRecord,
    AttributionRootKind, AttributionSize,
};
use crate::schema::types::declarative_schemas::SchemaSource;
use crate::schema::SchemaError;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

fn attribution_digest(parts: impl IntoIterator<Item = String>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"lastdb:schema-root-attribution:v1\0");
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

impl DbOperations {
    /// Persist the catalog schema-to-molecule roots for one attribution epoch.
    ///
    /// The root path set stays in the local attribution namespace. It does not
    /// change atoms, tips, catalog rows, or liveness edges. A caller may run
    /// this only as the first layer of a resumable attribution walk; it must
    /// still classify all descendant and unrooted physical objects before it
    /// can complete the epoch.
    pub async fn attribute_schema_root_molecules(
        &self,
        epoch_id: &str,
        source_sequence: u64,
    ) -> Result<SchemaRootAttributionReport, SchemaError> {
        if epoch_id.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "schema root attribution requires an epoch id".to_string(),
            ));
        }
        let schemas = self.get_all_schemas().await?;
        let paths_by_molecule = Self::schema_root_paths_by_molecule(&schemas);

        let ledger = self.attribution();
        let meters = self.atoms().keep_small();
        let mut report = SchemaRootAttributionReport {
            schemas_read: schemas.len() as u64,
            molecule_roots: 0,
            root_paths_written: 0,
            size_complete: meters.all_counter_domains_complete(),
            missing_molecule_counters: 0,
            pending_protein_folds: 0,
            logical_value_bytes: 0,
            structure_bytes: 0,
            retained_history_bytes: 0,
        };
        for (molecule_id, paths) in paths_by_molecule {
            self.attribute_molecule_root(
                epoch_id,
                source_sequence,
                &molecule_id,
                paths,
                meters,
                &mut report,
            )
            .await?;
        }
        // A completed page must survive before its epoch cursor advances.
        ledger.flush().await?;
        Ok(report)
    }

    /// Group the catalog's schema-to-molecule edges by molecule, tagged with the root kind.
    fn schema_root_paths_by_molecule(
        schemas: &std::collections::HashMap<String, crate::schema::Schema>,
    ) -> BTreeMap<String, Vec<(AttributionRootKind, String, String)>> {
        // A schema the catalog does not mark `User` is a system-owned seed
        // (`SystemSeed`/`StarterSeed`): its molecules are reachable only
        // because the service itself depends on or ships them, not because a
        // node operator declared them, so they root through `System` rather
        // than `Schema`.
        let mut paths_by_molecule: BTreeMap<String, Vec<(AttributionRootKind, String, String)>> =
            BTreeMap::new();
        for (schema_name, schema) in schemas {
            let root_kind = if schema.source == SchemaSource::User {
                AttributionRootKind::Schema
            } else {
                AttributionRootKind::System
            };
            for edge in schema_molecule_ref_edges(schema_name, schema) {
                let root_id = edge.source;
                let edge_path_digest = attribution_digest([
                    format!("schema={schema_name}"),
                    format!("root={root_id}"),
                    format!("molecule={}", edge.molecule_uuid),
                ]);
                paths_by_molecule
                    .entry(edge.molecule_uuid)
                    .or_default()
                    .push((root_kind, root_id, edge_path_digest));
            }
        }
        paths_by_molecule
    }

    /// Write one molecule's root paths and its attribution record.
    async fn attribute_molecule_root(
        &self,
        epoch_id: &str,
        source_sequence: u64,
        molecule_id: &str,
        mut paths: Vec<(AttributionRootKind, String, String)>,
        meters: &crate::db_operations::keep_small::KeepSmallMeters,
        report: &mut SchemaRootAttributionReport,
    ) -> Result<(), SchemaError> {
        let ledger = self.attribution();
        paths.sort();
        paths.dedup();
        for (root_kind, root_id, edge_path_digest) in &paths {
            ledger
                .put_attribution_path(&AttributionPath::new(
                    epoch_id,
                    AttributionObjectKind::Molecule,
                    molecule_id,
                    *root_kind,
                    root_id,
                    edge_path_digest,
                ))
                .await?;
            report.root_paths_written = report.root_paths_written.saturating_add(1);
        }
        let path_set_digest = attribution_digest(
            paths
                .iter()
                .map(|(root_kind, root_id, digest)| format!("{root_kind:?}\0{root_id}\0{digest}")),
        );
        let classification = classification_for_roots(paths.iter().map(|(kind, _, _)| kind));
        let size = meters.molecule_counter(molecule_id).map(|counter| {
            let size = AttributionSize {
                logical_value_bytes: counter.logical_value_bytes(),
                structure_bytes: counter.structure_bytes(),
                retained_history_bytes: counter.retained_history_bytes,
            };
            report.logical_value_bytes = report
                .logical_value_bytes
                .saturating_add(size.logical_value_bytes);
            report.structure_bytes = report.structure_bytes.saturating_add(size.structure_bytes);
            report.retained_history_bytes = report
                .retained_history_bytes
                .saturating_add(size.retained_history_bytes);
            size
        });
        let mut record = AttributionRecord::attributed(
            epoch_id,
            AttributionObjectKind::Molecule,
            molecule_id,
            classification,
            paths.len() as u64,
            path_set_digest,
            source_sequence,
        );
        if let Some(size) = size {
            record.size = Some(size);
        } else {
            report.size_complete = false;
            report.missing_molecule_counters = report.missing_molecule_counters.saturating_add(1);
        }
        ledger.put_attribution_record(&record).await?;
        report.molecule_roots = report.molecule_roots.saturating_add(1);

        Ok(())
    }

    /// Attribute the node-local retention registry's own bookkeeping rows as
    /// `Retention`-rooted objects.
    ///
    /// A schema's retention policy row lives in `schema_states`, outside the
    /// schema-molecule graph the walk above follows, so no schema root ever
    /// reaches it. Without a root of its own it would read as unattributed
    /// residue the moment a generic sweep looked at `schema_states`, even
    /// though the retention subsystem owns it on purpose. This gives it one,
    /// independent of whether the owning schema is still installed.
    pub async fn attribute_schema_retention_roots(
        &self,
        epoch_id: &str,
        source_sequence: u64,
    ) -> Result<SchemaRetentionAttributionReport, SchemaError> {
        if epoch_id.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "retention root attribution requires an epoch id".to_string(),
            ));
        }
        let policies = self.schemas().list_schema_retention_policies().await?;
        let ledger = self.attribution();
        let mut report = SchemaRetentionAttributionReport {
            schemas_with_policy: 0,
            root_paths_written: 0,
        };
        for (schema_name, policy) in policies {
            let object_id = format!("retention-policy:{schema_name}");
            let root_id = format!("retention:{schema_name}");
            let edge_path_digest = attribution_digest([
                format!("schema={schema_name}"),
                format!("root={root_id}"),
                format!("ttl_seconds={}", policy.ttl_seconds),
            ]);
            ledger
                .put_attribution_path(&AttributionPath::new(
                    epoch_id,
                    AttributionObjectKind::DerivedIndex,
                    &object_id,
                    AttributionRootKind::Retention,
                    &root_id,
                    &edge_path_digest,
                ))
                .await?;
            report.root_paths_written = report.root_paths_written.saturating_add(1);
            let path_set_digest = attribution_digest([format!("{root_id}\0{edge_path_digest}")]);
            let structure_bytes = serde_json::to_vec(&policy).map_or(0, |bytes| bytes.len() as u64);
            let mut record = AttributionRecord::attributed(
                epoch_id,
                AttributionObjectKind::DerivedIndex,
                &object_id,
                classification_for_roots(std::iter::once(&AttributionRootKind::Retention)),
                1,
                path_set_digest,
                source_sequence,
            );
            record.size = Some(AttributionSize {
                logical_value_bytes: 0,
                structure_bytes,
                retained_history_bytes: 0,
            });
            ledger.put_attribution_record(&record).await?;
            report.schemas_with_policy = report.schemas_with_policy.saturating_add(1);
        }
        ledger.flush().await?;
        Ok(report)
    }
}
