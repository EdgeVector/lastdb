//! Who else shares the records a manifest TTL would purge.
//!
//! The sweep reads and purges through the anchor schema. The anchor's molecules
//! are shared with every schema bound to it, so a TTL on one name deletes the
//! records of all of them. This is the guard that runs before an alias or a
//! policy is written.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::manifest_retention::RetentionSharing;
use crate::schema::types::declarative_schemas::FieldMapper;
use crate::schema::{Schema, SchemaError};
use std::collections::{BTreeSet, HashMap, HashSet};

impl SchemaStore {
    /// Every retention alias row: `(manifest name, anchor)`. Reads only the
    /// reserved alias prefix, never product rows.
    pub async fn retention_aliases(&self) -> Result<Vec<(String, String)>, SchemaError> {
        let scan = self
            .schema_states_store
            .scan_items_with_prefix_partition_undecodable::<SchemaRetentionAnchor>(
                RETENTION_ANCHOR_KEY_PREFIX,
            )
            .await?;
        for (key, error) in &scan.undecodable {
            tracing::warn!(
                key = %key,
                error = %error,
                "unreadable schema retention anchor row; skipping"
            );
        }
        Ok(scan
            .items
            .into_iter()
            .filter_map(|(key, row)| {
                let name = key.strip_prefix(RETENTION_ANCHOR_KEY_PREFIX)?;
                (!name.is_empty()).then(|| (name.to_string(), row.anchor))
            })
            .collect())
    }

    /// The manifest names that point at `anchor`, sorted.
    pub async fn retention_aliases_of(&self, anchor: &str) -> Result<Vec<String>, SchemaError> {
        let mut names: Vec<String> = self
            .retention_aliases()
            .await?
            .into_iter()
            .filter(|(_, target)| target == anchor)
            .map(|(name, _)| name)
            .collect();
        names.sort();
        Ok(names)
    }

    /// Which other schemas and apps a TTL purge through `anchor` would reach.
    ///
    /// `binding` is the manifest name that declares the TTL. It and the anchor
    /// are the two names of one manifest entry, so they are not reported.
    /// Another schema counts as bound to the anchor when one of its field
    /// mappers names the anchor (or a schema bound to it), when a retention
    /// alias points at it, or when it holds a molecule the anchor holds. A
    /// schema the anchor was expanded from is the same app's earlier version
    /// and is not reported, unless another app owns it.
    pub async fn retention_sharing(
        &self,
        binding: &str,
        anchor: &str,
        owner_app: &str,
    ) -> Result<RetentionSharing, SchemaError> {
        let schemas = self.get_all_schemas().await?;
        let aliases = self.retention_aliases().await?;
        Ok(compute_sharing(
            &schemas, &aliases, binding, anchor, owner_app,
        ))
    }
}

fn mapper_sources(schema: &Schema) -> impl Iterator<Item = &str> {
    schema
        .field_mappers
        .as_ref()
        .into_iter()
        .flat_map(|mappers| mappers.values())
        .map(FieldMapper::source_schema)
}

fn molecules_of<'a>(
    schemas: &'a HashMap<String, Schema>,
    names: &BTreeSet<String>,
) -> HashSet<&'a str> {
    names
        .iter()
        .filter_map(|name| schemas.get(name))
        .filter_map(|schema| schema.field_molecule_uuids.as_ref())
        .flat_map(|map| map.values())
        .map(String::as_str)
        .filter(|uuid| !uuid.is_empty())
        .collect()
}

fn compute_sharing(
    schemas: &HashMap<String, Schema>,
    aliases: &[(String, String)],
    binding: &str,
    anchor: &str,
    owner_app: &str,
) -> RetentionSharing {
    // The anchor and the schemas it was expanded from: the same data's history.
    let mut chain: BTreeSet<String> = BTreeSet::from([anchor.to_string()]);
    let mut pending = vec![anchor.to_string()];
    while let Some(name) = pending.pop() {
        let Some(schema) = schemas.get(&name) else {
            continue;
        };
        for source in mapper_sources(schema) {
            if chain.insert(source.to_string()) {
                pending.push(source.to_string());
            }
        }
    }
    let foreign_owners: Vec<String> = chain
        .iter()
        .filter(|name| {
            schemas
                .get(*name)
                .is_some_and(|schema| schema.owner_app_id.as_deref() != Some(owner_app))
        })
        .cloned()
        .collect();

    let molecules = molecules_of(schemas, &chain);
    let mut reach = chain;
    reach.insert(binding.to_string());
    let mut shared: BTreeSet<String> = BTreeSet::new();
    loop {
        let mut added = Vec::new();
        for (name, schema) in schemas {
            if reach.contains(name) || shared.contains(name) {
                continue;
            }
            let in_reach = |other: &str| reach.contains(other) || shared.contains(other);
            let mapped = mapper_sources(schema).any(in_reach);
            let aliased = aliases
                .iter()
                .any(|(alias, target)| alias == name && in_reach(target));
            let overlaps = schema.field_molecule_uuids.as_ref().is_some_and(|map| {
                map.values()
                    .any(|uuid| !uuid.is_empty() && molecules.contains(uuid.as_str()))
            });
            if mapped || aliased || overlaps {
                added.push(name.clone());
            }
        }
        if added.is_empty() {
            break;
        }
        shared.extend(added);
    }
    RetentionSharing {
        shared_with: shared.into_iter().collect(),
        foreign_owners,
    }
}
