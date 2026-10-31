//! Who else shares the records a manifest TTL would delete.
//!
//! The sweep reads and deletes through the anchor schema. The anchor's molecules
//! are shared with every schema bound to it, so a TTL on one name deletes the
//! records of all of them. This is the guard that runs before an alias or a
//! policy is written.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::manifest_retention::{RetentionPolicyInReach, RetentionReach, RetentionSharing};
use crate::schema::{Schema, SchemaError, SchemaRetentionPolicy};
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

    /// Which other schemas and apps a TTL sweep through `anchor` would reach.
    ///
    /// `binding` is the manifest name that declares the TTL. It and the anchor
    /// are the two names of one manifest entry, so they are not reported.
    /// Another schema counts as bound to the anchor when a retention alias
    /// points at it (or at a schema bound to it), or when it holds a molecule
    /// the anchor holds. A schema the anchor was expanded from is the same
    /// app's earlier version and is not reported, unless another app owns it.
    ///
    /// `predecessors` seeds the expansion chain with schemas the anchor will be
    /// expanded from: a read-only check of a declare that the Schema Service
    /// would expand.
    pub async fn retention_sharing(
        &self,
        binding: &str,
        anchor: &str,
        owner_app: &str,
        predecessors: &[String],
    ) -> Result<RetentionSharing, SchemaError> {
        let schemas = self.get_all_schemas().await?;
        let aliases = self.retention_aliases().await?;
        Ok(compute_sharing(
            &schemas,
            &aliases,
            binding,
            anchor,
            owner_app,
            predecessors,
        ))
    }

    /// The retention policies whose sweep would delete records of a schema
    /// bound to `anchor`: a policy on the anchor, on a schema it was expanded
    /// from, or on any schema bound to either. This is the same closure as
    /// [`Self::retention_sharing`], so a plain declare is checked as widely as
    /// a declare with a TTL. A policy that only `binding` itself declared is its
    /// own and is not reported.
    pub async fn retention_policies_in_reach(
        &self,
        binding: &str,
        anchor: &str,
        predecessors: &[String],
    ) -> Result<RetentionReach, SchemaError> {
        let schemas = self.get_all_schemas().await?;
        let aliases = self.retention_aliases().await?;
        let reach = compute_reach(&schemas, &aliases, binding, anchor, predecessors);
        let names: Vec<String> = reach.chain.into_iter().chain(reach.shared).collect();
        let keys: Vec<String> = names
            .iter()
            .map(|name| retention_policy_key(name))
            .collect();
        let policies = self
            .schema_states_store
            .get_items::<SchemaRetentionPolicy>(&keys)
            .await?;
        let mut found = Vec::new();
        for (name, policy) in names.into_iter().zip(policies) {
            let Some(policy) = policy else {
                continue;
            };
            let declared: Vec<&str> = aliases
                .iter()
                .filter(|(_, target)| *target == name)
                .map(|(alias, _)| alias.as_str())
                .collect();
            let declared_by: Vec<String> = declared
                .iter()
                .filter(|alias| **alias != binding)
                .map(|alias| (*alias).to_string())
                .collect();
            if !declared.is_empty() && declared_by.is_empty() {
                continue;
            }
            found.push(RetentionPolicyInReach {
                anchor: name,
                declared_by,
                policy,
            });
        }
        Ok(RetentionReach { policies: found })
    }
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

/// The schemas a TTL sweep through an anchor reaches: the anchor's expansion
/// chain, and every schema bound to the chain.
struct Reach {
    /// The anchor and the schemas it was expanded from.
    chain: BTreeSet<String>,
    /// Other schemas bound to the chain, by retention alias or by a molecule
    /// that the chain holds, transitively. Neither `binding` nor the chain.
    shared: BTreeSet<String>,
}

fn compute_reach(
    schemas: &HashMap<String, Schema>,
    aliases: &[(String, String)],
    binding: &str,
    anchor: &str,
    predecessors: &[String],
) -> Reach {
    // The anchor and the schemas it was expanded from: the same data's history.
    let mut chain: BTreeSet<String> = BTreeSet::from([anchor.to_string()]);
    chain.extend(predecessors.iter().cloned());
    let molecules = molecules_of(schemas, &chain);
    let mut reach = chain.clone();
    reach.insert(binding.to_string());
    let mut shared: BTreeSet<String> = BTreeSet::new();
    loop {
        let mut added = Vec::new();
        for (name, schema) in schemas {
            if reach.contains(name) || shared.contains(name) {
                continue;
            }
            let in_reach = |other: &str| reach.contains(other) || shared.contains(other);
            let aliased = aliases
                .iter()
                .any(|(alias, target)| alias == name && in_reach(target));
            let overlaps = schema.field_molecule_uuids.as_ref().is_some_and(|map| {
                map.values()
                    .any(|uuid| !uuid.is_empty() && molecules.contains(uuid.as_str()))
            });
            if aliased || overlaps {
                added.push(name.clone());
            }
        }
        if added.is_empty() {
            break;
        }
        shared.extend(added);
    }
    Reach { chain, shared }
}

fn compute_sharing(
    schemas: &HashMap<String, Schema>,
    aliases: &[(String, String)],
    binding: &str,
    anchor: &str,
    owner_app: &str,
    predecessors: &[String],
) -> RetentionSharing {
    let reach = compute_reach(schemas, aliases, binding, anchor, predecessors);
    let foreign_owners: Vec<String> = reach
        .chain
        .iter()
        .filter(|name| {
            schemas
                .get(*name)
                .is_some_and(|schema| schema.owner_app_id.as_deref() != Some(owner_app))
        })
        .cloned()
        .collect();
    RetentionSharing {
        shared_with: reach.shared.into_iter().collect(),
        foreign_owners,
    }
}
