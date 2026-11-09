//! Who else shares the records a manifest TTL would delete.
//!
//! The sweep reads and deletes through the anchor schema. A retention alias
//! binds another schema to that anchor, so a TTL on one name deletes the
//! records of every schema the alias chain reaches. This is the guard that
//! runs before an alias or a policy is written.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::manifest_retention::{RetentionPolicyInReach, RetentionReach, RetentionSharing};
use crate::schema::{Schema, SchemaError, SchemaRetentionPolicy};
use std::collections::{BTreeSet, HashMap};

impl SchemaStore {
    /// Every retention alias row: `(manifest name, anchor)`. Reads only the
    /// reserved alias prefix, never product rows.
    pub async fn retention_aliases(&self) -> Result<Vec<(String, String)>, SchemaError> {
        Ok(self
            .schema_states_store
            .scan_items_with_prefix::<SchemaRetentionAnchor>(RETENTION_ANCHOR_KEY_PREFIX)
            .await?
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
    /// points at it, or at a schema bound to it. The reach chain is the anchor
    /// alone.
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

    /// The retention policies whose sweep would delete records of a schema
    /// bound to `anchor`: a policy on the anchor, or on any schema bound to
    /// it. This is the same closure as [`Self::retention_sharing`], so a plain
    /// declare is checked as widely as a declare with a TTL. A policy that
    /// only `binding` itself declared is its own and is not reported.
    pub async fn retention_policies_in_reach(
        &self,
        binding: &str,
        anchor: &str,
    ) -> Result<RetentionReach, SchemaError> {
        let schemas = self.get_all_schemas().await?;
        let aliases = self.retention_aliases().await?;
        let reach = compute_reach(&schemas, &aliases, binding, anchor);
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

/// The schemas a TTL sweep through an anchor reaches: the anchor, and every
/// schema bound to it.
struct Reach {
    /// The anchor alone.
    chain: BTreeSet<String>,
    /// Other schemas bound to the anchor by a retention alias, transitively.
    /// Neither `binding` nor the anchor.
    shared: BTreeSet<String>,
}

fn compute_reach(
    schemas: &HashMap<String, Schema>,
    aliases: &[(String, String)],
    binding: &str,
    anchor: &str,
) -> Reach {
    // The anchor is the schema the sweep reads.
    let chain: BTreeSet<String> = BTreeSet::from([anchor.to_string()]);
    let mut reach = chain.clone();
    reach.insert(binding.to_string());
    let mut shared: BTreeSet<String> = BTreeSet::new();
    loop {
        let mut added = Vec::new();
        for name in schemas.keys() {
            if reach.contains(name) || shared.contains(name) {
                continue;
            }
            let in_reach = |other: &str| reach.contains(other) || shared.contains(other);
            let aliased = aliases
                .iter()
                .any(|(alias, target)| alias == name && in_reach(target));
            if aliased {
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
) -> RetentionSharing {
    let reach = compute_reach(schemas, aliases, binding, anchor);
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
