//! Bounded repair for sparse HashRange key-field molecules.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::FoldDB;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;

const REPAIR_BATCH_ROWS: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HashRangeKeyFieldRepairReport {
    pub schema: String,
    pub api_hash: String,
    pub hash_field: String,
    pub range_field: String,
    pub dry_run: bool,
    pub member_rows_before: u64,
    pub hash_rows_before: u64,
    pub range_rows_before: u64,
    pub rows_planned: u64,
    pub rows_written: u64,
    pub member_rows_after: u64,
    pub hash_rows_after: u64,
    pub range_rows_after: u64,
}

struct PartitionKeySets {
    members: HashSet<KeyValue>,
    hash_keys: HashSet<KeyValue>,
    range_keys: HashSet<KeyValue>,
}

impl FoldDB {
    /// Plan or execute the key-field repair for one named HashRange partition.
    ///
    /// The declared range-field molecule is the member list. The method never
    /// walks another schema or hash partition. Execute routes missing members
    /// through the normal mutation path, where both declared key fields join
    /// the existing atomic field batch.
    pub async fn repair_hashrange_key_fields(
        &self,
        schema_name: &str,
        api_hash: &str,
        execute: bool,
    ) -> Result<HashRangeKeyFieldRepairReport, SchemaError> {
        if schema_name.trim().is_empty() || api_hash.is_empty() {
            return Err(SchemaError::InvalidData(
                "schema and api_hash are required for HashRange key-field repair".to_string(),
            ));
        }
        let mut schema = self
            .schema_manager
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        if !matches!(schema.schema_type, DeclarativeSchemaType::HashRange) {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{schema_name}' is not HashRange"
            )));
        }
        if schema.runtime_fields.is_empty() {
            schema.populate_runtime_fields()?;
        }
        let key = schema.key.as_ref().ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "HashRange schema '{schema_name}' has no key configuration"
            ))
        })?;
        let hash_field = key.hash_field.clone().ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "HashRange schema '{schema_name}' has no declared hash field"
            ))
        })?;
        let range_field = key.range_field.clone().ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "HashRange schema '{schema_name}' has no declared range field"
            ))
        })?;

        let before = self
            .hashrange_partition_key_sets(&schema, &hash_field, &range_field, api_hash)
            .await?;
        let mut planned: Vec<KeyValue> = before
            .members
            .iter()
            .filter(|member| {
                !before.hash_keys.contains(*member) || !before.range_keys.contains(*member)
            })
            .cloned()
            .collect();
        planned.sort_unstable_by(KeyValue::cmp_page_order);

        let rows_planned = planned.len() as u64;
        let mut rows_written = 0_u64;
        if execute {
            for page in planned.chunks(REPAIR_BATCH_ROWS) {
                let mutations: Vec<Mutation> = page
                    .iter()
                    .map(|key_value| {
                        let mut mutation = Mutation::new(
                            schema_name.to_string(),
                            HashMap::new(),
                            key_value.clone(),
                            "lastdb-hashrange-key-field-repair".to_string(),
                            MutationType::Update,
                        );
                        mutation.synchronous = Some(true);
                        mutation
                    })
                    .collect();
                self.mutation_manager
                    .write_mutations_batch_with_receipt(mutations, None)
                    .await?;
                rows_written = rows_written.saturating_add(page.len() as u64);
            }
        }

        let after = if execute {
            self.hashrange_partition_key_sets(&schema, &hash_field, &range_field, api_hash)
                .await?
        } else {
            PartitionKeySets {
                members: before.members.clone(),
                hash_keys: before.hash_keys.clone(),
                range_keys: before.range_keys.clone(),
            }
        };
        Ok(HashRangeKeyFieldRepairReport {
            schema: schema_name.to_string(),
            api_hash: api_hash.to_string(),
            hash_field,
            range_field,
            dry_run: !execute,
            member_rows_before: before.members.len() as u64,
            hash_rows_before: before.hash_keys.len() as u64,
            range_rows_before: before.range_keys.len() as u64,
            rows_planned,
            rows_written,
            member_rows_after: after.members.len() as u64,
            hash_rows_after: after.hash_keys.len() as u64,
            range_rows_after: after.range_keys.len() as u64,
        })
    }

    async fn hashrange_partition_key_sets(
        &self,
        schema: &crate::schema::types::Schema,
        hash_field: &str,
        range_field: &str,
        api_hash: &str,
    ) -> Result<PartitionKeySets, SchemaError> {
        let filter = Some(HashRangeFilter::HashKey(api_hash.to_string()));
        let mut member_field =
            schema
                .runtime_fields
                .get(range_field)
                .cloned()
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "HashRange range field '{range_field}' is absent from schema '{}'",
                        schema.name
                    ))
                })?;
        let members: HashSet<KeyValue> = member_field
            .collect_matches(&self.db_ops, filter.clone(), None, false)
            .await?
            .into_iter()
            .map(|matched| matched.0)
            .collect();

        let mut hash_key_field =
            schema
                .runtime_fields
                .get(hash_field)
                .cloned()
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "HashRange hash field '{hash_field}' is absent from schema '{}'",
                        schema.name
                    ))
                })?;
        let hash_keys = hash_key_field
            .collect_matches(&self.db_ops, filter, None, false)
            .await?
            .into_iter()
            .map(|matched| matched.0)
            .collect();

        Ok(PartitionKeySets {
            range_keys: members.clone(),
            members,
            hash_keys,
        })
    }
}
