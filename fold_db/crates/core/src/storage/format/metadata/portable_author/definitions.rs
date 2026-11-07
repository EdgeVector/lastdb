//! Verified shared schema definitions by local alias. They live beside the
//! shared records, so a snapshot carries the definition of every shared alias.

use super::{invalid, EncodedMetadata, LocalAuthor, Metadata, DEFINITION_PREFIX};
use crate::storage::error::StorageResult;
use lastdb_model::portable_mutation::Stamp;
use schema_types::PortableSchemaDefinition;
use std::collections::BTreeMap;

impl Metadata {
    /// The recorded definition of each alias in the shared scope.
    pub(crate) async fn portable_definitions(
        &self,
    ) -> StorageResult<BTreeMap<String, PortableSchemaDefinition>> {
        let aliases = self.runtime.shared_schema_aliases();
        let rows = self.definition_rows(&aliases).await?;
        aliases
            .into_iter()
            .zip(rows)
            .filter_map(|(alias, row)| row.map(|bytes| (alias, bytes)))
            .map(|(alias, bytes)| {
                Ok((
                    alias,
                    serde_json::from_slice::<PortableSchemaDefinition>(&bytes).map_err(invalid)?,
                ))
            })
            .collect()
    }

    /// Record definitions the caller verified against the installed schemas.
    /// A recorded alias keeps its definition.
    pub(crate) async fn stage_portable_definitions(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let aliases: Vec<_> = definitions.keys().cloned().collect();
        let rows = self.definition_rows(&aliases).await?;
        let mut staged = Vec::new();
        for ((alias, definition), row) in definitions.iter().zip(rows) {
            match row {
                Some(bytes) => {
                    let recorded: PortableSchemaDefinition =
                        serde_json::from_slice(&bytes).map_err(invalid)?;
                    if recorded.id() != definition.id() {
                        return Err(invalid("a shared alias already has another definition"));
                    }
                }
                None => staged.push(EncodedMetadata {
                    key: definition_key(alias),
                    bytes: encode(definition)?,
                }),
            }
        }
        if staged.is_empty() {
            return Ok(());
        }
        self.stage_batch(staged).await
    }

    /// The recorded definition of each admitted schema that this batch uses
    /// before its stream has defined it, in alias order.
    pub(super) async fn first_definitions(
        &self,
        state: &LocalAuthor,
        schemas: &[(&str, Option<Stamp>)],
    ) -> StorageResult<Vec<PortableSchemaDefinition>> {
        let Some(shared) = &state.shared else {
            return Ok(Vec::new());
        };
        let pending: BTreeMap<String, &str> = schemas
            .iter()
            .filter(|(_, imported)| imported.is_none())
            .filter_map(|(alias, _)| {
                let bound = shared.schemas.get(*alias)?;
                (!shared.defined.contains(&bound.id)).then(|| ((*alias).to_owned(), &*bound.id))
            })
            .collect();
        if pending.is_empty() {
            return Ok(Vec::new());
        }
        let aliases: Vec<_> = pending.keys().cloned().collect();
        let rows = self.definition_rows(&aliases).await?;
        pending
            .values()
            .zip(rows)
            .map(|(id, row)| {
                let bytes =
                    row.ok_or_else(|| invalid("a bound shared schema lacks its definition"))?;
                let definition: PortableSchemaDefinition =
                    serde_json::from_slice(&bytes).map_err(invalid)?;
                if definition.id().as_str() != *id {
                    return Err(invalid(
                        "a recorded definition differs from the writer binding",
                    ));
                }
                Ok(definition)
            })
            .collect()
    }

    async fn definition_rows(&self, aliases: &[String]) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let rows = self
            .metadata_store
            .get_many(
                aliases
                    .iter()
                    .map(|alias| definition_key(alias).into_bytes())
                    .collect(),
            )
            .await?;
        if rows.len() != aliases.len() {
            return Err(invalid("incomplete shared schema definition read"));
        }
        Ok(rows)
    }
}

fn definition_key(alias: &str) -> String {
    format!("{DEFINITION_PREFIX}{alias}")
}

/// Canonical bytes: the definition holds hash maps, and a JSON value orders keys.
fn encode(definition: &PortableSchemaDefinition) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(&serde_json::to_value(definition).map_err(invalid)?).map_err(invalid)
}
