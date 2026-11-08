//! Verified shared schema definitions by local alias. They live beside the
//! shared records, so a snapshot carries the definition of every shared alias.

use super::outgoing::entry_key;
use super::{
    invalid, EncodedMetadata, JournalSchema, LocalAuthor, Metadata, DEFINITION_PREFIX,
    VERSIONS_PREFIX,
};
use crate::storage::error::StorageResult;
use lastdb_model::portable_mutation::{
    MutationEntry, MutationOperation, SchemaIdentity, Stamp, StreamId,
};
use schema_types::{PortableSchemaDefinition, PortableSchemaId};
use std::collections::{BTreeMap, BTreeSet};

/// Every definition one shared name accepted, and the one it holds now
/// (`decision-2026-11-07-lastdb-schema-name-slot-definition-per-record`).
/// The accepted definitions include the current one. A record keeps the ID
/// of the definition it was written with, and any accepted ID binds to the
/// name's schema.
pub(crate) struct DefinitionSet {
    pub(crate) current: PortableSchemaDefinition,
    pub(crate) accepted: BTreeMap<PortableSchemaId, PortableSchemaDefinition>,
}

impl DefinitionSet {
    pub(crate) fn new(
        current: PortableSchemaDefinition,
        accepted: impl IntoIterator<Item = PortableSchemaDefinition>,
    ) -> Self {
        let mut accepted: BTreeMap<_, _> = accepted
            .into_iter()
            .map(|definition| (definition.id().clone(), definition))
            .collect();
        accepted.insert(current.id().clone(), current.clone());
        Self { current, accepted }
    }
}

impl Metadata {
    /// The definitions of each alias in the shared scope, in one read. A name
    /// that accepted one definition has no versions row.
    pub(crate) async fn portable_definition_sets(
        &self,
    ) -> StorageResult<BTreeMap<String, DefinitionSet>> {
        let aliases = self.runtime.shared_schema_aliases();
        let keys = aliases
            .iter()
            .flat_map(|alias| [definition_key(alias), versions_key(alias)])
            .map(String::into_bytes)
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() != aliases.len() * 2 {
            return Err(invalid("incomplete shared schema definition read"));
        }
        let mut sets = BTreeMap::new();
        for (alias, pair) in aliases.into_iter().zip(rows.chunks_exact(2)) {
            let Some(current) = &pair[0] else {
                continue;
            };
            let current = serde_json::from_slice(current).map_err(invalid)?;
            let accepted: Vec<PortableSchemaDefinition> = match &pair[1] {
                Some(bytes) => serde_json::from_slice(bytes).map_err(invalid)?,
                None => Vec::new(),
            };
            sets.insert(alias, DefinitionSet::new(current, accepted));
        }
        Ok(sets)
    }

    /// Record the current and accepted definitions the caller verified.
    pub(crate) async fn stage_definition_sets(
        &self,
        sets: &BTreeMap<String, DefinitionSet>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let mut rows = Vec::with_capacity(sets.len() * 2);
        for (alias, set) in sets {
            rows.push(EncodedMetadata {
                key: definition_key(alias),
                bytes: encode(&set.current)?,
            });
            if set.accepted.len() > 1 {
                let accepted: Vec<_> = set.accepted.values().collect();
                rows.push(EncodedMetadata {
                    key: versions_key(alias),
                    bytes: encode(&accepted)?,
                });
            }
        }
        if rows.is_empty() {
            return Ok(());
        }
        self.stage_batch(rows).await
    }

    /// The bound writer's schemas by alias. `None` when no writer is bound.
    pub(crate) async fn portable_writer_bindings(
        &self,
    ) -> StorageResult<Option<BTreeMap<String, JournalSchema>>> {
        Ok(self
            .local_author()
            .await?
            .shared
            .map(|shared| shared.schemas))
    }

    /// Bind aliases to the current definitions of their names. The stream
    /// defines each new binding before its next record. A definition it
    /// carried for an earlier binding is no longer one it binds.
    pub(crate) async fn rebind_portable_schemas(
        &self,
        schemas: BTreeMap<String, JournalSchema>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let mut state = self.local_author().await?;
        let shared = state
            .shared
            .as_mut()
            .ok_or_else(|| invalid("shared journal is not bound"))?;
        for (alias, schema) in schemas {
            *shared
                .schemas
                .get_mut(&alias)
                .ok_or_else(|| invalid("a rebound shared alias is not bound"))? = schema;
        }
        let bound: BTreeSet<_> = shared
            .schemas
            .values()
            .map(|schema| schema.id.clone())
            .collect();
        shared.defined.retain(|id| bound.contains(id));
        shared.validate()?;
        self.stage_local_author(&state).await
    }

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

    /// Extend a bound writer. The stream defines each new schema before its first record.
    pub(crate) async fn admit_portable_schemas(
        &self,
        schemas: BTreeMap<String, JournalSchema>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let mut state = self.local_author().await?;
        let shared = state
            .shared
            .as_mut()
            .ok_or_else(|| invalid("shared journal is not bound"))?;
        let mut added = false;
        for (alias, schema) in schemas {
            match shared.schemas.get(&alias) {
                Some(bound) if *bound == schema => continue,
                Some(_) => return Err(invalid("a bound shared alias cannot change")),
                None => {}
            }
            shared.schemas.insert(alias, schema);
            added = true;
        }
        if !added {
            return Ok(());
        }
        shared.validate()?;
        self.stage_local_author(&state).await
    }

    /// The bound alias whose name accepted the definition `id`.
    pub(super) async fn accepting_alias(
        &self,
        bound: &BTreeMap<String, JournalSchema>,
        id: &str,
    ) -> StorageResult<String> {
        let id: PortableSchemaId = id.parse().map_err(invalid)?;
        self.portable_definition_sets()
            .await?
            .into_iter()
            .find(|(alias, set)| bound.contains_key(alias) && set.accepted.contains_key(&id))
            .map(|(alias, _)| alias)
            .ok_or_else(|| invalid("outgoing schema has no admitted binding"))
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

/// The definition entry that precedes a schema's first record in a stream.
pub(super) fn define_row(
    stream: &StreamId,
    stamp: Stamp,
    definition: &PortableSchemaDefinition,
) -> StorageResult<EncodedMetadata> {
    let entry = MutationEntry {
        stamp,
        schema: SchemaIdentity::Shared(definition.id().as_str().to_owned()),
        operation: MutationOperation::DefineSchema {
            definition: Box::new(definition.clone()),
        },
    };
    entry.validate().map_err(invalid)?;
    Ok(EncodedMetadata {
        key: entry_key(stream, entry.stamp.position.sequence),
        bytes: entry.encode().map_err(invalid)?,
    })
}

fn definition_key(alias: &str) -> String {
    format!("{DEFINITION_PREFIX}{alias}")
}

fn versions_key(alias: &str) -> String {
    format!("{VERSIONS_PREFIX}{alias}")
}

/// Canonical bytes: a definition holds hash maps, and a JSON value orders keys.
fn encode(definition: &impl serde::Serialize) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(&serde_json::to_value(definition).map_err(invalid)?).map_err(invalid)
}
