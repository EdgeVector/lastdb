//! Read-through lookup for schemas committed by another service instance.

use std::collections::hash_map::Entry;

use serde_json::{json, Value};

use super::*;

/// The named-schema result both HTTP routers answer.
#[derive(Debug)]
pub enum SchemaByNameReply {
    /// The named schema exists. HTTP 200.
    ///
    /// The envelope is boxed so the other variants stay small.
    Ready(Box<SchemaEnvelope>),
    /// No schema has that name. HTTP 404.
    ///
    /// `body` is `{ "error": "Schema not found" }`. The actix router still
    /// writes the warn log.
    Missing { body: Value },
    /// Any other failure. HTTP 500.
    ///
    /// `message` is the log line and the error text. `body` is
    /// `{ "error": message }`. The actix router still writes the error log.
    Failed { message: String, body: Value },
}

impl SchemaServiceState {
    /// Resolve public reposts against durable definitions, including expansions.
    /// A cached predecessor cannot establish which canonical is current.
    pub async fn lookup_idempotent_repost(
        &self,
        schema: &Schema,
        _mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<Option<Schema>> {
        if schema
            .descriptive_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            return Ok(None);
        }
        let stored = self.storage.backend().load_all_schemas().await?;
        let mut probe = schema.clone();
        probe.dedup_fields();
        probe.compute_identity_hash();
        let existing = match probe.get_identity_hash().and_then(|id| stored.get(id)) {
            Some(existing) => Some(durable_active_schema(&stored, existing)?.clone()),
            None => stored_repost_by_namespace(&stored, schema)?,
        };
        let Some(existing) = existing else {
            return Ok(None);
        };
        self.validate_loaded_schemas(&HashMap::from([(existing.name.clone(), existing.clone())]))
            .await?;
        // Do not overwrite concurrent cache entries with a snapshot read. The
        // same read-only checks decide against this fresh durable candidate.
        Ok(self.classify_repost_candidate(schema, &existing))
    }

    /// A warm process can lack an identity that another instance acknowledged.
    /// Consult durable definitions on a miss; never replace concurrent cache entries.
    pub async fn lookup_schema_by_name(&self, name: &str) -> FoldDbResult<Option<Schema>> {
        if let Some(schema) = self.get_schema_by_name(name)? {
            return Ok(Some(schema));
        }
        let stored = self.storage.backend().load_all_schemas().await?;
        let schema = match stored.get(name) {
            Some(schema) => Some(schema.clone()),
            None => Self::stored_schema_by_description(&stored, name)?,
        };
        if let Some(schema) = &schema {
            self.validate_loaded_schemas(&HashMap::from([(schema.name.clone(), schema.clone())]))
                .await?;
            let inserted = {
                let mut schemas = write_lock(&self.schemas, "schemas")?;
                if let Entry::Vacant(entry) = schemas.entry(schema.name.clone()) {
                    entry.insert(schema.clone());
                    self.bump_state_version();
                    true
                } else {
                    false
                }
            };
            if inserted {
                self.rebuild_descriptive_name_index();
            }
        }
        Ok(schema)
    }

    /// The same call as [`Self::lookup_schema_by_name`], classified for HTTP.
    ///
    /// `Ready` carries the envelope. `system` follows [`Self::is_system_schema`]
    /// for the stored schema name. An empty lookup stays HTTP 404. Any other
    /// error stays HTTP 500. The lookup method stays public. Each router still
    /// writes its response.
    #[must_use]
    pub async fn schema_by_name_reply(&self, name: &str) -> SchemaByNameReply {
        match self.lookup_schema_by_name(name).await {
            Ok(Some(schema)) => {
                let system = self.is_system_schema(&schema.name);
                SchemaByNameReply::Ready(Box::new(SchemaEnvelope { schema, system }))
            }
            Ok(None) => SchemaByNameReply::Missing {
                body: json!({"error": "Schema not found"}),
            },
            Err(error) => {
                let message = format!("Failed to get schema: {error}");
                SchemaByNameReply::Failed {
                    body: json!({"error": message.clone()}),
                    message,
                }
            }
        }
    }

    fn stored_schema_by_description(
        stored: &HashMap<String, Schema>,
        name: &str,
    ) -> FoldDbResult<Option<Schema>> {
        let mut matches = stored.values().filter(|schema| {
            schema.superseded_by.is_none()
                && schema
                    .descriptive_name
                    .as_deref()
                    .is_some_and(|description| {
                        descriptive_name_key(schema.owner_app_id.as_deref(), description) == name
                    })
        });
        let schema = matches.next().cloned();
        if matches.next().is_some() {
            return Err(FoldDbError::Config(
                "stored schema descriptive name is ambiguous".to_owned(),
            ));
        }
        Ok(schema)
    }
}

/// Owner and descriptive name are distinct principals, not an encoded path.
fn stored_repost_by_namespace(
    stored: &HashMap<String, Schema>,
    proposal: &Schema,
) -> FoldDbResult<Option<Schema>> {
    let owner = proposal.owner_app_id.as_deref().filter(|id| !id.is_empty());
    let mut matches = stored.values().filter(|candidate| {
        candidate.superseded_by.is_none()
            && candidate
                .owner_app_id
                .as_deref()
                .filter(|id| !id.is_empty())
                == owner
            && candidate.descriptive_name == proposal.descriptive_name
            && !crate::state_expansion::is_cross_key_layout(proposal, candidate)
    });
    let candidate = matches.next().cloned();
    if matches.next().is_some() {
        return Err(FoldDbError::Config(
            "stored schema namespace has ambiguous active entries".into(),
        ));
    }
    Ok(candidate)
}

/// Resolve only links in this snapshot; reject broken or cyclic chains.
fn durable_active_schema<'a>(
    stored: &'a HashMap<String, Schema>,
    original: &'a Schema,
) -> FoldDbResult<&'a Schema> {
    let mut current = original;
    let mut visited = HashSet::new();
    while let Some(next) = &current.superseded_by {
        if !visited.insert(current.name.as_str()) {
            return Err(FoldDbError::Config(
                "stored schema supersession cycle".into(),
            ));
        }
        let successor = stored
            .get(next)
            .ok_or_else(|| FoldDbError::Config("stored schema successor is missing".into()))?;
        if current.owner_app_id != successor.owner_app_id
            || current.descriptive_name != successor.descriptive_name
            || crate::state_expansion::is_cross_key_layout(current, successor)
        {
            return Err(FoldDbError::Config(
                "stored schema successor changes namespace or key layout".into(),
            ));
        }
        current = successor;
    }
    Ok(current)
}
