//! Resolve canonical schema-field aliases for one exact storage scope.

use super::*;

impl MutationManager {
    pub(super) async fn path_bindings(
        &self,
        schema: &crate::schema::types::Schema,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<Option<Vec<Binding>>, SchemaError> {
        let mut bindings = schema_bindings(schema, molecule)?;
        if bindings.is_empty() && schema.molecule_uuid.as_deref() == Some(molecule) {
            return Ok(None);
        }
        let edges = self
            .db_ops
            .atoms()
            .molecule_ref_edges_for_molecule(molecule, scope)
            .await?;
        if !edges.complete {
            return Ok(None);
        }
        if edges.edges.iter().any(|edge| {
            !matches!(
                edge.edge_type.as_str(),
                "schema-field" | "schema-record" | "protein-member"
            )
        }) {
            return Ok(None);
        }
        let sources = edges
            .edges
            .iter()
            .filter(|edge| matches!(edge.edge_type.as_str(), "schema-field" | "schema-record"));
        let resolved = futures::future::try_join_all(sources.map(|edge| async move {
            if edge.edge_type == "schema-record" {
                self.resolve_record_source(&edge.source, molecule).await
            } else {
                Ok(self
                    .resolve_path_source(&edge.source, molecule)
                    .await?
                    .map(|binding| vec![binding]))
            }
        }))
        .await?;
        for resolved in resolved {
            let Some(resolved) = resolved else {
                return Ok(None);
            };
            for binding in resolved {
                if !bindings.contains(&binding) {
                    bindings.push(binding);
                    if bindings.len() > 10_000 {
                        return Ok(None);
                    }
                }
            }
        }
        Ok((!bindings.is_empty()).then_some(bindings))
    }

    async fn resolve_record_source(
        &self,
        source: &str,
        molecule: &str,
    ) -> Result<Option<Vec<Binding>>, SchemaError> {
        let Some(name) = source
            .strip_prefix("schema:")
            .and_then(|source| source.strip_suffix(":record"))
        else {
            return Ok(None);
        };
        let snapshot = self.db_ops.schemas().read_snapshot(name).await?;
        let Some(schema) = snapshot
            .schema
            .filter(|schema| schema.molecule_uuid.as_deref() == Some(molecule))
        else {
            return Ok(None);
        };
        let bindings = schema_bindings(&schema, molecule)?;
        Ok((!bindings.is_empty()).then_some(bindings))
    }

    async fn resolve_path_source(
        &self,
        source: &str,
        molecule: &str,
    ) -> Result<Option<Binding>, SchemaError> {
        let Some(source) = source.strip_prefix("schema:") else {
            return Ok(None);
        };
        let candidates: Vec<_> = source
            .match_indices(":field:")
            .map(|(at, _)| (&source[..at], &source[at + 7..]))
            .collect();
        if candidates.len() > 64 {
            return Ok(None);
        }
        let values = futures::future::try_join_all(candidates.into_iter().map(
            |(schema, field)| async move {
                let snapshot = self.db_ops.schemas().read_snapshot(schema).await?;
                let Some(schema) = snapshot.schema else {
                    return Ok::<_, SchemaError>(None);
                };
                Ok(schema_bindings(&schema, molecule)?
                    .into_iter()
                    .find(|binding| binding.name == field))
            },
        ))
        .await?;
        let mut found = values.into_iter().flatten();
        let Some(binding) = found.next() else {
            return Ok(None);
        };
        Ok(found.next().is_none().then_some(binding))
    }
}

pub(super) fn schema_bindings(
    schema: &crate::schema::types::Schema,
    molecule: &str,
) -> Result<Vec<Binding>, SchemaError> {
    let envelope = schema.molecule_uuid.as_deref() == Some(molecule);
    if schema.runtime_fields.len() > 10_000 {
        return Ok(Vec::new());
    }
    let mut bindings = Vec::new();
    if envelope {
        let name = crate::schema::types::RECORD_SENTINEL;
        bindings.push(Binding {
            schema: schema_identity(schema)?,
            field: schema
                .field_hashes
                .get(name)
                .cloned()
                .unwrap_or_else(|| molecule.to_owned()),
            name: name.to_owned(),
            envelope: false,
        });
    }
    for (name, field) in &schema.runtime_fields {
        if crate::record_molecule::is_record_molecule_field(name) {
            continue;
        }
        if !envelope && field.common().molecule_uuid().map(String::as_str) != Some(molecule) {
            continue;
        }
        let Some(identity) = schema.field_hashes.get(name).filter(|id| !id.is_empty()) else {
            return Ok(Vec::new());
        };
        bindings.push(Binding {
            schema: schema_identity(schema)?,
            field: identity.clone(),
            name: name.clone(),
            envelope,
        });
    }
    Ok(bindings)
}
