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
        let edges = self
            .db_ops
            .atoms()
            .molecule_ref_edges_for_molecule(molecule, scope)
            .await?;
        if !edges.complete {
            return Ok(None);
        }
        if edges
            .edges
            .iter()
            .any(|edge| !matches!(edge.edge_type.as_str(), "schema-field" | "protein-member"))
        {
            return Ok(None);
        }
        let sources = edges
            .edges
            .iter()
            .filter(|edge| edge.edge_type == "schema-field");
        let resolved = futures::future::try_join_all(
            sources
                .map(|edge| async move { self.resolve_path_source(&edge.source, molecule).await }),
        )
        .await?;
        for binding in resolved {
            let Some(binding) = binding else {
                return Ok(None);
            };
            if !bindings.contains(&binding) {
                bindings.push(binding);
                if bindings.len() > 10_000 {
                    return Ok(None);
                }
            }
        }
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
    if schema.runtime_fields.len() > 10_000 {
        return Ok(Vec::new());
    }
    let mut bindings = Vec::new();
    for (name, field) in &schema.runtime_fields {
        if field.common().molecule_uuid().map(String::as_str) != Some(molecule) {
            continue;
        }
        let Some(identity) = schema.field_hashes.get(name).filter(|id| !id.is_empty()) else {
            return Ok(Vec::new());
        };
        bindings.push(Binding {
            schema: schema_identity(schema)?,
            field: identity.clone(),
            name: name.clone(),
        });
    }
    Ok(bindings)
}
