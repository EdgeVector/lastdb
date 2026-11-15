use std::collections::{BTreeSet, HashSet};

use schema_types::DeclarativeSchemaDefinition;

use super::{
    draft, AdviceEvidence, AdviceFinding, AdviceKind, SchemaAdviceRequest, SchemaAdviceResponse,
    SchemaProvenance,
};

/// A currently listed, normalized definition paired with one of its signed listings.
pub struct CatalogSchema<'a> {
    pub definition: &'a DeclarativeSchemaDefinition,
    pub app_id: &'a str,
    pub source: &'a str,
    pub sha: &'a str,
    pub channel: &'a str,
    pub api_version: u32,
}

impl SchemaAdviceResponse {
    /// Add up to three public schema examples using distinctly named matching fields.
    pub fn add_catalog_examples<'a>(
        &mut self,
        request: &SchemaAdviceRequest,
        catalog: impl IntoIterator<Item = CatalogSchema<'a>>,
    ) {
        let Ok(draft) = draft(&request.schema) else {
            return;
        };
        let fields: BTreeSet<_> = draft.fields.iter().map(String::as_str).collect();
        if fields.len() < 2 {
            return;
        }

        let mut matches = Vec::new();
        for row in catalog {
            if request.app_id.as_deref() == Some(row.app_id) {
                continue;
            }
            let Some(id) = row.definition.identity_hash.as_deref() else {
                continue;
            };
            let candidate: BTreeSet<_> = row
                .definition
                .fields
                .iter()
                .flatten()
                .map(String::as_str)
                .collect();
            let shared = fields
                .intersection(&candidate)
                .filter(|&&name| {
                    draft.types.get(name).is_none_or(|expected| {
                        row.definition.field_types.get(name) == Some(expected)
                    })
                })
                .count();
            // Require at least half the draft to match and prefer the
            // strongest overlap relative to the size of both schemas.
            if shared >= 2 && shared * 2 >= fields.len() {
                let score = shared * 1024 / (fields.len() + candidate.len() - shared);
                matches.push((score, shared, id, row));
            }
        }
        matches.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| (&a.3.definition.name, a.2).cmp(&(&b.3.definition.name, b.2)))
        });
        let mut seen = HashSet::new();
        self.findings.extend(
            matches
                .into_iter()
                .filter(|(_, _, id, row)| seen.insert((*id, row.definition.name.as_str())))
                .take(3)
                .map(|(_, shared, id, row)| similar_finding(&row, id, shared, fields.len())),
        );
    }
}

fn similar_finding(
    row: &CatalogSchema<'_>,
    id: &str,
    shared: usize,
    total: usize,
) -> AdviceFinding {
    AdviceFinding {
        kind: AdviceKind::SimilarSchema,
        field: None,
        message: format!(
            "`{}` from `{}` matches {shared} of {total} draft fields.",
            row.definition.name, row.app_id
        ),
        evidence: AdviceEvidence {
            schema: Some(SchemaProvenance {
                app_id: row.app_id.to_owned(),
                schema_name: row.definition.name.clone(),
                identity_hash: id.to_owned(),
                source: row.source.to_owned(),
                sha: row.sha.to_owned(),
                channel: row.channel.to_owned(),
                api_version: row.api_version,
            }),
            ..AdviceEvidence::default()
        },
    }
}
