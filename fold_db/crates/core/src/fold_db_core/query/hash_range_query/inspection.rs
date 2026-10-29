//! Administrative inspection proves a bounded logical set before its window.

use std::collections::{HashMap, HashSet};

use crate::schema::types::field::{
    fetch_atoms_with_key_metadata_async_with_prefix, FieldValue, FieldVariant, HashRangeFilter,
    KeyedAtomMatch, SAMPLE_PEEK_CAP,
};
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};

use super::{
    cokey_primary::CokeyPrimaryPage,
    field_rules,
    inspection_rules::{self, inspection_limit, INSPECTION_KEY_CAP},
    HashRangeQueryProcessor, KeySource,
};

struct InspectedField {
    field_name: String,
    values: HashMap<KeyValue, FieldValue>,
    sources: HashMap<KeyValue, KeySource>,
    had_spine: bool,
    complete: bool,
}

impl HashRangeQueryProcessor {
    /// Recover API coordinates and remove other layouts before any result
    /// limit. Pages require a complete bounded set. Samples can stop after
    /// enough valid members; keyed reads retain their narrower body window.
    pub(super) async fn resolve_inspection_primary_page(
        &self,
        schema: &mut Schema,
        selected: &[String],
        filter: Option<&HashRangeFilter>,
        include_tombstones: bool,
        received: &[String],
    ) -> Result<CokeyPrimaryPage, SchemaError> {
        let sample_size = match filter {
            Some(HashRangeFilter::SampleN(n)) => Some((*n).min(SAMPLE_PEEK_CAP)),
            _ => None,
        };
        if sample_size == Some(0) {
            return Ok(CokeyPrimaryPage {
                primary_field: field_rules::primary_field(schema, selected),
                values: HashMap::new(),
                page_keys: Vec::new(),
                key_sources: HashMap::new(),
                layout_values: HashMap::new(),
            });
        }
        let mut candidate_cap = sample_size.unwrap_or(INSPECTION_KEY_CAP);
        let mut examined = 0usize;
        let mut inspected = loop {
            let inspected = self
                .inspection_candidates(
                    schema,
                    selected,
                    include_tombstones,
                    received,
                    candidate_cap,
                    &mut examined,
                )
                .await?;
            if sample_size.is_none() && !inspected.complete {
                return Err(inspection_limit());
            }
            if inspected.complete || sample_size.is_some_and(|n| inspected.values.len() >= n) {
                break inspected;
            }
            candidate_cap = inspection_rules::next_candidate_cap(candidate_cap, examined)?;
        };
        let keys = inspected.values.keys().cloned().collect();
        let page_keys = inspection_rules::page_keys(keys, filter)?;
        let kept: HashSet<_> = page_keys.iter().cloned().collect();
        inspected.values.retain(|key, _| kept.contains(key));
        inspected.sources.retain(|key, _| kept.contains(key));
        Ok(CokeyPrimaryPage {
            primary_field: inspected.field_name,
            values: inspected.values,
            page_keys,
            key_sources: inspected.sources,
            layout_values: HashMap::new(),
        })
    }

    async fn inspection_candidates(
        &self,
        schema: &mut Schema,
        selected: &[String],
        include_tombstones: bool,
        received: &[String],
        candidate_cap: usize,
        examined: &mut usize,
    ) -> Result<InspectedField, SchemaError> {
        let mut inspected = self
            .inspection_field(
                schema,
                field_rules::primary_field(schema, selected),
                include_tombstones,
                received,
                candidate_cap,
                examined,
            )
            .await?;
        if !inspected.had_spine {
            if let Some(fallback) = field_rules::fallback_field(selected, &inspected.field_name) {
                inspected = self
                    .inspection_field(
                        schema,
                        fallback.to_string(),
                        include_tombstones,
                        received,
                        candidate_cap,
                        examined,
                    )
                    .await?;
            }
        }

        let named_primary = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_deref())
            == Some(inspected.field_name.as_str());
        if named_primary {
            self.retain_named_layout(
                schema,
                &inspected.field_name,
                &mut inspected.values,
                &inspected.sources,
                include_tombstones,
            )
            .await?;
        }
        inspected
            .sources
            .retain(|key, _| inspected.values.contains_key(key));

        // Re-enter the existing exact core route only after both coordinates
        // verify. It admits logical API keys and retains the selected scope.
        let keys: Vec<_> = inspected.values.keys().cloned().collect();
        let pending = self
            .collect_cokey_secondary_pending(
                schema,
                vec![inspected.field_name.clone()],
                &keys,
                &inspected.sources,
                include_tombstones,
                None,
            )
            .await?;
        let mut fields = HashMap::new();
        self.fill_cokey_secondaries(schema, pending, include_tombstones, &mut fields)
            .await?;
        inspected.values = fields.remove(&inspected.field_name).unwrap_or_default();
        inspected
            .sources
            .retain(|key, _| inspected.values.contains_key(key));
        Ok(inspected)
    }

    /// Candidate limits apply to the sum of examined scopes, before bodies.
    /// Merge physical identity before recovery so personal precedence survives
    /// the layout check. A failed read or an exceeded allowance is an error.
    async fn inspection_field(
        &self,
        schema: &Schema,
        name: String,
        include_tombstones: bool,
        received: &[String],
        candidate_cap: usize,
        examined: &mut usize,
    ) -> Result<InspectedField, SchemaError> {
        let field = schema.runtime_fields.get(&name).ok_or_else(|| {
            SchemaError::InvalidField(format!("inspection field '{name}' is missing"))
        })?;
        let binding =
            crate::storage::disk_pack::logical_paths::field::FieldScope::new(schema, &name)?;
        let own = field.common().storage_prefix().map(str::to_owned);
        let mut scopes = vec![own];
        for namespace in received {
            let scope = Some(namespace.clone());
            if !scopes.contains(&scope) {
                scopes.push(scope);
            }
        }
        let mut complete = true;
        let mut selected_count = 0usize;
        let mut candidates: HashMap<KeyValue, (KeySource, KeyedAtomMatch)> = HashMap::new();
        for scope in scopes {
            let mut working = field.cloned_without_molecule();
            working.common_mut().set_storage_prefix(scope.clone());
            let scope_cap = candidate_cap.saturating_sub(selected_count);
            let mut matches = binding
                .run(working.collect_matches(
                    &self.db_ops,
                    Some(HashRangeFilter::Page {
                        offset: 0,
                        limit: scope_cap.saturating_add(1),
                    }),
                    include_tombstones,
                ))
                .await?;
            *examined = examined.saturating_add(matches.len());
            if *examined > INSPECTION_KEY_CAP {
                return Err(inspection_limit());
            }
            complete = matches.len() <= scope_cap;
            matches.truncate(scope_cap);
            selected_count = selected_count.saturating_add(matches.len());
            for entry in matches {
                candidates
                    .entry(entry.0.clone())
                    .or_insert((scope.clone(), entry));
            }
            if !complete {
                break;
            }
        }
        let had_spine = !candidates.is_empty();
        let mut by_scope: HashMap<KeySource, Vec<KeyedAtomMatch>> = HashMap::new();
        for (_, (scope, entry)) in candidates {
            by_scope.entry(scope).or_default().push(entry);
        }
        let mut values = HashMap::new();
        let mut sources = HashMap::new();
        for (scope, matches) in by_scope {
            // Physical coordinates are not logical warm-set identities. Body
            // lookup runs outside FieldScope; only verified API keys proceed.
            let mut resolved = fetch_atoms_with_key_metadata_async_with_prefix(
                &self.db_ops,
                matches,
                scope.as_deref(),
                field.common().molecule_uuid().map(String::as_str),
            )
            .await?;
            field.stamp_molecule_meta(&mut resolved);
            if scope
                .as_deref()
                .is_some_and(|scope| scope.starts_with("from:"))
            {
                Self::stamp_shared_writer(scope.as_deref().unwrap(), &mut resolved);
            }
            for (storage_key, value) in resolved {
                if !include_tombstones && crate::atom::is_tombstone_value(&value.value) {
                    continue;
                }
                if let Some(api_key) =
                    self.inspection_api_key(schema, &name, field, storage_key, &value)?
                {
                    sources.insert(api_key.clone(), scope.clone());
                    values.insert(api_key, value);
                }
            }
        }
        Ok(InspectedField {
            field_name: name,
            values,
            sources,
            had_spine,
            complete,
        })
    }

    /// Only the current codec and declared hash value can recover a blind hash.
    /// A nonmatching declared value belongs to another layout, never an opaque
    /// response row. A sparse projection cannot prove a blind API coordinate.
    fn inspection_api_key(
        &self,
        schema: &Schema,
        name: &str,
        field: &FieldVariant,
        storage_key: KeyValue,
        value: &FieldValue,
    ) -> Result<Option<KeyValue>, SchemaError> {
        let declared = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_deref())
            == Some(name);
        if !declared {
            return Err(SchemaError::InvalidData("administrative projection fallback cannot recover the API hash; query one hash or an exact key batch".into()));
        }
        let Some(hash) = scalar_key_coordinate(&value.value) else {
            return Ok(None);
        };
        let molecule = field.common().molecule_uuid().ok_or_else(|| {
            SchemaError::InvalidData("inspection field has no molecule identity".into())
        })?;
        if self.db_ops.atoms().storage_hash(molecule, &hash)?.as_str()
            != storage_key.hash.as_deref().unwrap_or_default()
        {
            return Ok(None);
        }
        Ok(Some(KeyValue::new(
            Some(hash.into_owned()),
            storage_key.range,
        )))
    }
}
