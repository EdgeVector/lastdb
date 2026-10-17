use super::*;

/// Every returned value belongs to a held canonical path in the existing warm set.
pub struct WarmQueryFields {
    pub fields: FieldResults,
    pub(super) wire: HashMap<String, WireMetadata>,
}

impl WarmQueryFields {
    #[must_use]
    pub fn complete_wire_metadata(&self) -> bool {
        let Some(first) = self.wire.values().next() else {
            return false;
        };
        self.wire.len() == self.fields.len()
            && self.wire.values().all(|entry| {
                entry.conflict_flags == first.conflict_flags && entry.key_form == first.key_form
            })
    }

    #[must_use]
    pub fn conflict_flags(&self) -> Option<&str> {
        self.wire
            .values()
            .next()
            .map(|entry| entry.conflict_flags.as_str())
    }

    #[must_use]
    pub fn key_form(&self) -> Option<&str> {
        self.wire
            .values()
            .next()
            .and_then(|entry| entry.key_form.as_deref())
    }

    #[must_use]
    pub fn field_flags(&self) -> HashMap<String, serde_json::Value> {
        self.wire
            .iter()
            .filter_map(|(field, entry)| {
                entry
                    .field_flag
                    .as_ref()
                    .map(|flag| (field.clone(), flag.clone()))
            })
            .collect()
    }
}

/// Exact positive current-key reads can bypass all storage metadata work.
pub fn warm_wire_query(
    runtime: Option<Arc<PackRuntime>>,
    query: &Query,
    access: &AccessContext,
) -> StorageResult<Option<WarmQueryFields>> {
    Ok(lookup(runtime, query, Some(access))?.filter(WarmQueryFields::complete_wire_metadata))
}

/// Conflict flags stay with each selected field, never as an independent response.
pub fn remember_wire_query(
    runtime: Option<Arc<PackRuntime>>,
    query: &Query,
    access: &AccessContext,
    payload: &serde_json::Value,
) -> StorageResult<()> {
    if !access.is_owner {
        return Ok(());
    }
    let Some(key) = key(query) else { return Ok(()) };
    let Some(runtime) = runtime else {
        return Ok(());
    };
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let query_alias = query_alias(query, Some(access))?;
    let Some(rows) = payload.get("results").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    if rows.len() != 1 || rows[0].get("key") != serde_json::to_value(&key).ok().as_ref() {
        return Ok(());
    }
    let Some(flags) = payload
        .get("conflict_flags")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime
        .logical
        .lock()
        .expect("native complete query field metadata");
    let Some(fields) = rows[0].get("fields").and_then(serde_json::Value::as_object) else {
        return Ok(());
    };
    let mut ids = BTreeMap::new();
    for field in fields.keys() {
        let Some(metadata) = rows[0].get("metadata").and_then(|m| m.get(field)) else {
            continue;
        };
        let Some(atom) = metadata
            .get("atom_uuid")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Some(id) = context
            .paths
            .lock()
            .expect("native resolved paths")
            .one(field, &key, atom)
        else {
            continue;
        };
        let Some(Some(bytes)) = logical.fetched_record(PATH_COLLECTION, &id) else {
            continue;
        };
        let mut body: PathBody = serde_json::from_slice(&bytes).map_err(|e| serialization(&e))?;
        if body.provenance.as_ref().map(|(version, _)| *version) != Some(context.version) {
            continue;
        }
        body.wire = Some(WireMetadata {
            query: query_alias.clone(),
            conflict_flags: flags.to_string(),
            key_form: payload
                .get("key_form")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            field_flag: metadata.get("has_conflicts").cloned(),
        });
        let bytes = serde_json::to_vec(&body).map_err(|e| serialization(&e))?;
        call.account_memory_record(id.len(), bytes.len())
            .map_err(map)?;
        let epoch = logical.record_epoch(&id);
        if logical.admit_record(PATH_COLLECTION, &id, Some(bytes), epoch) {
            ids.insert(field.clone(), id);
        }
    }
    if ids.len() == fields.len() {
        projection::save(
            &context,
            &mut call,
            &mut logical,
            &query_alias,
            &query.fields,
            &ids,
        )?;
    }
    Ok(())
}
