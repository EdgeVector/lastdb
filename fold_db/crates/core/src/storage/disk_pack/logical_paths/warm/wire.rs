use super::*;

/// Every returned value belongs to a held canonical path in the existing warm set.
pub struct WarmQueryFields {
    pub fields: FieldResults,
    pub(super) wire: HashMap<String, WireMetadata>,
}

impl WarmQueryFields {
    #[must_use]
    pub fn requested_key_count(query: &Query) -> Option<usize> {
        keys(query).map(|keys| keys.len())
    }

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
    let Some(keys) = keys(query) else {
        return Ok(());
    };
    let Some(runtime) = runtime else {
        return Ok(());
    };
    let Some(context) = ReadContext::captured(&runtime) else {
        return Ok(());
    };
    let Some(rows) = payload.get("results").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    if rows.len() != keys.len() || !complete_page(query, payload, keys.len()) {
        return Ok(());
    }
    let Some(flags) = payload
        .get("conflict_flags")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };
    let mut wanted: std::collections::HashSet<_> = keys.into_iter().collect();
    let mut selected = Vec::new();
    let mut paths = 0usize;
    for row in rows {
        let Some(key) = row
            .get("key")
            .and_then(|key| serde_json::from_value::<KeyValue>(key.clone()).ok())
        else {
            return Ok(());
        };
        if !wanted.remove(&key) {
            return Ok(());
        }
        let Some(fields) = row.get("fields").and_then(serde_json::Value::as_object) else {
            return Ok(());
        };
        paths += fields.len();
        if paths > 10_000 {
            return Ok(());
        }
        selected.push((key, row));
    }
    if !wanted.is_empty() {
        return Ok(());
    }
    let query_alias = query_alias(query, Some(access))?;
    let remember = RememberWire {
        context: &context,
        query,
        query_alias: &query_alias,
        payload,
        flags,
    };
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime
        .logical
        .lock()
        .expect("native complete query field metadata");
    for (key, row) in selected {
        remember.row(&mut call, &mut logical, &key, row)?;
    }
    Ok(())
}

fn complete_page(query: &Query, payload: &serde_json::Value, count: usize) -> bool {
    use crate::schema::types::SortOrder;
    let total = payload.get("total_count");
    payload.get("has_more").and_then(serde_json::Value::as_bool) == Some(false)
        && payload.get("offset").and_then(serde_json::Value::as_u64) == Some(0)
        && payload
            .get("unresolved_rows")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && payload
            .get("tombstoned_rows")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && payload
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|limit| limit >= count as u64)
        && payload
            .get("returned_count")
            .and_then(serde_json::Value::as_u64)
            == Some(count as u64)
        && payload.get("next_cursor") == Some(&serde_json::Value::Null)
        && payload.get("key_form").is_some()
        && if matches!(query.sort_order, Some(SortOrder::Desc)) {
            total.and_then(serde_json::Value::as_u64) == Some(count as u64)
        } else {
            total == Some(&serde_json::Value::Null)
        }
}

struct RememberWire<'a> {
    context: &'a ReadContext,
    query: &'a Query,
    query_alias: &'a str,
    payload: &'a serde_json::Value,
    flags: &'a str,
}

impl RememberWire<'_> {
    fn row(
        &self,
        call: &mut laststore::disk_pack::home::HomeRead,
        logical: &mut LogicalResidentSet,
        key: &KeyValue,
        row: &serde_json::Value,
    ) -> StorageResult<()> {
        let fields = row
            .get("fields")
            .and_then(serde_json::Value::as_object)
            .expect("checked wire fields");
        let mut ids = BTreeMap::new();
        for field in fields.keys() {
            let Some(metadata) = row.get("metadata").and_then(|m| m.get(field)) else {
                continue;
            };
            let Some(atom) = metadata
                .get("atom_uuid")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(id) = self
                .context
                .paths
                .lock()
                .expect("native resolved paths")
                .one(field, key, atom)
            else {
                continue;
            };
            let Some(mut body) =
                projection::owned_body(self.context, call, logical, PATH_COLLECTION, &id)?
            else {
                continue;
            };
            if !body.valid(self.context) {
                continue;
            }
            body.wire = Some(WireMetadata {
                query: self.query_alias.to_string(),
                version: self.context.version,
                witness: self.context.witness(),
                conflict_flags: self.flags.to_string(),
                key_form: self
                    .payload
                    .get("key_form")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                field_flag: metadata.get("has_conflicts").cloned(),
            });
            let Some(bytes) = projection::encode_optional(call, &id, &body)? else {
                return Ok(());
            };
            let epoch = logical.record_epoch(&id);
            if logical.admit_record(PATH_COLLECTION, &id, Some(bytes), epoch) {
                ids.insert(field.clone(), id);
            }
        }
        if ids.len() == fields.len() {
            projection::save(
                self.context,
                call,
                logical,
                self.query_alias,
                key,
                &self.query.fields,
                &ids,
            )?;
        }
        Ok(())
    }
}
