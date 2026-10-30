//! App membership is explicit. Shared legacy key fields alone do not prove it.

use super::*;
use crate::schema::types::KeyValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaMembership {
    FieldExists {
        field: String,
    },
    Directory {
        schema: String,
        field: String,
        range: Option<String>,
        contains: String,
        expected_keys: u64,
        count_guard: MembershipCountGuard,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCountGuard {
    pub schema: String,
    pub key: KeyValue,
    pub field: String,
    pub expected: Option<String>,
}

pub(super) fn expected(target: &Target) -> Result<u64, SchemaError> {
    let field = match &target.membership {
        Some(SchemaMembership::Directory { expected_keys, .. }) => return Ok(*expected_keys),
        Some(SchemaMembership::FieldExists { field }) => field.as_str(),
        None => primary(&target.source)?,
    };
    target
        .expected
        .get(field)
        .copied()
        .ok_or_else(|| invalid("membership field is not declared"))
}

pub(super) async fn validate(
    db: &FoldDB,
    source: &Schema,
    membership: Option<&SchemaMembership>,
) -> Result<(), SchemaError> {
    match membership {
        Some(SchemaMembership::FieldExists { field })
            if !source.runtime_fields.contains_key(field) =>
        {
            return Err(invalid("membership field is not declared"))
        }
        Some(SchemaMembership::Directory {
            schema,
            field,
            count_guard,
            ..
        }) => {
            if source
                .key
                .as_ref()
                .is_some_and(|key| key.range_field.is_some())
            {
                return Err(invalid("directory membership requires a hash-only source"));
            }
            let directory = definition(db, schema).await?;
            let counter = definition(db, &count_guard.schema).await?;
            if directory.owner_app_id != source.owner_app_id
                || counter.owner_app_id != source.owner_app_id
                || !directory.runtime_fields.contains_key(field)
                || !counter.runtime_fields.contains_key(&count_guard.field)
            {
                return Err(invalid(
                    "membership directory and count must use the same app's declared fields",
                ));
            }
            let names = BTreeSet::from([count_guard.field.as_str()]);
            let values = db
                .db_ops
                .atoms()
                .schema_fields_at_keys(
                    &counter,
                    std::slice::from_ref(&count_guard.key),
                    None,
                    &names,
                )
                .await?;
            let actual = values
                .first()
                .and_then(|fields| fields.get(&count_guard.field));
            if actual
                != count_guard
                    .expected
                    .as_ref()
                    .map(|text| serde_json::Value::String(text.clone()))
                    .as_ref()
            {
                return Err(invalid(
                    "membership count changed after the app prepared its conversion",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) async fn mask(
    db: &FoldDB,
    target: &Target,
    keys: &[KeyValue],
) -> Result<Vec<bool>, SchemaError> {
    match &target.membership {
        None => Ok(vec![true; keys.len()]),
        Some(SchemaMembership::FieldExists { field }) => {
            let field = &target.source.runtime_fields[field];
            let mut slots = Vec::new();
            for key in keys {
                slots.push(super::tips::slot(db.db_ops.atoms(), field, key)?);
            }
            let rows = db
                .db_ops
                .atoms()
                .load_visible_per_key_records(None, &slots)
                .await?;
            if rows.len() != keys.len() {
                return Err(invalid("membership tip batch is incomplete"));
            }
            Ok(rows.into_iter().map(|row| row.is_some()).collect())
        }
        Some(SchemaMembership::Directory {
            schema,
            field,
            range,
            contains,
            ..
        }) => {
            let directory = definition(db, schema).await?;
            let lookups: Vec<_> = keys
                .iter()
                .map(|key| KeyValue::new(key.hash.clone(), range.clone()))
                .collect();
            let names = BTreeSet::from([field.as_str()]);
            let rows = db
                .db_ops
                .atoms()
                .schema_fields_at_keys(&directory, &lookups, None, &names)
                .await?;
            if rows.len() != keys.len() {
                return Err(invalid("membership directory batch is incomplete"));
            }
            rows.into_iter()
                .map(|row| {
                    let Some(value) = row.get(field) else {
                        return Ok(false);
                    };
                    let text = value
                        .as_str()
                        .ok_or_else(|| invalid("membership directory is not text"))?;
                    let members: Vec<String> = serde_json::from_str(text)
                        .map_err(|_| invalid("membership directory is not a JSON string array"))?;
                    if members.len() > 128 {
                        return Err(invalid("membership directory exceeds 128 kinds"));
                    }
                    Ok(members.contains(contains))
                })
                .collect()
        }
    }
}

async fn definition(db: &FoldDB, name: &str) -> Result<Schema, SchemaError> {
    let mut schema = db
        .db_ops
        .get_schema(name)
        .await?
        .ok_or_else(|| invalid("membership schema is absent"))?;
    schema.populate_runtime_fields()?;
    db.db_ops.molecule_keys().load_schema(&schema).await?;
    Ok(schema)
}
