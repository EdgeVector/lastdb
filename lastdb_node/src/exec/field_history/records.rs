//! The history route of a home: field history entries keep a copy of
//! the old value at a record history key (decision D5). A policy needs only
//! the field identity; a data action reads the record key of the location.

use super::*;

pub(super) async fn execute(
    host: &Host,
    ctx: &AccessContext,
    request: Request,
) -> Result<Value, HostError> {
    let scope = ctx.storage_prefix.as_deref();
    let db = host.db.db_ops();
    let location = &request.location;
    let schema = db
        .get_schema(&location.schema)
        .await?
        .ok_or_else(|| HostError::new(404, "history schema does not exist"))?;
    // The record key needs the key codec of the record space. After a
    // restart nothing has loaded it until a query or a write of the schema.
    db.field_keys().load_schema(&schema).await?;
    let identity = schema
        .field_hashes
        .get(&location.field)
        .cloned()
        .ok_or_else(|| HostError::new(400, "history field does not exist"))?;
    let records = db.records();
    let key = &location.key;
    match request.action {
        Action::Policy { policy } => {
            if let Some(policy) = policy {
                records
                    .set_field_history_policy(&identity, policy, scope)
                    .await?;
            }
            let policy = records.field_history_policy(&identity, scope).await?;
            Ok(serde_json::json!({"field_identity": identity, "policy": policy}))
        }
        Action::List { cursor, limit } => {
            let page = records
                .record_history_page(
                    &schema,
                    key,
                    (&location.field, &identity),
                    scope,
                    (cursor.as_deref(), limit.unwrap_or(32)),
                )
                .await?;
            serde_json::to_value(page).map_err(|e| HostError::new(500, e.to_string()))
        }
        Action::Get { id } => records
            .record_history_value(&schema, key, &identity, scope, &id)
            .await?
            .ok_or_else(|| HostError::new(404, "history entry is absent or expired")),
        Action::Purge { cursor } => Ok(records
            .purge_record_history(&schema, key, &identity, scope, cursor.as_deref())
            .await?),
        Action::Restore {
            id,
            expected_mutation,
            absent,
        } => {
            require_restore_guard(expected_mutation.as_deref(), absent)?;
            let value = records
                .record_history_value(&schema, key, &identity, scope, &id)
                .await?
                .ok_or_else(|| HostError::new(404, "history entry is absent or expired"))?;
            restore(host, ctx, request.location, &value, expected_mutation).await
        }
    }
}
