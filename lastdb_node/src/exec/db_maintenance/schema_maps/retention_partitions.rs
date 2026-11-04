//! Owner-only pagination of the observed expiry partitions.

use super::*;

pub(super) async fn execute(body: RetentionBody, ctx: &AccessContext, host: &Host) -> UdsResponse {
    match host
        .db
        .db_ops()
        .schemas()
        .schema_retention_hash_partitions_page(
            &body.schema,
            body.cursor.as_deref(),
            body.limit.unwrap_or(256),
        )
        .await
    {
        Ok((partitions, next_cursor)) => json_ok(&envelope(
            &serde_json::json!({
                "schema": body.schema,
                "observed_hash_partitions": partitions,
                "next_cursor": next_cursor,
            }),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(400, &format!("schema retention failed: {error}"), ctx),
    }
}
