//! Run confirmed retention maintenance as bounded, resumable server pages.

use super::*;

pub(super) fn run(
    socket: &std::path::Path,
    schema: &str,
    page_size: u16,
    confirmed: bool,
    json_only: bool,
) -> Result<(), String> {
    if !confirmed {
        return Err("backfill can expire existing records; pass --confirm-expire-existing".into());
    }
    let mut cursor: Option<String> = None;
    let (mut tracked, mut pages) = (0u64, 0u64);
    loop {
        let response = db_post_json(
            socket,
            "/api/db/schema-retention",
            &serde_json::json!({
                "action": "backfill", "schema": schema, "limit": page_size,
                "cursor": cursor, "confirm_expire_existing": true,
            }),
        )?;
        tracked += response
            .get("tracked")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| "backfill response has no tracked count".to_string())?;
        pages += 1;
        if response
            .get("complete")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            break;
        }
        let next = response
            .get("next_cursor")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "incomplete backfill response has no cursor".to_string())?;
        if cursor.as_deref() == Some(next) {
            return Err("retention backfill cursor did not advance".into());
        }
        cursor = Some(next.to_string());
    }
    if json_only {
        println!(
            "{}",
            serde_json::json!({ "schema": schema, "scope": "personal",
            "tracked": tracked, "pages": pages, "complete": true })
        );
    } else {
        println!(
            "Retention backfill for {schema}: {tracked} tracked records in {pages} durable pages"
        );
    }
    Ok(())
}
