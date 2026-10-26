//! Preview or execute one bounded page. The returned cursor is the next call's bound.

use super::*;

pub(crate) fn db_cleanup_delete_markers(
    socket: &Path,
    body: &serde_json::Value,
    json: bool,
) -> Result<(), String> {
    let page = db_post_json(socket, "/api/db/molecule-drain", body)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&page).map_err(|e| e.to_string())?
        );
    } else {
        println!(
            "Delete-marker page: {} eligible, {} deleted; complete: {}",
            page["eligible"], page["deleted"], page["complete"]
        );
        if let Some(reason) = page["blocked"].as_str() {
            println!("Retained: {reason}");
        }
        if let Some(cursor) = page["next_cursor"].as_str() {
            println!("Next --after-key: {cursor}");
        }
    }
    if let Some(reason) = page["blocked"].as_str() {
        return Err(format!(
            "delete-marker cleanup retained this molecule: {reason}"
        ));
    }
    Ok(())
}
