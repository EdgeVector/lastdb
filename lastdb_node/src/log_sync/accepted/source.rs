//! Read and classify the next durable local upload page.

use database_policy::SpecBinding;
use fold_db::schema::types::KeyValue;
use fold_db::storage::disk_pack::PackRuntime;
use serde::Deserialize;

use crate::accepted_writes::{self, Blocked, Item, Page, Publication, StreamStatus};

const PAGE_LIMIT: u32 = 64;

pub(super) fn capture_block(status: &StreamStatus) -> Option<&'static str> {
    if status.gap {
        Some("local capture has a gap")
    } else if status.snapshot_required {
        Some("recovery required for writes from before local capture")
    } else if status.acked < status.legacy_through {
        Some("old local writes still await cloud acceptance")
    } else {
        None
    }
}

#[derive(Deserialize)]
struct AcceptedTarget {
    schema: String,
    key: Option<KeyValue>,
    action: String,
    file: Option<serde::de::IgnoredAny>,
    inline_file: Option<serde::de::IgnoredAny>,
}

pub(super) type LegacyPending<'a> = (String, Option<KeyValue>, &'a str, bool);

pub(super) fn legacy_pending(page: &Page) -> Option<Vec<LegacyPending<'_>>> {
    page.items
        .iter()
        .filter_map(|item| match item {
            Item::Publish {
                mutation_id,
                kind,
                canonical,
                authored_at_ms: None,
                ..
            } => Some((mutation_id, kind, canonical)),
            _ => None,
        })
        .map(|(id, kind, canonical)| {
            let target: AcceptedTarget = serde_json::from_slice(canonical).ok()?;
            let attachment_affected = target.file.is_some()
                || target.inline_file.is_some()
                || matches!(target.action.as_str(), "delete" | "purge");
            ((kind == "schema") == target.key.is_none()).then_some((
                target.schema,
                target.key,
                id.as_str(),
                attachment_affected,
            ))
        })
        .collect()
}

pub(super) enum PublishPolicy {
    Private,
    Share(SpecBinding),
}

pub(super) fn read_page(
    runtime: &PackRuntime,
    key: &[u8; 32],
    policy: &PublishPolicy,
) -> Result<Option<Page>, &'static str> {
    let status = accepted_writes::status(runtime);
    if status.gap {
        tracing::warn!(target: "lastdbd::log_sync", "accepted prefix has a gap");
        return Ok(None);
    }
    if status.snapshot_required {
        return Ok(None);
    }
    let publication = match policy {
        PublishPolicy::Private => Publication::Personal,
        PublishPolicy::Share(spec) => Publication::Share(spec),
    };
    match accepted_writes::prefix(runtime, key, publication, status.acked, PAGE_LIMIT) {
        Ok(page) if !page.snapshot_required => Ok(Some(page)),
        Ok(_) | Err(Blocked::Snapshot | Blocked::Gap) => Ok(None),
        Err(blocked) => {
            tracing::warn!(target: "lastdbd::log_sync", reason = ?blocked, "accepted prefix was not read");
            Err("the local upload queue could not be read")
        }
    }
}
