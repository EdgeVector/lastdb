use super::{emit, files, require, Change, Replica, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use fold_db::storage::disk_pack::peer_sync_spike::Entry;
use fold_db::storage::disk_pack::peer_sync_spike::{
    MAX_CHANGES, MAX_ENTRY_BYTES, MAX_EXPORT_BATCH, MAX_IMPORT_BYTES, MAX_IMPORT_CHANGES,
    MAX_ORIGINS, MAX_PAGE,
};
use std::io::{BufWriter, Write};
use std::path::Path;

pub(super) async fn status(replica: &Replica) -> Result<()> {
    emit(&serde_json::json!({
        "origin": replica.writer()?.origin,
        "frontiers": replica.frontiers().await?,
        "clock": replica.clock().await?,
        "limits": {
            "origins": MAX_ORIGINS, "changes_per_event": MAX_CHANGES,
            "event_bytes": MAX_ENTRY_BYTES, "import_bytes": MAX_IMPORT_BYTES,
            "import_changes": MAX_IMPORT_CHANGES, "range_page_records": MAX_PAGE,
            "export_fetch_entries": MAX_EXPORT_BATCH,
            "tail_events_per_adoption": 128,
        },
    }))
}

pub(super) async fn batch(replica: &Replica, args: &[String]) -> Result<()> {
    require(args, 6)?;
    let start: u64 = args[3].parse()?;
    let count: usize = args[4].parse()?;
    let value_bytes: usize = args[5].parse()?;
    if count == 0 || count > 128 || value_bytes > 16 * 1024 {
        return Err("batch cap: 1..128 changes and at most 16 KiB per value".into());
    }
    let changes = (0..count)
        .map(|offset| {
            Ok(Change {
                hash: args[2].clone(),
                range: format!(
                    "{:012}",
                    start.checked_add(offset as u64).ok_or("range overflow")?
                ),
                value: Some("v".repeat(value_bytes)),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let started = std::time::Instant::now();
    let entry = replica.author(&replica.writer()?, changes).await?;
    emit(&serde_json::json!({
        "origin": entry.origin, "sequence": entry.sequence, "changes": count,
        "durable_write_ms": started.elapsed().as_secs_f64() * 1000.0,
    }))
}

pub(super) async fn export(replica: &Replica, args: &[String]) -> Result<()> {
    if args.len() != 5 && args.len() != 6 {
        return Err(super::usage().into());
    }
    let limit = args
        .get(5)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(128);
    let mut entries = replica.export(&args[2], args[3].parse()?, limit).await?;
    while serde_json::to_vec(&entries)?.len() > 1024 * 1024
        || entries
            .iter()
            .map(|entry| entry.changes.len())
            .sum::<usize>()
            > 1024
    {
        entries.pop().ok_or("one event exceeds the packet cap")?;
    }
    let bytes = files::write_packet(Path::new(&args[4]), &args[1], &entries)?;
    emit(&serde_json::json!({
        "entries": entries.len(), "bytes": bytes,
        "last_sequence": entries.last().map(|entry| entry.sequence),
    }))
}

pub(super) async fn import(replica: &Replica, args: &[String]) -> Result<()> {
    require(args, 3)?;
    let entries: Vec<Entry> = files::read_packet(Path::new(&args[2]), &args[1])?;
    emit(&replica.import(&entries).await?)
}

pub(super) async fn range(replica: &Replica, args: &[String]) -> Result<()> {
    require(args, 5)?;
    let after = if args[3] == "-" {
        None
    } else if let Some(encoded) = args[3].strip_prefix("after:") {
        Some(String::from_utf8(URL_SAFE_NO_PAD.decode(encoded)?)?)
    } else {
        Some(args[3].clone())
    };
    let page = replica
        .range(&args[2], after.as_deref(), args[4].parse()?)
        .await?;
    let next = page
        .next
        .map(|key| format!("after:{}", URL_SAFE_NO_PAD.encode(key.as_bytes())));
    emit(&serde_json::json!({"items": page.items, "next": next}))
}

pub(super) async fn dump(replica: &Replica, args: &[String]) -> Result<()> {
    require(args, 4)?;
    let mut output = BufWriter::new(files::private_file(Path::new(&args[3]))?);
    let mut after = None;
    let mut count = 0_usize;
    loop {
        let page = replica.range(&args[2], after.as_deref(), 128).await?;
        for change in page.items {
            serde_json::to_writer(&mut output, &change)?;
            output.write_all(b"\n")?;
            count += 1;
        }
        if page.next.is_none() {
            break;
        }
        if page.next == after {
            return Err("range cursor does not advance".into());
        }
        after = page.next;
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    files::sync_parent(Path::new(&args[3]))?;
    emit(&serde_json::json!({"records": count}))
}
