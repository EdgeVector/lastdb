//! Explicit terminal migration. It never changes job/index counters or calls cloud.

use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct GcCompactReport {
    pub job_id: String,
    pub records: u32,
    pub pack_bytes: u64,
    pub source_files_removed: usize,
    pub summary_sha256: String,
    pub pack_sha256: String,
    pub summaries_and_index_untouched: bool,
}

fn direct_directory(path: &Path) -> SyncResult<()> {
    if !path.is_absolute() {
        return Err(io_error("receipt directory must be absolute"));
    }
    for parent in path.ancestors() {
        if !std::fs::symlink_metadata(parent)
            .map_err(io_error)?
            .is_dir()
        {
            return Err(io_error("receipt directory has an unsafe parent"));
        }
    }
    Ok(())
}

fn digest(path: &Path) -> SyncResult<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(io_error)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count = file.read(&mut buffer).map_err(io_error)?;
        if count == 0 {
            break;
        }
        hash.update(
            buffer
                .get(..count)
                .ok_or_else(|| io_error("invalid hash length"))?,
        );
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn source_paths(directory: &Path) -> SyncResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut entries = 0;
    for entry in std::fs::read_dir(directory).map_err(io_error)? {
        entries += 1;
        if entries > packed::MAX_SLOTS as usize + 8192 {
            return Err(io_error("receipt directory entry bound exceeded"));
        }
        let entry = entry.map_err(io_error)?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| io_error("invalid receipt filename"))?;
        let numbered = name.len() == 25
            && name.ends_with(".json")
            && name
                .get(..20)
                .is_some_and(|text| text.bytes().all(|byte| byte.is_ascii_digit()));
        let block = name.len() == 24
            && name.starts_with("objects-v1-")
            && name.ends_with(".pack")
            && name
                .get(11..19)
                .is_some_and(|text| text.bytes().all(|byte| byte.is_ascii_digit()));
        if numbered || block {
            if !entry.file_type().map_err(io_error)?.is_file() {
                return Err(io_error("receipt source is not a regular file"));
            }
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn source_records(paths: &[PathBuf], id: &str) -> SyncResult<BTreeMap<u64, Vec<u8>>> {
    let mut records = BTreeMap::new();
    let mut bytes = 0_u64;
    for path in paths {
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            let sequence = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse().ok())
                .ok_or_else(|| io_error("invalid numbered receipt"))?;
            let raw =
                storage::loose_raw(path)?.ok_or_else(|| io_error("receipt source disappeared"))?;
            packed::decode(&raw, id, sequence)?;
            insert(&mut records, sequence, raw, &mut bytes)?;
        } else {
            let (mut file, header) = packed::open(path, id)?;
            let expected_first = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.get(11..19))
                .and_then(|number| number.parse::<u64>().ok())
                .map(|number| number * u64::from(packed::BLOCK_SLOTS) + 1)
                .ok_or_else(|| io_error("invalid packed block filename"))?;
            if header.slots != packed::BLOCK_SLOTS || header.first != expected_first {
                return Err(io_error("invalid packed block range"));
            }
            for sequence in header.first..header.first + u64::from(header.slots) {
                if let Some(raw) = packed::read_raw(&mut file, header, sequence)? {
                    packed::decode(&raw, id, sequence)?;
                    insert(&mut records, sequence, raw, &mut bytes)?;
                }
            }
        }
    }
    Ok(records)
}

fn insert(
    records: &mut BTreeMap<u64, Vec<u8>>,
    sequence: u64,
    raw: Vec<u8>,
    bytes: &mut u64,
) -> SyncResult<()> {
    if sequence == 0 || sequence > u64::from(packed::MAX_SLOTS) {
        return Err(io_error("source sequence exceeds archive bound"));
    }
    if let Some(previous) = records.get(&sequence) {
        if previous == &raw {
            return Ok(());
        }
        if !superseded_intent(previous, &raw)? {
            return Err(io_error("duplicate receipt sources differ"));
        }
        // A packed recovery outcome supersedes a retained legacy intent. The
        // loose unresolved source stays in place; the archive keeps the result.
        *bytes -= previous.len() as u64;
    }
    *bytes += raw.len() as u64;
    if *bytes
        > packed::MAX_PACK_BYTES
            - u64::from(packed::MAX_SLOTS) * packed::SLOT_BYTES
            - packed::HEADER_BYTES
    {
        return Err(io_error("terminal receipt payload capacity exceeded"));
    }
    records.insert(sequence, raw);
    Ok(())
}

fn superseded_intent(previous: &[u8], current: &[u8]) -> SyncResult<bool> {
    let old: GcObjectReceipt = serde_json::from_slice(previous).map_err(io_error)?;
    let new: GcObjectReceipt = serde_json::from_slice(current).map_err(io_error)?;
    Ok(old.outcome == GcObjectOutcome::Intent
        && new.outcome != GcObjectOutcome::Intent
        && old.job_id == new.job_id
        && old.sequence == new.sequence
        && old.key == new.key
        && old.listed_bytes == new.listed_bytes
        && old.object_class == new.object_class
        && old.instance_id == new.instance_id
        && old.provider_version == new.provider_version)
}

fn unchanged(summary: &Path, summary_hash: &str) -> SyncResult<()> {
    if digest(summary)? != summary_hash {
        return Err(io_error("summary changed during terminal migration"));
    }
    Ok(())
}

fn terminal_job(root: &Path, job_id: &str) -> SyncResult<(PathBuf, GcJobReceipt, String)> {
    direct_directory(root)?;
    uuid::Uuid::parse_str(job_id).map_err(|_| io_error("job_id must be a UUID"))?;
    let directory = root.join(job_id);
    direct_directory(&directory)?;
    let summary = directory.join("summary.json");
    for path in [&summary, &root.join("index.json")] {
        let info = std::fs::symlink_metadata(path).map_err(io_error)?;
        if !info.is_file() || info.len() > 1024 * 1024 {
            return Err(io_error("receipt metadata is not a bounded regular file"));
        }
    }
    let job: GcJobReceipt = read(&summary)?;
    if job.job_id != job_id
        || !job.state.terminal()
        || job.in_flight_len() != 0
        || job.objects_reconciled > u64::from(packed::MAX_SLOTS)
    {
        return Err(io_error(
            "terminal compaction refuses active, queued, or pending work",
        ));
    }
    let index_path = root.join("index.json");
    let index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&index_path).map_err(io_error)?).map_err(io_error)?;
    let active = index
        .get("active")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| io_error("receipt index has no active-ID array"))?;
    if active.iter().any(|id| id.as_str() == Some(job_id)) {
        return Err(io_error("terminal compaction refuses an active index ID"));
    }
    let summary_hash = digest(&summary)?;
    Ok((directory, job, summary_hash))
}

fn remove_sources(
    paths: &[PathBuf],
    job_id: &str,
    file: &mut std::fs::File,
    header: packed::Header,
) -> SyncResult<usize> {
    let mut removed = 0;
    for path in paths {
        // Re-read the exact source immediately before unlink. A crash can
        // leave any subset; the published archive makes retries idempotent.
        let one = source_records(std::slice::from_ref(path), job_id)?;
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
            && one.iter().any(|(sequence, raw)| {
                packed::decode(raw, job_id, *sequence).is_ok_and(|receipt| {
                    matches!(
                        receipt.outcome,
                        GcObjectOutcome::Intent
                            | GcObjectOutcome::Unknown
                            | GcObjectOutcome::UncertainWrite
                    )
                })
            })
        {
            continue;
        }
        for (sequence, raw) in one {
            if packed::read_raw(file, header, sequence)? != Some(raw) {
                return Err(io_error("receipt source changed before unlink"));
            }
        }
        std::fs::remove_file(path).map_err(io_error)?;
        removed += 1;
    }
    Ok(removed)
}

/// Compact one exact terminal job. No daemon, credentials, or DB copy is needed.
/// With remove_sources=false this only publishes the complete durable archive.
pub fn compact_terminal_receipts(
    root: &Path,
    job_id: &str,
    prune: bool,
) -> SyncResult<GcCompactReport> {
    let (directory, job, summary_hash) = terminal_job(root, job_id)?;
    let summary = directory.join("summary.json");
    let paths = source_paths(&directory)?;
    let sources = source_records(&paths, job_id)?;
    let archive = directory.join("objects-v1.pack");
    if !archive.exists() {
        for sequence in 1..=job.objects_reconciled {
            if !sources.contains_key(&sequence) {
                return Err(io_error("terminal source receipt is missing"));
            }
        }
        if let Some(last) = sources.keys().next_back() {
            unchanged(&summary, &summary_hash)?;
            packed::publish(
                &archive,
                job_id,
                1,
                *last as u32,
                sources
                    .iter()
                    .map(|(sequence, raw)| Ok((*sequence, raw.clone()))),
            )?;
        }
    }
    if sources.is_empty() && !archive.exists() {
        return Ok(GcCompactReport {
            job_id: job_id.into(),
            records: 0,
            pack_bytes: 0,
            source_files_removed: 0,
            summary_sha256: summary_hash,
            pack_sha256: String::new(),
            summaries_and_index_untouched: true,
        });
    }
    let (mut file, header) = packed::open(&archive, job_id)?;
    if header.first != 1 || u64::from(header.slots) < job.objects_reconciled {
        return Err(io_error("terminal archive does not start at sequence one"));
    }
    let mut record_count = 0;
    for sequence in 1..=u64::from(header.slots) {
        match packed::read_raw(&mut file, header, sequence)? {
            Some(raw) => {
                packed::decode(&raw, job_id, sequence)?;
                record_count += 1;
            }
            None if sequence <= job.objects_reconciled => {
                return Err(io_error("terminal archive receipt is missing"))
            }
            None => {}
        }
    }
    if record_count != header.records {
        return Err(io_error("terminal archive record count differs"));
    }
    for (sequence, raw) in &sources {
        let packed = packed::read_raw(&mut file, header, *sequence)?
            .ok_or_else(|| io_error("terminal archive receipt is missing"))?;
        if &packed != raw && !superseded_intent(raw, &packed)? {
            return Err(io_error(
                "terminal pack does not preserve exact receipt bytes",
            ));
        }
    }
    unchanged(&summary, &summary_hash)?;
    let removed = if prune {
        let removed = remove_sources(&paths, job_id, &mut file, header)?;
        sync_parent(&archive)?;
        removed
    } else {
        0
    };
    unchanged(&summary, &summary_hash)?;
    Ok(GcCompactReport {
        job_id: job_id.into(),
        records: header.records,
        pack_bytes: file.metadata().map_err(io_error)?.len(),
        source_files_removed: removed,
        summary_sha256: summary_hash,
        pack_sha256: digest(&archive)?,
        summaries_and_index_untouched: true,
    })
}
