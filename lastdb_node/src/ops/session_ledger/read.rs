//! Ledger and current-session readers and writers. Moved verbatim from `session_ledger.rs`.

use super::*;

/// Read every session record from `sessions.jsonl`, oldest first. Lines that
/// don't parse are skipped (a partially-written tail line from a crash mid-write
/// shouldn't poison the whole ledger). Missing file ⇒ empty vec.
pub fn read_all_records(home: &Path) -> Vec<SessionRecord> {
    let path = Ledger::ledger_path(home);
    let Ok(file) = std::fs::File::open(&path) else {
        return Vec::new();
    };
    std::io::BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<SessionRecord>(&l).ok())
        .collect()
}

/// The most recent session record, or `None` if the ledger is empty/missing.
pub fn read_last_record(home: &Path) -> Option<SessionRecord> {
    read_recent_records(home, 1).pop()
}

/// Read up to `limit` completed session records from the durable ledger,
/// newest last. This reads only the final bounded ledger window. A ledger with
/// a giant malformed tail row returns no records rather than scanning older
/// data to compensate.
pub fn read_recent_records(home: &Path, limit: usize) -> Vec<SessionRecord> {
    if limit == 0 {
        return Vec::new();
    }
    let path = Ledger::ledger_path(home);
    let Ok(mut file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let length = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(_) => return Vec::new(),
    };
    let start = length.saturating_sub(LAST_RECORD_MAX_BYTES);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return Vec::new();
    }
    let mut tail = Vec::new();
    if file.read_to_end(&mut tail).is_err() {
        return Vec::new();
    }
    let Ok(text) = std::str::from_utf8(&tail) else {
        return Vec::new();
    };
    // When the window begins in the middle of a row, discard that partial row.
    // If no newline exists, the whole window is one malformed giant row.
    let rows = if start > 0 {
        let Some((_, rows)) = text.split_once('\n') else {
            return Vec::new();
        };
        rows
    } else {
        text
    };
    let mut records: Vec<_> = rows
        .lines()
        .rev()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<SessionRecord>(line).ok())
        .take(limit.min(RECENT_RECORD_LIMIT))
        .collect();
    records.reverse();
    records
}

pub(super) fn read_current_session(home: &Path) -> Option<CurrentSession> {
    let raw = std::fs::read_to_string(Ledger::current_session_path(home)).ok()?;
    serde_json::from_str(&raw).ok()
}

/// The live session's identity + heartbeat, as read back from
/// `current-session.json`. Exposes the otherwise-private [`CurrentSession`] so
/// the `lastdb status` surface can compute current uptime (`now - start_ts`)
/// against the live daemon without a socket round-trip. `None` when no session
/// is live (the file is removed on clean shutdown).
#[derive(Debug, Clone)]
pub struct LiveSession {
    /// PID of the live daemon.
    pub pid: u32,
    /// Unix epoch seconds the live session started (uptime anchor).
    pub start_ts: u64,
    /// Unix epoch seconds of the most recent heartbeat (~60s cadence).
    pub last_heartbeat_ts: u64,
}

/// Read the live session's heartbeat file, if a session is currently running.
pub fn read_live_session(home: &Path) -> Option<LiveSession> {
    read_current_session(home).map(|c| LiveSession {
        pid: c.pid,
        start_ts: c.start_ts,
        last_heartbeat_ts: c.last_heartbeat_ts,
    })
}

pub(super) fn write_current_session(home: &Path, cur: &CurrentSession) -> std::io::Result<()> {
    std::fs::create_dir_all(home)?;
    let json = serde_json::to_string(cur).map_err(std::io::Error::other)?;
    // Write to a temp file then rename so a reader never sees a half-written
    // file (the heartbeat fires concurrently with a possible next-start read).
    let path = Ledger::current_session_path(home);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, &path)
}

pub(super) fn append_record(home: &Path, record: &SessionRecord) -> std::io::Result<()> {
    std::fs::create_dir_all(home)?;
    let json = serde_json::to_string(record).map_err(std::io::Error::other)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(Ledger::ledger_path(home))?;
    writeln!(file, "{json}")?;
    file.sync_data()
}
