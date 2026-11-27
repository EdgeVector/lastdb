//! The spool's records in the main home: reads of the committed root and the operations a
//! commit writes. Every operation goes into the same commit as the write it belongs to.

use super::types::{decode_entry, encode_entry, Meta, State};
use super::Error;
use crate::storage::disk_pack::physical::V2Home;
use laststore::disk_run::home::{HomeMutation, ReadBudget};

/// One record per entry, key the sequence as 20 digits so key order is sequence order.
pub(super) const ENTRIES: &str = "accepted_entries";
/// One record per recent mutation id: the plaintext SHA-256 of its write. The write path does
/// not read these: the cloud log is not read back for a check.
pub(super) const IDS: &str = "accepted_ids";
/// One record per recent mutation id by sequence, so the oldest ids can be found and trimmed.
pub(super) const ID_ORDER: &str = "accepted_id_order";
/// The spool's state row.
pub(super) const STATE: &str = "accepted_state";
const STATE_KEY: &str = "state";

/// The collections a reset empties. The state row is not deleted: the reset writes a new one.
const CLEARED: [&str; 3] = [ENTRIES, IDS, ID_ORDER];

/// The end of a range over a whole collection: above every key byte.
const ALL: &[u8] = &[0xff];

/// Records read in one page of a range.
const PAGE: usize = 1024;
/// Id records one trim removes at most.
pub(super) const TRIM_MAX: usize = 4_096;

pub(super) fn seq_key(sequence: u64) -> String {
    format!("{sequence:020}")
}

fn parse_seq(key: &str) -> Result<u64, Error> {
    if key.len() != 20 || !key.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Gap);
    }
    key.parse().map_err(|_| Error::Gap)
}

fn put(collection: &str, key: String, body: Vec<u8>) -> HomeMutation {
    HomeMutation::new(collection, key, Some(body))
}

fn delete(collection: &str, key: String) -> HomeMutation {
    HomeMutation::new(collection, key, None)
}

fn budget() -> ReadBudget {
    ReadBudget::new(256 << 20, 8_192)
}

/// Point reads of the committed root, in the order of `keys`.
fn get_many(
    physical: &V2Home,
    collection: &str,
    keys: &[String],
) -> Result<Vec<Option<Vec<u8>>>, Error> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let root = physical.home.current_root();
    let coords: Vec<(String, String)> = keys
        .iter()
        .map(|key| (collection.to_owned(), key.clone()))
        .collect();
    let mut read = physical.home.read_root(&root, budget());
    read.get_many_bounded(&coords, u64::MAX)
        .map_err(|_| Error::Unavailable)?
        .into_iter()
        .map(|found| found.map_err(|_| Error::Unavailable))
        .collect()
}

/// Every live record of `collection` with a key in `[start, end)`, in key order, read in pages.
/// `visit` returns false to stop.
fn range(
    physical: &V2Home,
    collection: &str,
    start: &[u8],
    end: &[u8],
    mut visit: impl FnMut(String, Vec<u8>) -> Result<bool, Error>,
) -> Result<(), Error> {
    let root = physical.home.current_root();
    let mut from = start.to_vec();
    loop {
        let mut read = physical.home.read_root(&root, budget());
        let mut cursor = read
            .administrative_range(collection, &from, end)
            .map_err(|_| Error::Unavailable)?;
        let page = cursor
            .next_page_yielding(PAGE, 0, 1)
            .map_err(|_| Error::Unavailable)?;
        // A page cut by the read budget names where to resume. A page that reached its record
        // limit names nothing: it resumes after its last record.
        let resume = page
            .resume
            .or_else(|| page.records.last().map(|(key, _)| key.clone()));
        for (key, value) in page.records {
            if !visit(key, value)? {
                return Ok(());
            }
        }
        if page.exhausted {
            return Ok(());
        }
        let Some(last) = resume else {
            return Err(Error::Unavailable);
        };
        from = last.into_bytes();
        from.push(0);
    }
}

pub(super) fn read_state(physical: &V2Home) -> Result<Option<State>, Error> {
    let found = get_many(physical, STATE, &[STATE_KEY.to_owned()])?;
    match found.into_iter().next().flatten() {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| Error::Gap),
        None => Ok(None),
    }
}

pub(super) fn state_op(state: &State) -> Result<HomeMutation, Error> {
    let bytes = serde_json::to_vec(state).map_err(|_| Error::Gap)?;
    Ok(put(STATE, STATE_KEY.into(), bytes))
}

/// The entries the store holds: (sequence, meta, weight), in sequence order.
pub(super) fn list_entries(physical: &V2Home) -> Result<Vec<(u64, Meta, u64)>, Error> {
    let mut out = Vec::new();
    range(physical, ENTRIES, b"", ALL, |key, value| {
        let sequence = parse_seq(&key)?;
        let (meta, signed) = decode_entry(&value).ok_or(Error::Gap)?;
        if meta.sequence != sequence {
            return Err(Error::Gap);
        }
        let weight = meta.weight(signed.len());
        out.push((sequence, meta, weight));
        Ok(true)
    })?;
    Ok(out)
}

/// The entries with sequence in `first..=last`, each with its signed bytes. A missing entry is
/// a gap.
pub(super) fn read_entries(
    physical: &V2Home,
    first: u64,
    last: u64,
) -> Result<Vec<(Meta, Vec<u8>)>, Error> {
    if last < first {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = (first..=last).map(seq_key).collect();
    let mut out = Vec::with_capacity(keys.len());
    for (sequence, found) in (first..=last).zip(get_many(physical, ENTRIES, &keys)?) {
        let value = found.ok_or(Error::Gap)?;
        let (meta, signed) = decode_entry(&value).ok_or(Error::Gap)?;
        if meta.sequence != sequence {
            return Err(Error::Gap);
        }
        let signed = signed.to_vec();
        out.push((meta, signed));
    }
    Ok(out)
}

pub(super) fn entry_op(meta: &Meta, signed: &[u8]) -> Result<HomeMutation, Error> {
    let value = encode_entry(meta, signed).ok_or(Error::Gap)?;
    Ok(put(ENTRIES, seq_key(meta.sequence), value))
}

pub(super) fn delete_entry_op(sequence: u64) -> HomeMutation {
    delete(ENTRIES, seq_key(sequence))
}

/// The sequence number of the newest entry of a device's chain: the row `chain_key` of the
/// metadata namespace, which the commit of each captured write updates.
pub(super) fn chain_seq(physical: &V2Home, chain_key: &str) -> Result<Option<u64>, Error> {
    let namespace = crate::storage::format::METADATA_NAMESPACE;
    let found = get_many(physical, namespace, &[chain_key.to_owned()])?;
    match found.into_iter().next().flatten() {
        Some(bytes) => serde_json::from_slice::<super::super::LocalEntry>(&bytes)
            .map(|entry| Some(entry.seq))
            .map_err(|_| Error::Gap),
        None => Ok(None),
    }
}

/// True when `metadata` holds any `signed-replay-head:` row from an incoming signed-log stream.
pub(super) fn has_any_stream_head(physical: &V2Home) -> Result<bool, Error> {
    let namespace = crate::storage::format::METADATA_NAMESPACE;
    let mut found = false;
    range(
        physical,
        namespace,
        b"signed-replay-head:",
        b"signed-replay-head;",
        |_, _| {
            found = true;
            Ok(false)
        },
    )?;
    Ok(found)
}

/// How many id records the store holds.
pub(super) fn count_ids(physical: &V2Home) -> Result<u64, Error> {
    let mut count = 0u64;
    range(physical, ID_ORDER, b"", ALL, |_, _| {
        count += 1;
        Ok(true)
    })?;
    Ok(count)
}

pub(super) fn id_ops(meta: &Meta) -> [HomeMutation; 2] {
    [
        put(
            IDS,
            meta.mutation_id.clone(),
            meta.plaintext_sha256.clone().into_bytes(),
        ),
        put(
            ID_ORDER,
            seq_key(meta.sequence),
            meta.mutation_id.clone().into_bytes(),
        ),
    ]
}

/// Deletes of the id records of the oldest writes with sequence at most `through`, at most
/// `TRIM_MAX` of them.
pub(super) fn trim_id_ops(physical: &V2Home, through: u64) -> Result<Vec<HomeMutation>, Error> {
    let mut ops = Vec::new();
    let end = seq_key(through.saturating_add(1));
    range(physical, ID_ORDER, b"", end.as_bytes(), |key, value| {
        let id = String::from_utf8(value).map_err(|_| Error::Gap)?;
        ops.push(delete(IDS, id));
        ops.push(delete(ID_ORDER, key));
        // One trim stays well inside the operations of one commit; the next trim goes on.
        Ok(ops.len() < 2 * TRIM_MAX)
    })?;
    Ok(ops)
}

/// Deletes of every spool record but the state row.
pub(super) fn clear_ops(physical: &V2Home) -> Result<Vec<HomeMutation>, Error> {
    let mut ops = Vec::new();
    for collection in CLEARED {
        range(physical, collection, b"", ALL, |key, _| {
            ops.push(delete(collection, key));
            Ok(true)
        })?;
    }
    Ok(ops)
}
