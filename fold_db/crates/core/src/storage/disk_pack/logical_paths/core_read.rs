//! Full selected tips belong to one captured read, not a second warm authority.

use super::*;
use crate::atom::{AtomEntry, KeyMetadata};
use crate::schema::types::key_value::KeyValue;
use std::collections::HashMap;

const MAX_ENTRIES: usize = 10_000;
const MAX_BYTES: usize = 4 * 1024 * 1024;
const SNAPSHOT_NAME: &str = "native_selected_core_tips_v1";
static SELECTED_TIPS: u8 = 0;

#[derive(Clone, Serialize)]
pub(crate) struct SelectedTip {
    pub(crate) entry: AtomEntry,
    pub(crate) metadata: Option<KeyMetadata>,
    pub(crate) tip_source: Option<(String, String)>,
    /// Complete possible winners and structural controls for this exact key.
    pub(crate) sources: Option<Vec<(String, String)>>,
    pub(super) partition: Option<String>,
}

#[derive(Default)]
struct SelectedTips {
    entries: HashMap<String, (SelectedTip, usize)>,
    bytes: usize,
}

/// A missing storage key keeps the typed receipt without direct-tip authority.
pub(crate) fn note_tip(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    entry: &AtomEntry,
    metadata: Option<&KeyMetadata>,
    storage_key: Option<&str>,
) -> StorageResult<()> {
    let Some(receipts) = receipts(runtime.clone())? else {
        return Ok(());
    };
    let tip_source = storage_key
        .map(|key| super::source::source_for_request(runtime, key))
        .transpose()?
        .flatten();
    let partition = storage_key
        .filter(|_| tip_source.is_some())
        .and_then(|key| partition_of(scope, key));
    let identity = identity(scope, molecule, key, &entry.atom_uuid)?;
    let mut receipts = receipts.lock().map_err(|_| poisoned())?;
    if let Some((previous, _)) = receipts.entries.get(&identity) {
        if previous.entry == *entry
            && previous.metadata.as_ref() == metadata
            && (tip_source.is_none() || previous.tip_source == tip_source)
            && (partition.is_none() || previous.partition == partition)
        {
            return Ok(());
        }
    }
    if let Some((_, bytes)) = receipts.entries.remove(&identity) {
        receipts.bytes = receipts.bytes.saturating_sub(bytes);
    }
    let bytes = receipt_bytes(
        &identity,
        entry,
        metadata,
        tip_source.as_ref(),
        partition.as_deref(),
        None,
    )?;
    let Some(total) = receipts
        .bytes
        .checked_add(bytes)
        .filter(|n| *n <= MAX_BYTES)
    else {
        return Ok(());
    };
    if receipts.entries.len() >= MAX_ENTRIES {
        return Ok(());
    }
    receipts.bytes = total;
    receipts.entries.insert(
        identity,
        (
            SelectedTip {
                entry: entry.clone(),
                metadata: metadata.cloned(),
                tip_source,
                sources: None,
                partition,
            },
            bytes,
        ),
    );
    Ok(())
}

/// Attach the full exact-key search only after the resolver records its winner.
/// Receipts without complete candidates can reuse bodies but cannot skip lookup.
pub(crate) fn note_candidates(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    atom_uuid: &str,
    candidates: &[String],
    encoding: crate::atom::AtomKeyEncoding,
) -> StorageResult<()> {
    let Some(receipts) = receipts(runtime.clone())? else {
        return Ok(());
    };
    let identity = identity(scope, molecule, key, atom_uuid)?;
    let mut receipts = receipts.lock().map_err(|_| poisoned())?;
    let Some((selected, previous_bytes)) = receipts.entries.get(&identity) else {
        return Ok(());
    };
    // Core-only selections retain their certified edges in the core. Rebuilding
    // candidates here cannot strengthen them and only repeats key encoding.
    let Some(winner) = selected.tip_source.as_ref() else {
        return Ok(());
    };
    let expanded = super::dependencies::tip_sources(scope, molecule, candidates);
    let mut sources = (!candidates.is_empty() && expanded.contains(winner)).then_some(expanded);
    let mut bytes = receipt_bytes(
        &identity,
        &selected.entry,
        selected.metadata.as_ref(),
        selected.tip_source.as_ref(),
        selected.partition.as_deref(),
        sources.as_deref(),
    )?;
    let retained = receipts.bytes.saturating_sub(*previous_bytes);
    if retained.saturating_add(bytes) > MAX_BYTES {
        sources = None;
        bytes = receipt_bytes(
            &identity,
            &selected.entry,
            selected.metadata.as_ref(),
            selected.tip_source.as_ref(),
            selected.partition.as_deref(),
            None,
        )?;
    }
    let Some(total) = receipts
        .bytes
        .saturating_sub(*previous_bytes)
        .checked_add(bytes)
        .filter(|bytes| *bytes <= MAX_BYTES)
    else {
        return Ok(());
    };
    let selected = if let Some((selected, previous_bytes)) = receipts.entries.get_mut(&identity) {
        selected.sources = sources;
        *previous_bytes = bytes;
        Some(selected.clone())
    } else {
        None
    };
    receipts.bytes = total;
    drop(receipts);
    if let Some(selected) = selected {
        super::tips::admit(runtime, scope, molecule, key, encoding, &selected)?;
    }
    Ok(())
}

/// The selected atom identity prevents reuse of a receipt for another tip target.
pub(crate) fn selected_tip(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    atom: &str,
) -> StorageResult<Option<SelectedTip>> {
    let cache = std::ptr::from_ref(&SELECTED_TIPS) as usize;
    let Some(receipts) = read_snapshot_get::<Mutex<SelectedTips>>(runtime, cache, SNAPSHOT_NAME)?
    else {
        return Ok(None);
    };
    let identity = identity(scope, molecule, key, atom)?;
    let selected = receipts
        .lock()
        .map_err(|_| poisoned())?
        .entries
        .get(&identity)
        .map(|(selected, _)| selected.clone());
    Ok(selected)
}

pub(crate) fn selected_partition(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    atom: &str,
) -> StorageResult<Option<crate::atom::AtomPartition>> {
    let cache = std::ptr::from_ref(&SELECTED_TIPS) as usize;
    let Some(receipts) = read_snapshot_get::<Mutex<SelectedTips>>(runtime, cache, SNAPSHOT_NAME)?
    else {
        return Ok(None);
    };
    let identity = identity(scope, molecule, key, atom)?;
    let receipts = receipts.lock().map_err(|_| poisoned())?;
    Ok(receipts
        .entries
        .get(&identity)
        .and_then(|(selected, _)| selected.partition.as_deref())
        .and_then(crate::atom::AtomPartition::from_prefix))
}

/// The reader supplies its winning physical key before API-key projection.
/// Location retention does not depend on the optional source-authority ledger.
pub(crate) fn note_winning_tip(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    entry: &AtomEntry,
    metadata: Option<&KeyMetadata>,
    storage_key: &str,
) -> StorageResult<bool> {
    note_tip(
        runtime.clone(),
        scope,
        molecule,
        key,
        entry,
        metadata,
        Some(storage_key),
    )?;
    let Some(partition) = partition_of(scope, storage_key)
        .as_deref()
        .and_then(crate::atom::AtomPartition::from_prefix)
    else {
        return Ok(false);
    };
    note_partition(
        runtime,
        scope,
        molecule,
        key,
        entry,
        metadata,
        Some(&partition),
    )
}

/// A core-authorized selection can retain its certified body location without
/// inventing a physical read receipt or another current-value owner.
pub(crate) fn note_partition(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    entry: &AtomEntry,
    metadata: Option<&KeyMetadata>,
    partition: Option<&crate::atom::AtomPartition>,
) -> StorageResult<bool> {
    note_tip(runtime.clone(), scope, molecule, key, entry, metadata, None)?;
    let Some(receipts) = receipts(runtime)? else {
        return Ok(false);
    };
    let identity = identity(scope, molecule, key, &entry.atom_uuid)?;
    let mut receipts = receipts.lock().map_err(|_| poisoned())?;
    let Some((selected, previous_bytes)) = receipts.entries.get(&identity) else {
        return Ok(false);
    };
    if selected.entry != *entry || selected.metadata.as_ref() != metadata {
        return Ok(false);
    }
    if selected.partition.as_deref() == partition.map(crate::atom::AtomPartition::as_str) {
        return Ok(true);
    }
    let bytes = receipt_bytes(
        &identity,
        entry,
        metadata,
        selected.tip_source.as_ref(),
        partition.map(crate::atom::AtomPartition::as_str),
        selected.sources.as_deref(),
    )?;
    let Some(total) = receipts
        .bytes
        .saturating_sub(*previous_bytes)
        .checked_add(bytes)
        .filter(|bytes| *bytes <= MAX_BYTES)
    else {
        return Ok(false);
    };
    if let Some((selected, previous_bytes)) = receipts.entries.get_mut(&identity) {
        selected.partition = partition.map(|partition| partition.as_str().to_owned());
        *previous_bytes = bytes;
    }
    receipts.bytes = total;
    Ok(true)
}

fn partition_of(scope: Option<&str>, key: &str) -> Option<String> {
    let base = match scope {
        Some(scope) => key.strip_prefix(scope)?.strip_prefix(':')?,
        None => key,
    };
    crate::atom::AtomPartition::from_record_key(base).map(|partition| partition.as_str().to_owned())
}

fn receipts(runtime: Option<Arc<PackRuntime>>) -> StorageResult<Option<Arc<Mutex<SelectedTips>>>> {
    if !read_snapshot_active(runtime.clone()) {
        return Ok(None);
    }
    let cache = std::ptr::from_ref(&SELECTED_TIPS) as usize;
    if let Some(receipts) = read_snapshot_get(runtime.clone(), cache, SNAPSHOT_NAME)? {
        return Ok(Some(receipts));
    }
    match read_snapshot_insert(
        runtime,
        cache,
        SNAPSHOT_NAME.into(),
        Mutex::new(SelectedTips::default()),
    ) {
        Ok(receipts) => Ok(Some(receipts)),
        Err(StorageError::ReadResourceLimit { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn identity(
    scope: Option<&str>,
    molecule: &str,
    key: &KeyValue,
    atom: &str,
) -> StorageResult<String> {
    serde_json::to_string(&(
        scope,
        molecule,
        key.hash.as_deref().unwrap_or_default(),
        key.range.as_deref().unwrap_or_default(),
        atom,
    ))
    .map_err(|error| serialization(&error))
}

fn receipt_bytes(
    identity: &str,
    entry: &AtomEntry,
    metadata: Option<&KeyMetadata>,
    source: Option<&(String, String)>,
    partition: Option<&str>,
    sources: Option<&[(String, String)]>,
) -> StorageResult<usize> {
    let size = super::body::encoded_len(&(entry, metadata, source, partition, sources))?;
    Ok(identity
        .len()
        .saturating_add(size)
        .saturating_add(sources.map_or(0, |sources| sources.len().saturating_mul(128)))
        .saturating_add(256))
}

fn poisoned() -> StorageError {
    StorageError::BackendError("native selected core tips lock is poisoned".into())
}
