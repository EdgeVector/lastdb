//! Every authoritative field value must remain represented after membership selection.

use super::*;
use crate::storage::KvMutation;
use sha2::{Digest, Sha256};

pub(super) async fn observe(
    store: &dyn KvStore,
    id: &str,
    values: Vec<(String, String, bool)>,
) -> Result<(), SchemaError> {
    let mut seen = BTreeSet::new();
    let mut covered = BTreeSet::new();
    for (field, atom, selected) in values {
        let mut hash = Sha256::new();
        hash.update(field.as_bytes());
        hash.update([0]);
        hash.update(atom.as_bytes());
        let pair = format!("{:x}", hash.finalize());
        seen.insert(pair.clone());
        if selected {
            covered.insert(pair);
        }
    }
    let rows = seen
        .into_iter()
        .map(|pair| KvMutation::put(format!("seen:{id}:{pair}"), vec![1]))
        .chain(
            covered
                .into_iter()
                .map(|pair| KvMutation::put(format!("covered:{id}:{pair}"), vec![1])),
        )
        .collect();
    store.batch_mutate(rows).await?;
    Ok(())
}

pub(super) async fn verify(store: &dyn KvStore, mut change: Change) -> Result<Change, SchemaError> {
    if change.phase == Phase::Verified {
        return Ok(change);
    }
    change.phase = Phase::Verifying;
    let prefix = format!("seen:{}:", change.id);
    let (mut start, end) = crate::kind_partition::exact_prefix_bounds(&prefix);
    if let Some(after) = &change.audit_after {
        start = format!("{after}\0");
    }
    let rows = store
        .scan_range_paged(start.as_bytes(), end.as_bytes(), 256)
        .await?;
    let mut keys = Vec::new();
    for (key, value) in &rows {
        let key = std::str::from_utf8(key)
            .map_err(|_| invalid("conversion coverage key is unreadable"))?;
        if value.as_slice() != [1] {
            return Err(invalid("conversion coverage record is invalid"));
        }
        let suffix = key
            .strip_prefix(&prefix)
            .ok_or_else(|| invalid("conversion coverage prefix differs"))?;
        keys.push(format!("covered:{}:{suffix}", change.id).into_bytes());
    }
    let covered = store.get_many(keys).await?;
    if covered.len() != rows.len() || covered.iter().any(|value| value.as_deref() != Some(&[1])) {
        return Err(invalid("legacy source values lack an authoritative app member; repair membership or abort this conversion"));
    }
    change.audit_after = rows
        .last()
        .map(|(key, _)| String::from_utf8_lossy(key).into_owned());
    if rows.len() < 256 {
        change.phase = Phase::Verified;
    }
    save(store, &change).await?;
    Ok(change)
}

pub(super) async fn cleanup(store: &dyn KvStore, id: &str) -> Result<bool, SchemaError> {
    let mut deletes = Vec::new();
    for family in ["seen", "covered"] {
        let rows = store
            .scan_prefix_paged(format!("{family}:{id}:").as_bytes(), 256)
            .await?;
        deletes.extend(rows.into_iter().map(|(key, _)| KvMutation::delete(key)));
    }
    let done = deletes.is_empty();
    store.batch_mutate(deletes).await?;
    Ok(done)
}
