//! Order one `previous_ref` chain and reject a gap, a cycle, or an extra object.

use std::collections::{HashMap, HashSet};

use cloud_backup_protocol::Text;

use super::types::{SnapshotManifest, SnapshotObject, SnapshotPart};
use super::RestoreError;

const CHAIN_LIMIT: usize = 1024;

pub(super) struct Link {
    pub manifest: SnapshotManifest,
    pub parts: Vec<SnapshotPart>,
}

pub(super) fn order<'a>(
    database_id: &str,
    tip: &'a str,
    objects: &'a [SnapshotObject],
) -> Result<Vec<Link>, RestoreError> {
    let indexed = index(objects)?;
    let mut oldest = walk(tip, &indexed)?;
    oldest.reverse();
    let links = pack(database_id, &oldest)?;
    check_progress(&links)?;
    Ok(links)
}

fn index(objects: &[SnapshotObject]) -> Result<HashMap<&str, &SnapshotObject>, RestoreError> {
    if objects.len() > CHAIN_LIMIT {
        return Err(RestoreError::Chain("snapshot chain"));
    }
    let mut map = HashMap::new();
    for object in objects {
        check_ref(&object.manifest.snapshot_ref)?;
        if let Some(previous) = &object.manifest.previous_ref {
            check_ref(previous)?;
        }
        if object.manifest.at_ms == 0 {
            return Err(RestoreError::Chain("snapshot time"));
        }
        if map
            .insert(object.manifest.snapshot_ref.as_str(), object)
            .is_some()
        {
            return Err(RestoreError::Chain("snapshot ref"));
        }
    }
    Ok(map)
}

fn walk<'a>(
    tip: &'a str,
    map: &HashMap<&'a str, &'a SnapshotObject>,
) -> Result<Vec<&'a SnapshotObject>, RestoreError> {
    check_ref(tip)?;
    if !map.contains_key(tip) {
        return Err(RestoreError::Chain("snapshot ref"));
    }
    let mut newest = Vec::new();
    let mut seen = HashSet::new();
    let mut cursor = Some(tip);
    while let Some(refer) = cursor {
        if newest.len() >= CHAIN_LIMIT || !seen.insert(refer) {
            return Err(RestoreError::Chain("snapshot chain"));
        }
        let object = map
            .get(refer)
            .copied()
            .ok_or(RestoreError::Chain("snapshot chain"))?;
        newest.push(object);
        cursor = object.manifest.previous_ref.as_deref();
    }
    if seen.len() != map.len() {
        return Err(RestoreError::Chain("snapshot chain"));
    }
    Ok(newest)
}

fn pack(database_id: &str, oldest: &[&SnapshotObject]) -> Result<Vec<Link>, RestoreError> {
    let mut links = Vec::with_capacity(oldest.len());
    for object in oldest {
        links.push(Link {
            manifest: object.manifest.clone(),
            parts: order_parts(database_id, object)?,
        });
    }
    Ok(links)
}

fn order_parts(
    database_id: &str,
    object: &SnapshotObject,
) -> Result<Vec<SnapshotPart>, RestoreError> {
    let expect = object.manifest.parts as usize;
    if object.manifest.parts == 0 || object.parts.len() != expect {
        return Err(RestoreError::Chain("snapshot part"));
    }
    let mut parts = object.parts.clone();
    parts.sort_by_key(|part| part.index);
    for (offset, part) in parts.iter().enumerate() {
        let index = u32::try_from(offset).map_err(|_| RestoreError::Chain("snapshot part"))?;
        if part.index != index
            || part.database_id != database_id
            || part.snapshot_ref != object.manifest.snapshot_ref
        {
            return Err(RestoreError::Chain("snapshot part"));
        }
    }
    Ok(parts)
}

fn check_progress(links: &[Link]) -> Result<(), RestoreError> {
    for (index, link) in links.iter().enumerate() {
        let entries = entry_count(link);
        let through = link.manifest.through_delivery_seq;
        if through == 0 && (index != 0 || entries != 0) {
            return Err(RestoreError::Chain("snapshot order"));
        }
        if index == 0 {
            if link.manifest.previous_ref.is_some() {
                return Err(RestoreError::Chain("snapshot chain"));
            }
            continue;
        }
        let parent = &links[index - 1];
        if link.manifest.previous_ref.as_deref() != Some(parent.manifest.snapshot_ref.as_str()) {
            return Err(RestoreError::Chain("snapshot chain"));
        }
        let parent_through = parent.manifest.through_delivery_seq;
        if through < parent_through || (entries > 0 && through == parent_through) {
            return Err(RestoreError::Chain("snapshot order"));
        }
    }
    Ok(())
}

fn entry_count(link: &Link) -> usize {
    link.parts.iter().map(|part| part.entries.len()).sum()
}

fn check_ref(value: &str) -> Result<(), RestoreError> {
    Text::<1024>::try_from(value.to_owned()).map_err(|_| RestoreError::Chain("snapshot ref"))?;
    Ok(())
}
