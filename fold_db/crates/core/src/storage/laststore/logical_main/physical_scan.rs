use super::*;

/// One blocking pass over the main collections that can hold `start..end`,
/// resuming from `cursor` and stopping at `limit` rows or `max_handles`.
pub(super) fn scan_main_range(
    store: &LastStore,
    start: &[u8],
    end: &[u8],
    cursor: Option<PhysicalScanCursor>,
    limit: usize,
    max_handles: usize,
) -> StorageResult<PhysicalScanPage> {
    let collections = main_collections_for_range(start, end);
    if collections.is_empty() || limit == 0 || max_handles == 0 {
        return Ok(PhysicalScanPage {
            next_cursor: cursor,
            ..Default::default()
        });
    }
    let start_collection = cursor
        .as_ref()
        .and_then(|position| position.collection.as_deref())
        .and_then(|wanted| collections.iter().position(|name| *name == wanted))
        .unwrap_or(0);
    let start_id = LastStoreKvStore::encode_bound(start);
    let end_id = LastStoreKvStore::encode_bound(end);
    let mut rows = Vec::new();
    let mut handles_left = max_handles;
    let mut handles_visited = 0u64;
    let mut cold_shard_loads = 0u64;
    let mut row_handle = None;

    for (offset, collection) in collections[start_collection..].iter().enumerate() {
        let collection_index = start_collection + offset;
        let vendor_cursor = vendor_cursor_for(cursor.as_ref(), collection);
        let page = walk_collection(
            store,
            collection,
            (start_id.as_str(), end_id.as_str()),
            vendor_cursor.as_ref(),
            limit - rows.len(),
            handles_left,
        )?;
        handles_visited = handles_visited.saturating_add(page.handles_visited);
        cold_shard_loads = cold_shard_loads.saturating_add(page.cold_shard_loads);
        handles_left = handles_left.saturating_sub(page.handles_visited as usize);
        if let Some((shard, group_id)) = page.row_handle {
            row_handle = Some(collection_cursor(collection, shard, group_id, None));
        }
        rows.extend(LastStoreKvStore::decode_rows(page.rows)?);
        if let Some(position) = page.next_cursor {
            return Ok(PhysicalScanPage {
                rows,
                next_cursor: Some(position_cursor(collection, position)?),
                row_handle,
                handles_visited,
                cold_shard_loads,
            });
        }
        if rows.len() >= limit || handles_left == 0 {
            let next_cursor = next_collection_cursor(&collections, collection_index);
            return Ok(PhysicalScanPage {
                rows,
                next_cursor,
                row_handle,
                handles_visited,
                cold_shard_loads,
            });
        }
    }

    Ok(PhysicalScanPage {
        rows,
        next_cursor: None,
        row_handle,
        handles_visited,
        cold_shard_loads,
    })
}

/// One page from `collection`. An empty start bound is the startup walk.
fn walk_collection(
    store: &LastStore,
    collection: &str,
    (start_id, end_id): (&str, &str),
    vendor_cursor: Option<&laststore::PhysicalRangeCursor>,
    limit: usize,
    max_handles: usize,
) -> StorageResult<laststore::PhysicalRangePage> {
    if start_id.is_empty() {
        store.walk_all_groups_at_startup(collection, vendor_cursor, limit, max_handles)
    } else {
        store.walk_all_groups(
            collection,
            start_id..end_id,
            vendor_cursor,
            limit,
            max_handles,
            laststore::AllGroupsPurpose::Admin,
        )
    }
    .map_err(LastStoreKvStore::map_error)
}

/// The caller cursor for a vendor position inside `collection`.
fn position_cursor(
    collection: &str,
    position: laststore::PhysicalRangeCursor,
) -> StorageResult<PhysicalScanCursor> {
    Ok(collection_cursor(
        collection,
        position.shard,
        position.group_id,
        position
            .after_id
            .map(|id| LastStoreKvStore::decode_key(&id))
            .transpose()?,
    ))
}

/// The cursor that starts the collection after `collection_index`, if any.
fn next_collection_cursor(
    collections: &[&str],
    collection_index: usize,
) -> Option<PhysicalScanCursor> {
    collections
        .get(collection_index + 1)
        .map(|next| PhysicalScanCursor {
            collection: Some((*next).to_string()),
            ..Default::default()
        })
}

/// The vendor resume position, when the caller's cursor points into `collection`.
fn vendor_cursor_for(
    cursor: Option<&PhysicalScanCursor>,
    collection: &str,
) -> Option<laststore::PhysicalRangeCursor> {
    cursor
        .filter(|position| position.collection.as_deref() == Some(collection))
        .map(|position| laststore::PhysicalRangeCursor {
            shard: position.shard,
            group_id: position.group_id,
            after_id: position
                .after_key
                .as_deref()
                .map(LastStoreKvStore::encode_key),
        })
}

fn collection_cursor(
    collection: &str,
    shard: u16,
    group_id: Option<u32>,
    after_key: Option<Vec<u8>>,
) -> PhysicalScanCursor {
    PhysicalScanCursor {
        collection: Some(collection.to_string()),
        shard,
        group_id,
        after_key,
    }
}
