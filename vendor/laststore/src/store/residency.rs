use super::*;

/// Measure a group's warm-set charge, split into what a trim could give back
/// and what it could not.
///
/// `trimmable` must stay in step with what [`trim_shard_read_caches`] actually
/// releases, or the trim pass will keep being told there is work to do and keep
/// finding none.
pub(super) fn estimate_shard_residency(handle: &ShardHandle) -> Residency {
    let sh = handle.lock().expect("poison");
    estimate_shard_residency_locked(&sh)
}

/// Estimate all heap owned by one resident group.
///
/// The warm limit used to charge only payload buffers plus `128 * index rows`.
/// That omitted the shard allocation, every map table, all map keys, frame
/// locations, LRU storage, segment ids, sidecar stamps, and path/name storage.
/// A group could therefore spend substantially more heap than its charge.
///
/// The standard collections do not expose their allocator's exact byte count.
/// These helpers charge their full logical capacities, hash control bytes, and
/// conservative B-tree node capacities. The result can overcharge allocator
/// padding, but it must not omit an owned allocation.
pub(super) fn estimate_shard_residency_locked(sh: &Shard) -> Residency {
    let values_payload = sh
        .values
        .iter()
        .map(|(key, value)| (key.capacity() as u64).saturating_add(vec_allocation_bytes(value)))
        .sum::<u64>();
    let values = hash_map_allocation_bytes(&sh.values).saturating_add(values_payload);

    let seg_payload = sh.seg_bytes.values().map(vec_allocation_bytes).sum::<u64>();
    let seg_bytes = hash_map_allocation_bytes(&sh.seg_bytes).saturating_add(seg_payload);

    let frame_payload = sh
        .frame_cache
        .values()
        .map(vec_allocation_bytes)
        .sum::<u64>();
    let frame_cache = hash_map_allocation_bytes(&sh.frame_cache).saturating_add(frame_payload);

    let frame_locs =
        hash_map_allocation_bytes(&sh.frame_locs).saturating_add(sh.frame_loc_path_bytes);

    let open_buf = vec_allocation_bytes(&sh.open_buf);
    let index = btree_map_allocation_bytes(&sh.index).saturating_add(sh.index_key_bytes);
    let frame_cache_order = vec_deque_allocation_bytes(&sh.frame_cache_order);
    let segments = vec_allocation_bytes(&sh.segments);
    let sidecar_stamps = sh
        .sidecar_stamps
        .as_ref()
        .map(vec_allocation_bytes)
        .unwrap_or_default();
    // The handle owns one `ArcInner<Mutex<Shard>>`: charge the mutex wrapper
    // and the Arc's strong/weak counters, not only the fields inside `Shard`.
    let fixed = (std::mem::size_of::<Mutex<Shard>>() as u64)
        .saturating_add(2 * std::mem::size_of::<AtomicUsize>() as u64)
        .saturating_add(sh.dir.capacity() as u64)
        .saturating_add(sh.collection.capacity() as u64)
        .saturating_add(
            sh.pending_sorted_publish
                .as_ref()
                .map_or(0, |path| path.capacity() as u64),
        );

    // Must match what `trim_shard_read_caches` actually releases: the whole
    // buffer once it is fully spilled, and only the slack past its contents
    // while some of the tail is still unflushed.
    let open_buf_reclaimable = if open_tail_is_reproducible(sh) {
        open_buf
    } else {
        open_buf_slack(&sh.open_buf)
    };
    let mut trimmable = values
        .saturating_add(frame_cache)
        .saturating_add(frame_cache_order)
        .saturating_add(open_buf_reclaimable);
    // Under frame AEAD the segment bytes are decrypted plaintext with no
    // offset-addressable file behind them, so they are not reproducible.
    if sh.data_key.is_none() {
        trimmable = trimmable.saturating_add(seg_bytes);
    }

    Residency {
        total: fixed
            .saturating_add(open_buf)
            .saturating_add(values)
            .saturating_add(seg_bytes)
            .saturating_add(frame_cache)
            .saturating_add(frame_cache_order)
            .saturating_add(frame_locs)
            .saturating_add(segments)
            .saturating_add(sidecar_stamps)
            .saturating_add(index)
            .saturating_add(
                sh.sorted_segments
                    .iter()
                    .map(sorted::Segment::resident_bytes)
                    .sum::<u64>(),
            )
            .saturating_add(
                ((sh.sorted_segments.capacity() - sh.sorted_segments.len())
                    * std::mem::size_of::<sorted::Segment>()) as u64,
            ),
        trimmable,
    }
}

pub(super) fn vec_allocation_bytes<T>(value: &Vec<T>) -> u64 {
    (value.capacity() as u64).saturating_mul(std::mem::size_of::<T>() as u64)
}

pub(super) fn vec_deque_allocation_bytes<T>(value: &VecDeque<T>) -> u64 {
    (value.capacity() as u64).saturating_mul(std::mem::size_of::<T>() as u64)
}

pub(super) fn hash_map_allocation_bytes<K, V>(value: &HashMap<K, V>) -> u64 {
    if value.capacity() == 0 {
        return 0;
    }
    // hashbrown stores one control byte per bucket and one SIMD group after the
    // table. The extra group also keeps this conservative across allocators.
    const CONTROL_GROUP_BYTES: u64 = 16;
    (value.capacity() as u64)
        .saturating_mul((std::mem::size_of::<(K, V)>() as u64).saturating_add(1))
        .saturating_add(CONTROL_GROUP_BYTES)
}

pub(super) fn btree_map_allocation_bytes<K, V>(value: &BTreeMap<K, V>) -> u64 {
    if value.is_empty() {
        return 0;
    }
    // std's B-tree nodes hold at most 11 entries. A non-root node has at least
    // five, so this node count is a conservative upper bound after inserts and
    // deletes. Charge every node as an internal node, including twelve edges.
    const NODE_CAPACITY: u64 = 11;
    const MIN_NON_ROOT_LEN: u64 = 5;
    const NODE_HEADER_BYTES: u64 = 64;
    let len = value.len() as u64;
    let node_count = 1u64.saturating_add(len.saturating_sub(1) / MIN_NON_ROOT_LEN);
    let entry_bytes =
        (std::mem::size_of::<K>() as u64).saturating_add(std::mem::size_of::<V>() as u64);
    let node_bytes = NODE_HEADER_BYTES
        .saturating_add(NODE_CAPACITY.saturating_mul(entry_bytes))
        .saturating_add((NODE_CAPACITY + 1).saturating_mul(std::mem::size_of::<usize>() as u64));
    node_count.saturating_mul(node_bytes)
}
