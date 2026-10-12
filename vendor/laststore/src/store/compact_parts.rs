use super::*;

/// Live records grouped by the source frame that holds them.
pub(super) type FrameMembers = BTreeMap<FrameKey, Vec<(String, Loc)>>;

/// Group the live set by source frame, with locations that have no frame apart.
pub(super) fn live_locations_by_source_frame(
    sh: &Shard,
) -> Result<(FrameMembers, Vec<(String, Loc)>)> {
    let mut by_source: BTreeMap<FrameKey, Vec<(String, Loc)>> = BTreeMap::new();
    let mut unframed: Vec<(String, Loc)> = Vec::new();
    for (id, loc) in sh.live_locations()? {
        match loc {
            Loc::Chunk {
                chunk_uuid,
                frame_idx,
                ..
            } => by_source
                .entry((chunk_uuid, frame_idx))
                .or_default()
                .push((id.clone(), loc)),
            // Not addressable by frame. An encrypted shard should not hold
            // these, but a mixed-packaging home must still compact.
            Loc::Legacy { .. } | Loc::Sorted { .. } => unframed.push((id.clone(), loc)),
        }
    }
    Ok((by_source, unframed))
}

/// Drop the old addressing once every source read is done.
pub(super) fn reset_addressing_for_compaction(sh: &mut Shard) {
    sh.clear_index();
    sh.frame_cache.clear();
    sh.frame_cache_order.clear();
    sh.clear_frame_locs();
    sh.open_buf = Vec::new();
    sh.open_buf_base = 0;
    sh.open_len = 0;
    sh.file_len = 0;
    sh.open_file = None;
    sh.open_chunk_uuid = None;
    sh.next_frame_counter = 0;
}

/// Append the seal record and the footer (with trailer) to the rewritten chunk.
pub(super) fn write_compacted_seal_and_footer(
    sh: &Shard,
    data_key: &[u8; 32],
    f: &mut File,
    chunk_uuid: Uuid,
    frame_idx: u64,
    next_csn: u64,
) -> Result<()> {
    let end_csn = next_csn.saturating_sub(1);
    let seal_payload = encode_seal_record(end_csn);
    let seal_header = FrameHeader {
        chunk_uuid,
        shard: sh.shard,
        start_csn: end_csn.saturating_add(1),
        counter: frame_idx,
    };
    let seal_encoded = encode_frame_for_shard(sh, data_key, seal_header, &seal_payload)?;
    f.write_all(&seal_encoded)?;
    // `encode_footer_record` derives the chunk's tombstones from
    // `sh.open_buf`, which is empty here. That is the right answer rather
    // than an omission: a rewrite emits `encode_put` for every live id and
    // no deletes, so a compacted chunk has no tombstones to record.
    let footer_payload = encode_footer_record(sh, chunk_uuid)?;
    let footer_header = FrameHeader {
        chunk_uuid,
        shard: sh.shard,
        start_csn: end_csn.saturating_add(1),
        counter: frame_idx.saturating_add(1),
    };
    let footer_encoded = encode_frame_for_shard(sh, data_key, footer_header, &footer_payload)?;
    let footer_offset = f.metadata()?.len();
    f.write_all(&footer_encoded)?;
    write_footer_trailer(f, footer_offset, footer_encoded.len() as u64)?;
    Ok(())
}
