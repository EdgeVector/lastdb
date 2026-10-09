use super::*;

pub(super) fn encode_seal_record(end_csn: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(SEAL_RECORD_MAGIC.len() + 8);
    out.extend_from_slice(SEAL_RECORD_MAGIC);
    out.extend_from_slice(&end_csn.to_le_bytes());
    out
}

pub(super) fn decode_seal_record(payload: &[u8]) -> Result<u64> {
    if !is_seal_record(payload) {
        return Err(Error::Corrupt("missing seal record".into()));
    }
    Ok(u64::from_le_bytes(
        payload[SEAL_RECORD_MAGIC.len()..]
            .try_into()
            .expect("seal record length checked"),
    ))
}

pub(super) fn is_seal_record(payload: &[u8]) -> bool {
    payload.len() == SEAL_RECORD_MAGIC.len() + 8 && payload.starts_with(SEAL_RECORD_MAGIC)
}

pub(super) fn is_footer_record(payload: &[u8]) -> bool {
    payload.starts_with(FOOTER_RECORD_MAGIC)
}

pub(super) fn count_complete_records(
    payload: &[u8],
    allow_partial_last_frame: bool,
) -> Result<u64> {
    let mut off = 0usize;
    let mut records = 0u64;
    while off < payload.len() {
        match segfmt::parse_at(payload, off)? {
            Some(rec) => {
                off += rec.raw_len;
                records = records.saturating_add(1);
            }
            None if allow_partial_last_frame => break,
            None => return Err(Error::Corrupt("bad encrypted frame payload".into())),
        }
    }
    Ok(records)
}
pub(super) fn ensure_empty_migration_destination(destination: &Path) -> Result<()> {
    if !destination.exists() {
        return Ok(());
    }
    if fs::read_dir(destination)?.next().is_none() {
        return Ok(());
    }
    Err(Error::Config(format!(
        "migration destination must be empty: {}",
        destination.display()
    )))
}

pub(super) fn verify_migration_parity<F>(
    source: &LastStore,
    target: &LastStore,
    expected: &BTreeMap<String, u64>,
    transform: &mut F,
) -> Result<()>
where
    F: FnMut(&str, &str, &[u8]) -> Result<Vec<u8>>,
{
    let target_collections: Vec<_> = target
        .collections_on_disk()?
        .into_iter()
        .filter(|collection| expected.contains_key(collection))
        .collect();
    if target_collections != expected.keys().cloned().collect::<Vec<_>>() {
        return Err(Error::Corrupt(
            "migration destination collection set does not match source".into(),
        ));
    }
    for (collection, expected_count) in expected {
        let source_keys = source.list_prefix_keys(collection, "")?;
        let target_keys = target.list_prefix_keys(collection, "")?;
        if source_keys != target_keys {
            return Err(Error::Corrupt(format!(
                "migration key mismatch in collection {collection}"
            )));
        }
        let verified = source_keys.len() as u64;
        if verified != *expected_count {
            return Err(Error::Corrupt(format!(
                "migration count mismatch in {collection}: copied {expected_count}, verified {verified}"
            )));
        }

        // Keep parity reads in the same destination order as writes. A
        // lexicographic walk would thrash the bounded target warm set again
        // and create fresh encrypted tails merely while verifying the copy.
        let mut source_keys = source_keys
            .into_iter()
            .map(|id| (target.shard_of(&id), target.group_of(&id), id))
            .collect::<Vec<_>>();
        source_keys.sort_unstable();
        for (_, _, id) in source_keys {
            let source_body = source.get(collection, &id)?.ok_or_else(|| {
                Error::Corrupt(format!(
                    "migration source key disappeared: {collection}/{id}"
                ))
            })?;
            let expected_body = transform(collection, &id, &source_body)?;
            if target.get(collection, &id)?.as_deref() != Some(expected_body.as_slice()) {
                return Err(Error::Corrupt(format!(
                    "migration value mismatch for {collection}/{id}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    if path.exists() {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

pub(super) fn tag_backup_excluded_collection(sh: &Shard) -> Result<()> {
    if !sh.policy.backup_excluded {
        return Ok(());
    }
    if let Some(collection_dir) = collection_dir_for(sh) {
        fs::create_dir_all(collection_dir)?;
        let marker = collection_dir.join(".laststore-backup-excluded");
        if !marker.exists() {
            fs::write(&marker, format!("collection={}\n", sh.collection))?;
            let f = OpenOptions::new().read(true).open(&marker)?;
            durability::sync_dirty_file(&f)?;
            sync_dir(collection_dir)?;
        }
    }
    Ok(())
}

pub(super) fn collection_dir_for(sh: &Shard) -> Option<&Path> {
    sh.dir
        .ancestors()
        .find(|p| p.file_name().and_then(|s| s.to_str()) == Some(sh.collection.as_str()))
}

pub(super) fn encode_frame_for_shard(
    sh: &Shard,
    data_key: &[u8; 32],
    header: FrameHeader,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let encoded = frame::encode_frame_with_stats(data_key, header, payload)?;
    let mut by_collection = sh.frame_compression_stats.lock().expect("poison");
    let stats = by_collection.entry(sh.collection.clone()).or_default();
    stats.input_bytes = stats
        .input_bytes
        .saturating_add(encoded.compression.input_bytes);
    stats.stored_bytes = stats
        .stored_bytes
        .saturating_add(encoded.compression.stored_bytes);
    if encoded.compression.compressed {
        stats.compressed_frames = stats.compressed_frames.saturating_add(1);
    } else {
        stats.uncompressed_frames = stats.uncompressed_frames.saturating_add(1);
    }
    Ok(encoded.bytes)
}

pub(super) fn encode_segment_payload(
    sh: &Shard,
    start_csn: u64,
    payload: &[u8],
) -> Result<(Vec<u8>, Option<Uuid>, u64)> {
    let Some(data_key) = sh.data_key.as_ref() else {
        return Ok((payload.to_vec(), None, 0));
    };
    if payload.is_empty() {
        return Ok((Vec::new(), None, 0));
    }
    let chunk_uuid = Uuid::new_v4();
    let header = FrameHeader {
        chunk_uuid,
        shard: sh.shard,
        start_csn,
        counter: 0,
    };
    let encoded = encode_frame_for_shard(sh, data_key, header, payload)?;
    Ok((encoded, Some(chunk_uuid), 1))
}

pub(super) fn decode_segment_payload(
    sh: &Shard,
    seq: u64,
    disk_data: &[u8],
    last: bool,
) -> Result<(Vec<u8>, u64, Option<Uuid>, u64)> {
    let Some(data_key) = sh.data_key.as_ref() else {
        return Ok((disk_data.to_vec(), disk_data.len() as u64, None, 0));
    };

    let mut plaintext = Vec::new();
    let mut off = 0usize;
    let mut chunk_uuid = None;
    let mut next_frame_counter = 0u64;
    while off < disk_data.len() {
        if disk_data.len() - off < frame::min_encoded_len() {
            if last {
                break;
            }
            return Err(Error::Corrupt(format!("bad sealed encrypted seg {seq}")));
        }
        let frame_len = match frame::encoded_len(&disk_data[off..off + frame::header_size()]) {
            Ok(len) if off + len <= disk_data.len() => len,
            Ok(_) if last => break,
            Ok(_) => return Err(Error::Corrupt(format!("bad sealed encrypted seg {seq}"))),
            Err(e) => return Err(e),
        };
        let decoded = frame::decode_frame(data_key, &disk_data[off..off + frame_len])?;
        if decoded.header.shard != sh.shard {
            return Err(Error::AeadAuthFail);
        }
        if let Some(existing) = chunk_uuid {
            if existing != decoded.header.chunk_uuid {
                return Err(Error::AeadAuthFail);
            }
        } else {
            chunk_uuid = Some(decoded.header.chunk_uuid);
        }
        if decoded.header.start_csn != plaintext.len() as u64 {
            return Err(Error::AeadAuthFail);
        }
        plaintext.extend_from_slice(&decoded.payload);
        next_frame_counter = decoded.header.counter.saturating_add(1);
        off += frame_len;
    }
    Ok((plaintext, off as u64, chunk_uuid, next_frame_counter))
}
