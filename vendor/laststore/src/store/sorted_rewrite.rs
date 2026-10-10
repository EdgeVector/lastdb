use super::*;

pub(super) enum RewriteBody<'a> {
    Buffered(&'a [u8]),
    Frame {
        offset: usize,
    },
    File(std::io::Take<File>),
    Sealed {
        segment: usize,
        offset: u64,
    },
    Located {
        cursor: sorted::Cursor<'a>,
        offset: u64,
    },
}

/// A physical rewrite retains source cursors and a single selected body
/// location. The sorted writer supplies a fixed-size destination buffer.
pub(super) struct SortedRewrite<'a> {
    shard: &'a Shard,
    merge: Merge<'a, Loc>,
    keep_deletes: bool,
    body: Option<RewriteBody<'a>>,
    remaining: usize,
    frame: Option<(FrameKey, Vec<u8>)>,
}

impl<'a> SortedRewrite<'a> {
    pub(super) fn new(shard: &'a Shard, tail_only: bool) -> Result<Self> {
        Ok(Self {
            shard,
            merge: Merge::new(
                if tail_only {
                    &[]
                } else {
                    &shard.sorted_segments
                },
                shard.data_key.as_ref(),
                &shard.index,
                "",
            )?,
            keep_deletes: tail_only,
            body: None,
            remaining: 0,
            frame: None,
        })
    }

    fn tail_body(&mut self, id: &str, location: Loc) -> Result<(RewriteBody<'a>, usize)> {
        match location {
            Loc::Legacy { seg, offset, len } => {
                if self.shard.segments.last() == Some(&seg) {
                    if let Some(start) = offset.checked_sub(self.shard.open_buf_base) {
                        if let Some(line) = usize::try_from(start).ok().and_then(|start| {
                            usize::try_from(len)
                                .ok()
                                .and_then(|len| start.checked_add(len))
                                .and_then(|end| self.shard.open_buf.get(start..end))
                        }) {
                            let body_start = 7 + id.len();
                            if line.len() < body_start
                                || line[0] != segfmt::OP_PUT
                                || u16::from_le_bytes(line[1..3].try_into().unwrap()) as usize
                                    != id.len()
                                || &line[3..3 + id.len()] != id.as_bytes()
                            {
                                return Err(Error::Corrupt(
                                    "buffered rewrite key differs from its extent".into(),
                                ));
                            }
                            let body_len = u32::from_le_bytes(
                                line[body_start - 4..body_start].try_into().unwrap(),
                            ) as usize;
                            if body_start.checked_add(body_len) != Some(line.len()) {
                                return Err(Error::Corrupt(
                                    "buffered rewrite body differs from its extent".into(),
                                ));
                            }
                            return Ok((RewriteBody::Buffered(&line[body_start..]), body_len));
                        }
                    }
                }
                // The caller syncs the tail before the rewrite. Read its
                // record header, then stream the body directly from disk.
                if self.shard.data_key.is_some() {
                    return Err(Error::Corrupt(
                        "encrypted rollback bytes are unavailable".into(),
                    ));
                }
                let mut file = File::open(self.shard.dir.join(format!("{seg:010}.seg")))?;
                file.seek(SeekFrom::Start(offset))?;
                let mut prefix = [0; 3];
                file.read_exact(&mut prefix)?;
                let key_len = u16::from_le_bytes(prefix[1..].try_into().unwrap()) as usize;
                if prefix[0] != segfmt::OP_PUT || key_len != id.len() {
                    return Err(Error::Corrupt(
                        "rewrite tail record header differs from its key".into(),
                    ));
                }
                let mut header = vec![0; key_len + 4];
                file.read_exact(&mut header)?;
                let body_len = u32::from_le_bytes(header[key_len..].try_into().unwrap()) as usize;
                if &header[..key_len] != id.as_bytes() || len != (7 + key_len + body_len) as u64 {
                    return Err(Error::Corrupt(
                        "rewrite tail record differs from its indexed extent".into(),
                    ));
                }
                Ok((RewriteBody::File(file.take(body_len as u64)), body_len))
            }
            Loc::Sorted {
                segment,
                offset,
                body_len,
                ..
            } => {
                let segment = self.shard.sorted_segments.get(segment).ok_or_else(|| {
                    Error::Corrupt("rewrite location names a missing sorted segment".into())
                })?;
                Ok((
                    RewriteBody::Located {
                        cursor: segment.cursor(self.shard.data_key.as_ref(), b"")?,
                        offset,
                    },
                    body_len,
                ))
            }
            Loc::Chunk {
                chunk_uuid,
                frame_idx,
                offset_in_frame,
                len,
            } => {
                let key = (chunk_uuid, frame_idx);
                let pending = Some(chunk_uuid) == self.shard.open_chunk_uuid
                    && frame_idx == self.shard.next_frame_counter;
                let body_start = 7 + id.len();
                let validate = |bytes: &[u8], offset: u64| -> Result<(usize, usize)> {
                    let start = usize::try_from(offset)
                        .map_err(|_| Error::Corrupt("tail offset overflow".into()))?;
                    let end = start
                        .checked_add(len as usize)
                        .ok_or_else(|| Error::Corrupt("tail extent overflow".into()))?;
                    let line = bytes
                        .get(start..end)
                        .ok_or_else(|| Error::Corrupt("tail extent exceeds frame".into()))?;
                    if line.len() < body_start
                        || line[0] != segfmt::OP_PUT
                        || u16::from_le_bytes(line[1..3].try_into().unwrap()) as usize != id.len()
                        || &line[3..3 + id.len()] != id.as_bytes()
                    {
                        return Err(Error::Corrupt(
                            "tail rewrite key differs from its frame".into(),
                        ));
                    }
                    let length =
                        u32::from_le_bytes(line[body_start - 4..body_start].try_into().unwrap())
                            as usize;
                    if body_start.checked_add(length) != Some(line.len()) {
                        return Err(Error::Corrupt(
                            "tail rewrite body differs from its frame".into(),
                        ));
                    }
                    Ok((start + body_start, length))
                };
                if pending {
                    let offset = self.shard.file_len + offset_in_frame - self.shard.open_buf_base;
                    let (start, length) = validate(&self.shard.open_buf, offset)?;
                    return Ok((
                        RewriteBody::Buffered(&self.shard.open_buf[start..start + length]),
                        length,
                    ));
                }
                if let Some(bytes) = self.shard.frame_cache.get(&key) {
                    let (start, length) = validate(bytes, offset_in_frame)?;
                    return Ok((RewriteBody::Buffered(&bytes[start..start + length]), length));
                }
                let location = self
                    .shard
                    .frame_locs
                    .get(&key)
                    .ok_or_else(|| Error::Corrupt("tail frame is unavailable".into()))?;
                if self.frame.as_ref().map(|(cached, _)| *cached) != Some(key) {
                    self.frame = Some((
                        key,
                        read_encrypted_frame_uncached(self.shard, key, location)?,
                    ));
                }
                let (start, length) =
                    validate(&self.frame.as_ref().expect("frame").1, offset_in_frame)?;
                Ok((RewriteBody::Frame { offset: start }, length))
            }
        }
    }
}

impl sorted::RecordSource for SortedRewrite<'_> {
    fn next_record(&mut self) -> Result<Option<sorted::RecordHead>> {
        if self.remaining != 0 {
            return Err(Error::Corrupt(
                "rewrite advanced before consuming its selected body".into(),
            ));
        }
        self.body = None;
        loop {
            let Some(record) = self.merge.next_key()? else {
                return Ok(None);
            };
            let body = match record.body {
                None if !self.keep_deletes => continue,
                None => None,
                Some(BodyLocation::Tail(location)) => Some(self.tail_body(&record.key, location)?),
                Some(BodyLocation::Sealed {
                    segment,
                    offset,
                    length,
                }) => Some((RewriteBody::Sealed { segment, offset }, length)),
            };
            let body_len = body.as_ref().map(|(_, length)| *length);
            if let Some((body, length)) = body {
                self.body = Some(body);
                self.remaining = length;
            }
            return Ok(Some(sorted::RecordHead {
                key: record.key,
                body_len,
            }));
        }
    }

    fn read_body(&mut self, destination: &mut [u8]) -> Result<()> {
        if destination.len() > self.remaining {
            return Err(Error::Corrupt(
                "rewrite read exceeds its selected body".into(),
            ));
        }
        match self
            .body
            .as_mut()
            .ok_or_else(|| Error::Corrupt("rewrite has no selected body".into()))?
        {
            RewriteBody::Buffered(bytes) => bytes.read_exact(destination)?,
            RewriteBody::Frame { offset } => {
                let frame = &self
                    .frame
                    .as_ref()
                    .ok_or_else(|| Error::Corrupt("rewrite frame is absent".into()))?
                    .1;
                let end = offset
                    .checked_add(destination.len())
                    .ok_or_else(|| Error::Corrupt("rewrite frame offset overflow".into()))?;
                destination.copy_from_slice(frame.get(*offset..end).ok_or_else(|| {
                    Error::Corrupt("rewrite frame extent exceeds payload".into())
                })?);
                *offset = end;
            }
            RewriteBody::File(file) => file.read_exact(destination)?,
            RewriteBody::Sealed { segment, offset } => {
                self.merge
                    .read_sealed_into(*segment, *offset, destination)?;
                *offset += destination.len() as u64;
            }
            RewriteBody::Located { cursor, offset } => {
                cursor.read_into(*offset, destination)?;
                *offset += destination.len() as u64;
            }
        }
        self.remaining -= destination.len();
        Ok(())
    }
}
