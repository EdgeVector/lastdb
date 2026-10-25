//! File length does not determine allocation. Every chunk uses fixed buffers.
use super::*;

pub(super) fn plan(
    inner: &Inner,
    state: &mut State,
    index: &mut u64,
    file: &SnapshotFile,
) -> StorageResult<()> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    let path = RelativePath::try_from(
        file.path
            .to_str()
            .ok_or_else(|| invalid("byte native UTF8 path"))?
            .to_owned(),
    )
    .map_err(protocol)?;
    let mut reader = inner.home.reader(source, &file.path)?;
    let mut file_hash = Sha256::new();
    let mut offset = 0u64;
    let mut extents = Vec::new();
    let mut buffer = [0u8; 65536];
    while offset < file.bytes {
        let length = OBJECT_BYTES.min(file.bytes - offset);
        let mut hash = Sha256::new();
        let mut remaining = length;
        while remaining > 0 {
            let count = buffer.len().min(remaining as usize);
            reader.read_exact(&mut buffer[..count])?;
            hash.update(&buffer[..count]);
            file_hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        let input = input(
            inner,
            state,
            *index,
            Object {
                sha256: hex(hash.finalize().into())?,
                bytes: DecimalU64::new(length),
                destination: None,
            },
        )?;
        extents.push(ClientExtent {
            receipt_id: expected_receipt(&input)?.receipt_id,
            offset: DecimalU64::new(0),
            length: DecimalU64::new(length),
        });
        if extents.len() > super::super::codec::MAX_FILE_EXTENTS {
            return Err(invalid("byte native file extent metadata bound"));
        }
        let last = offset + length == file.bytes;
        let native_files = if last {
            vec![ClientNativeFile {
                path: path.clone(),
                native_sha256: hex(file_hash.clone().finalize().into())?,
                native_bytes: DecimalU64::new(file.bytes),
                extents: extents.clone(),
            }]
        } else {
            Vec::new()
        };
        save(
            inner,
            state,
            *index,
            ObjectRecord {
                input,
                segments: vec![Segment {
                    path: path.clone(),
                    offset,
                    bytes: length,
                }],
                native_files,
                metadata_pack: false,
                ciphertext: None,
                receipt: None,
                record_segments: Vec::new(),
                client_stream: None,
            },
        )?;
        *index += 1;
        offset += length;
    }
    if reader.read(&mut buffer[..1])? != 0 {
        return Err(invalid("byte native source length changed"));
    }
    Ok(())
}
