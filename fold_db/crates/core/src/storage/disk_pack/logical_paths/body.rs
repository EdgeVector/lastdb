//! Count encoded bytes without a retained serialized copy of a core object.
use super::*;

pub(super) fn encoded_len(value: &impl Serialize) -> StorageResult<usize> {
    let mut length = EncodedLength::default();
    serde_json::to_writer(&mut length, value).map_err(|error| serialization(&error))?;
    Ok(length.0)
}

#[derive(Default)]
struct EncodedLength(usize);

impl std::io::Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("native core encoded length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
