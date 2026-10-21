//! Measure compact JSON bytes without allocating a serialized copy.

pub(crate) fn encoded_len(value: &impl serde::Serialize) -> serde_json::Result<usize> {
    let mut length = EncodedLength::default();
    serde_json::to_writer(&mut length, value)?;
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
