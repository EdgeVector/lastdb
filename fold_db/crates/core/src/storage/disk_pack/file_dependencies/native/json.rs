//! Stream the existing flat CAS JSON without a body-sized string or JSON tree.
use super::{invalid, FileDependency, StorageResult};
use std::io::{self, BufRead, Write};

pub(super) struct Json<R> {
    input: R,
}

pub(super) fn extract(
    input: impl BufRead,
    output: &mut impl Write,
    dependency: &FileDependency,
) -> StorageResult<u64> {
    let mut json = Json { input };
    json.expect(b'{')?;
    let mut seen = 0u8;
    let mut size = None;
    loop {
        if json.peek()? == Some(b'}') {
            json.take()?;
            break;
        }
        let key = json.small_string()?;
        json.expect(b':')?;
        let bit = match key.as_str() {
            "blob_ref" => 1,
            "content_sha256" => 2,
            "size" => 4,
            "local_cipher_suite" => 8,
            "bytes_b64" => 16,
            "access" => 32,
            _ => 0,
        };
        if bit != 0 && seen & bit != 0 {
            return Err(invalid("duplicate native CAS selector"));
        }
        seen |= bit;
        match key.as_str() {
            "blob_ref" if json.small_string()? != dependency.blob_ref() => {
                return Err(invalid("native CAS blob identity"))
            }
            "content_sha256" if json.small_string()? != dependency.file_hash().as_str() => {
                return Err(invalid("native CAS file digest"))
            }
            "blob_ref" | "content_sha256" => {}
            "size" => {
                size = Some(
                    json.number()?
                        .parse::<u64>()
                        .map_err(|_| invalid("native CAS size"))?,
                );
            }
            "local_cipher_suite" => {
                let suite = json.small_string()?;
                if !super::compatible_local_suite(&suite, dependency.encryption_suite()) {
                    return Err(invalid(
                        "native CAS per-file encryption suite is uncertified",
                    ));
                }
            }
            "bytes_b64" => json.string(output)?,
            _ => json.skip(0)?,
        }
        match json.peek()? {
            Some(b',') => {
                json.take()?;
                if json.peek()? == Some(b'}') {
                    return Err(invalid("native CAS JSON trailing comma"));
                }
            }
            Some(b'}') => {
                json.take()?;
                break;
            }
            _ => return Err(invalid("native CAS JSON member separator")),
        }
    }
    if seen & 31 != 31 || json.peek()?.is_some() {
        return Err(invalid("native CAS selector inventory"));
    }
    let size = size.ok_or_else(|| invalid("native CAS size"))?;
    if size.checked_add(cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES)
        != Some(dependency.expected_stored_bytes())
    {
        return Err(invalid("native CAS file size differs"));
    }
    Ok(size)
}

impl<R: BufRead> Json<R> {
    pub(super) fn peek(&mut self) -> StorageResult<Option<u8>> {
        loop {
            let byte = self.input.fill_buf()?.first().copied();
            if byte.is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t')) {
                self.input.consume(1);
            } else {
                return Ok(byte);
            }
        }
    }
    pub(super) fn take(&mut self) -> StorageResult<u8> {
        let byte = self
            .input
            .fill_buf()?
            .first()
            .copied()
            .ok_or_else(|| invalid("native CAS JSON ended"))?;
        self.input.consume(1);
        Ok(byte)
    }
    pub(super) fn expect(&mut self, expected: u8) -> StorageResult<()> {
        if self.peek()? != Some(expected) {
            return Err(invalid("native CAS JSON token"));
        }
        self.take()?;
        Ok(())
    }
    fn small_string(&mut self) -> StorageResult<String> {
        let mut bytes = Small {
            bytes: Vec::new(),
            overflow: false,
        };
        self.string(&mut bytes)?;
        if bytes.overflow {
            return Ok(String::new());
        }
        String::from_utf8(bytes.bytes).map_err(|_| invalid("native CAS selector UTF-8"))
    }
    fn number(&mut self) -> StorageResult<String> {
        self.peek()?;
        let mut bytes = Vec::new();
        while self
            .input
            .fill_buf()?
            .first()
            .is_some_and(|b| matches!(b, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'))
        {
            if bytes.len() == 64 {
                return Err(invalid("native CAS JSON number"));
            }
            bytes.push(self.take()?);
        }
        serde_json::from_slice::<serde_json::Number>(&bytes)
            .map_err(|_| invalid("native CAS JSON number"))?;
        String::from_utf8(bytes).map_err(|_| invalid("native CAS JSON number"))
    }
    fn skip(&mut self, depth: u16) -> StorageResult<()> {
        if depth == 128 {
            return Err(invalid("native CAS JSON depth"));
        }
        match self.peek()? {
            Some(b'"') => self.string(&mut io::sink())?,
            Some(open @ (b'{' | b'[')) => {
                self.take()?;
                let close = if open == b'{' { b'}' } else { b']' };
                if self.peek()? == Some(close) {
                    self.take()?;
                    return Ok(());
                }
                loop {
                    if open == b'{' {
                        self.string(&mut io::sink())?;
                        self.expect(b':')?;
                    }
                    self.skip(depth + 1)?;
                    match self.peek()? {
                        Some(byte) if byte == close => {
                            self.take()?;
                            break;
                        }
                        Some(b',') => {
                            self.take()?;
                        }
                        _ => return Err(invalid("native CAS JSON separator")),
                    }
                }
            }
            Some(b't') => self.literal(b"true")?,
            Some(b'f') => self.literal(b"false")?,
            Some(b'n') => self.literal(b"null")?,
            Some(_) => {
                self.number()?;
            }
            None => return Err(invalid("native CAS JSON value")),
        }
        Ok(())
    }
    fn literal(&mut self, expected: &[u8]) -> StorageResult<()> {
        for byte in expected {
            if self.take()? != *byte {
                return Err(invalid("native CAS JSON literal"));
            }
        }
        Ok(())
    }
}

struct Small {
    bytes: Vec<u8>,
    overflow: bool,
}
impl Write for Small {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if self.bytes.len() + input.len() <= super::super::DIRECTORY_MAX_BYTES as usize {
            self.bytes.extend_from_slice(input);
        } else {
            self.overflow = true;
        }
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
