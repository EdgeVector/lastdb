use super::json::Json;
use super::{invalid, StorageResult};
use std::io::{BufRead, Write};

impl<R: BufRead> Json<R> {
    /// Decode a JSON string directly to bounded disk output. The temporary
    /// scalar buffer never depends on the selected string's total length.
    pub(super) fn string(&mut self, output: &mut impl Write) -> StorageResult<()> {
        self.expect(b'"')?;
        loop {
            let byte = self.take()?;
            match byte {
                b'"' => return Ok(()),
                b'\\' => self.escape(output)?,
                0..=31 => return Err(invalid("native CAS JSON control byte")),
                32..=127 => output.write_all(&[byte])?,
                _ => {
                    let count = match byte {
                        0xc2..=0xdf => 2,
                        0xe0..=0xef => 3,
                        0xf0..=0xf4 => 4,
                        _ => return Err(invalid("native CAS JSON UTF-8")),
                    };
                    let mut scalar = [0; 4];
                    scalar[0] = byte;
                    for item in &mut scalar[1..count] {
                        *item = self.take()?;
                    }
                    std::str::from_utf8(&scalar[..count])
                        .map_err(|_| invalid("native CAS JSON UTF-8"))?;
                    output.write_all(&scalar[..count])?;
                }
            }
        }
    }

    fn escape(&mut self, output: &mut impl Write) -> StorageResult<()> {
        let byte = match self.take()? {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 8,
            b'f' => 12,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => {
                let first = self.hex4()?;
                let scalar = if (0xd800..=0xdbff).contains(&first) {
                    if self.take()? != b'\\' || self.take()? != b'u' {
                        return Err(invalid("native CAS JSON surrogate"));
                    }
                    let second = self.hex4()?;
                    if !(0xdc00..=0xdfff).contains(&second) {
                        return Err(invalid("native CAS JSON surrogate"));
                    }
                    0x10000 + ((first - 0xd800) << 10) + (second - 0xdc00)
                } else {
                    first
                };
                let scalar =
                    char::from_u32(scalar).ok_or_else(|| invalid("native CAS JSON scalar"))?;
                let mut bytes = [0; 4];
                output.write_all(scalar.encode_utf8(&mut bytes).as_bytes())?;
                return Ok(());
            }
            _ => return Err(invalid("native CAS JSON escape")),
        };
        output.write_all(&[byte])?;
        Ok(())
    }

    fn hex4(&mut self) -> StorageResult<u32> {
        let mut value = 0;
        for _ in 0..4 {
            let digit = match self.take()? {
                byte @ b'0'..=b'9' => byte - b'0',
                byte @ b'a'..=b'f' => byte - b'a' + 10,
                byte @ b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(invalid("native CAS JSON hex")),
            };
            value = value * 16 + u32::from(digit);
        }
        Ok(value)
    }
}
