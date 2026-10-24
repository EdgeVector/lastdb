use super::*;
use base64::{engine::general_purpose::STANDARD, read::DecoderReader};

/// Open original ENC/ENZ/ENB bytes into empty private scratch. The caller must
/// not expose any output before success, and must discard output on failure.
pub fn open_at_rest_stream(
    key: &[u8; 32],
    input: &mut (impl Read + Seek),
    output: &mut impl Write,
) -> CryptoResult<VerifiedEnvelope> {
    let bytes = input
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error(&error))?;
    if bytes > lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES {
        return Err(invalid("stored stream exceeds complete backup policy"));
    }
    let stored_sha256 = hash_input(input)?;
    input.rewind().map_err(|error| io_error(&error))?;
    let mut prefix = [0; 4];
    input
        .read_exact(&mut prefix)
        .map_err(|error| io_error(&error))?;
    match &prefix {
        b"ENB:" => {
            let mut flags = [0];
            input
                .read_exact(&mut flags)
                .map_err(|error| io_error(&error))?;
            if flags[0] & !1 != 0 {
                return Err(invalid("binary stream flags"));
            }
            let length = bytes
                .checked_sub(5)
                .ok_or_else(|| invalid("binary stream length"))?;
            decode(key, input, length, flags[0] == 1, output, stored_sha256)
        }
        b"ENC:" | b"ENZ:" => {
            let encoded = bytes
                .checked_sub(4)
                .filter(|n| *n >= 4 && *n % 4 == 0)
                .ok_or_else(|| invalid("text stream length"))?;
            input
                .seek(SeekFrom::End(-2))
                .map_err(|error| io_error(&error))?;
            let mut tail = [0; 2];
            input
                .read_exact(&mut tail)
                .map_err(|error| io_error(&error))?;
            let padding = u64::from(tail[1] == b'=') + u64::from(tail[0] == b'=');
            let length = (encoded / 4)
                .checked_mul(3)
                .and_then(|n| n.checked_sub(padding))
                .ok_or_else(|| invalid("decoded stream length"))?;
            input
                .seek(SeekFrom::Start(4))
                .map_err(|error| io_error(&error))?;
            let decoder = DecoderReader::new(input, &STANDARD);
            decode(
                key,
                decoder,
                length,
                &prefix == b"ENZ:",
                output,
                stored_sha256,
            )
        }
        _ => Err(invalid("encrypted stream prefix required")),
    }
}

fn decode<R: Read>(
    key: &[u8; 32],
    input: R,
    length: u64,
    compressed: bool,
    output: &mut impl Write,
    stored_sha256: [u8; 32],
) -> CryptoResult<VerifiedEnvelope> {
    let mut envelope_hash = Sha256::new();
    let input = HashReader {
        input,
        hash: &mut envelope_hash,
    };
    let reader = reader::EnvelopeReader::new(input, key, length)?;
    let mut output = CountHash::new(output);
    if compressed {
        inflate::copy(reader, &mut output)?;
    } else {
        let mut reader = reader;
        copy(&mut reader, &mut output)?;
    }
    Ok(output.proof(stored_sha256, envelope_hash.finalize().into()))
}

struct HashReader<'a, R> {
    input: R,
    hash: &'a mut Sha256,
}

impl<R: Read> Read for HashReader<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let count = self.input.read(output)?;
        self.hash.update(&output[..count]);
        Ok(count)
    }
}
