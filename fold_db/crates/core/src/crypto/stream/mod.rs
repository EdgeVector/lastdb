//! Bounded verification of the existing encrypted file formats.
//!
//! File length never selects a memory allocation. The OpenSSL GCM provider
//! checks the original tag. Callers may expose private output only after Ok.
use super::{CryptoError, CryptoResult};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};

mod at_rest;
mod inflate;
mod reader;
pub use at_rest::open_at_rest_stream;

#[derive(Clone, Debug)]
pub struct VerifiedEnvelope {
    pub plaintext_bytes: u64,
    pub plaintext_sha256: [u8; 32],
    pub stored_sha256: [u8; 32],
    /// SHA-256 of the binary encryption envelope, without its ENC/ENB wrapper.
    pub envelope_sha256: [u8; 32],
}

/// Authenticate one exact original v1 file. No plaintext leaves this function.
pub fn verify_v1_stream(
    key: &[u8; 32],
    input: &mut (impl Read + Seek),
    encrypted_bytes: u64,
) -> CryptoResult<VerifiedEnvelope> {
    if input
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error(&error))?
        != encrypted_bytes
    {
        return Err(invalid("encrypted file length differs"));
    }
    let stored_sha256 = hash_input(input)?;
    input.rewind().map_err(|error| io_error(&error))?;
    let mut reader = reader::EnvelopeReader::new(input, key, encrypted_bytes)?;
    let mut output = CountHash::new(std::io::sink());
    copy(&mut reader, &mut output)?;
    Ok(output.proof(stored_sha256, stored_sha256))
}

pub(super) struct CountHash<W> {
    output: W,
    bytes: u64,
    hash: Sha256,
}
impl<W: Write> CountHash<W> {
    fn new(output: W) -> Self {
        Self {
            output,
            bytes: 0,
            hash: Sha256::new(),
        }
    }
    fn proof(self, stored_sha256: [u8; 32], envelope_sha256: [u8; 32]) -> VerifiedEnvelope {
        VerifiedEnvelope {
            plaintext_bytes: self.bytes,
            plaintext_sha256: self.hash.finalize().into(),
            stored_sha256,
            envelope_sha256,
        }
    }
}
impl<W: Write> Write for CountHash<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .filter(|count| *count <= lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES)
            .ok_or_else(|| std::io::Error::other("decoded stream exceeds backup total policy"))?;
        self.output.write_all(bytes)?;
        self.hash.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

fn copy(input: &mut impl Read, output: &mut impl Write) -> CryptoResult<()> {
    let mut buffer = zeroize::Zeroizing::new(vec![0; buffer_bytes()]);
    loop {
        let count = input.read(&mut buffer).map_err(|error| io_error(&error))?;
        if count == 0 {
            return Ok(());
        }
        output
            .write_all(&buffer[..count])
            .map_err(|error| io_error(&error))?;
    }
}

fn hash_input(input: &mut (impl Read + Seek)) -> CryptoResult<[u8; 32]> {
    input.rewind().map_err(|error| io_error(&error))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; buffer_bytes()];
    loop {
        let count = input.read(&mut buffer).map_err(|error| io_error(&error))?;
        if count == 0 {
            return Ok(hash.finalize().into());
        }
        hash.update(&buffer[..count]);
    }
}

fn buffer_bytes() -> usize {
    lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize
}
fn invalid(message: &str) -> CryptoError {
    CryptoError::InvalidFormat(message.into())
}
fn io_error(error: &std::io::Error) -> CryptoError {
    CryptoError::InvalidFormat(format!("encrypted stream I/O: {error}"))
}
