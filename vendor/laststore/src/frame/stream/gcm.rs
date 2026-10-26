use super::{as_io, buffer_size, source::Source};
use crate::{read_receipt::TemporaryBytes, Error, Result};
use openssl::symm::{Cipher, Crypter, Mode};
use std::io::{self, BufRead, Read};
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) struct GcmReader {
    source: Arc<Source>,
    cipher: Crypter,
    input: Zeroizing<Vec<u8>>,
    plain: Zeroizing<Vec<u8>>,
    position: u64,
    begin: usize,
    end: usize,
    finalized: bool,
    _charge: TemporaryBytes,
}

impl GcmReader {
    pub fn new(source: Arc<Source>, data_key: &[u8; 32]) -> Result<Self> {
        let key = Zeroizing::new(super::super::chunk_key(data_key, source.header.chunk_uuid)?);
        let nonce = super::super::counter_nonce(source.header.counter);
        let mut cipher = Crypter::new(
            Cipher::aes_256_gcm(),
            Mode::Decrypt,
            key.as_ref(),
            Some(&nonce),
        )
        .map_err(|_| Error::Config("streamed frame cipher initialization failed".into()))?;
        cipher
            .aad_update(&source.header_bytes)
            .map_err(|_| Error::AeadAuthFail)?;
        cipher
            .set_tag(&source.tag)
            .map_err(|_| Error::AeadAuthFail)?;
        let input = buffer(buffer_size())?;
        let plain = buffer(buffer_size() + Cipher::aes_256_gcm().block_size())?;
        let charge = TemporaryBytes::new(input.capacity() + plain.capacity());
        Ok(Self {
            source,
            cipher,
            input,
            plain,
            position: 0,
            begin: 0,
            end: 0,
            finalized: false,
            _charge: charge,
        })
    }

    pub fn authenticate(mut self) -> Result<()> {
        while self.position < self.source.stored_bytes {
            self.fill()?;
            self.begin = self.end;
        }
        self.finish()
    }

    pub fn memory_bytes(&self) -> u64 {
        (self.input.capacity() + self.plain.capacity()) as u64
    }

    pub fn verify_consumed(&mut self) -> io::Result<()> {
        if self.position != self.source.stored_bytes || self.begin != self.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame has unconsumed encoded bytes",
            ));
        }
        self.finish().map_err(as_io)
    }

    fn fill(&mut self) -> Result<()> {
        if self.begin < self.end {
            return Ok(());
        }
        if self.position == self.source.stored_bytes {
            return self.finish();
        }
        let count =
            (self.source.stored_bytes - self.position).min(self.input.len() as u64) as usize;
        self.source
            .read_payload(&mut self.input[..count], self.position)?;
        let written = self
            .cipher
            .update(&self.input[..count], &mut self.plain)
            .map_err(|_| Error::AeadAuthFail)?;
        if written != count {
            return Err(Error::Corrupt("streamed GCM output length differs".into()));
        }
        self.position += count as u64;
        self.begin = 0;
        self.end = written;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !self.finalized {
            let count = self
                .cipher
                .finalize(&mut self.plain)
                .map_err(|_| Error::AeadAuthFail)?;
            if count != 0 {
                return Err(Error::Corrupt("streamed GCM final length differs".into()));
            }
            self.finalized = true;
            self.begin = 0;
            self.end = 0;
        }
        self.source.check()
    }
}

impl Read for GcmReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let available = self.fill_buf()?;
        let count = available.len().min(output.len());
        output[..count].copy_from_slice(&available[..count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for GcmReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.fill().map_err(as_io)?;
        Ok(&self.plain[self.begin..self.end])
    }

    fn consume(&mut self, count: usize) {
        self.begin += count.min(self.end - self.begin);
    }
}

fn buffer(size: usize) -> Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    bytes.resize(size, 0);
    Ok(bytes)
}
