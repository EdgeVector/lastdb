use super::*;
use openssl::symm::{Cipher, Crypter, Mode};

/// Plaintext is provisional until the final read authenticates the tag and EOF.
pub(super) struct EnvelopeReader<R> {
    input: R,
    cipher: Crypter,
    remaining: u64,
    ciphertext: Vec<u8>,
    plaintext: zeroize::Zeroizing<Vec<u8>>,
    offset: usize,
    count: usize,
    complete: bool,
    failed: bool,
}
impl<R: Read> EnvelopeReader<R> {
    pub fn new(mut input: R, key: &[u8; 32], bytes: u64) -> CryptoResult<Self> {
        if bytes < 29 {
            return Err(invalid("truncated v1 envelope"));
        }
        let mut header = [0; 13];
        input
            .read_exact(&mut header)
            .map_err(|error| io_error(&error))?;
        if header[0] != 1 {
            return Err(CryptoError::UnsupportedVersion(header[0]));
        }
        let mut cipher = Crypter::new(
            Cipher::aes_256_gcm(),
            Mode::Decrypt,
            key,
            Some(&header[1..]),
        )
        .map_err(|_| invalid("stream cipher initialization"))?;
        cipher.pad(false);
        Ok(Self {
            input,
            cipher,
            remaining: bytes - 29,
            ciphertext: vec![0; buffer_bytes()],
            plaintext: zeroize::Zeroizing::new(vec![0; buffer_bytes() + 16]),
            offset: 0,
            count: 0,
            complete: false,
            failed: false,
        })
    }

    fn fill(&mut self) -> std::io::Result<()> {
        self.offset = 0;
        if self.remaining > 0 {
            let count = self.remaining.min(self.ciphertext.len() as u64) as usize;
            self.input.read_exact(&mut self.ciphertext[..count])?;
            self.count = self
                .cipher
                .update(&self.ciphertext[..count], &mut self.plaintext)
                .map_err(|_| std::io::Error::other("encrypted stream update failed"))?;
            self.remaining -= count as u64;
        } else {
            let mut tag = [0; 16];
            self.input.read_exact(&mut tag)?;
            let mut extra = [0];
            if self.input.read(&mut extra)? != 0 {
                return Err(std::io::Error::other("encrypted stream has trailing bytes"));
            }
            self.cipher
                .set_tag(&tag)
                .map_err(|_| std::io::Error::other("encrypted stream tag format"))?;
            self.count = self
                .cipher
                .finalize(&mut self.plaintext)
                .map_err(|_| std::io::Error::other("encrypted stream authentication failed"))?;
            self.complete = true;
        }
        Ok(())
    }
}
impl<R: Read> Read for EnvelopeReader<R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.failed {
            return Err(std::io::Error::other("encrypted stream already failed"));
        }
        if output.is_empty() {
            return Ok(0);
        }
        while self.offset == self.count {
            if self.complete {
                return Ok(0);
            }
            if let Err(error) = self.fill() {
                self.failed = true;
                return Err(error);
            }
        }
        let count = output.len().min(self.count - self.offset);
        output[..count].copy_from_slice(&self.plaintext[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}
