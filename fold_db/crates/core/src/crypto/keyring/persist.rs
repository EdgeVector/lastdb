use super::super::envelope::{decrypt_envelope_v2, encrypt_envelope_v2, KeyId};
use super::super::error::{CryptoError, CryptoResult};
use super::{Dek, Entry, KeyPurpose, Keyring};

/// Magic prefix of a serialized keyring file (`keyring.enc`).
pub(super) const KEYRING_MAGIC: &[u8; 4] = b"FDKR";
/// Wire-format version of the `keyring.enc` container.
pub(super) const KEYRING_FORMAT_VERSION: u8 = 0x01;

/// Minimum on-disk size of a single serialized keyring entry, used to
/// bound the declared entry `count` against the buffer length before
/// pre-allocating.
const MIN_ENTRY_OVERHEAD: usize = 10 + 33;

impl Keyring {
    /// Serialize to `keyring.enc` bytes, wrapping every DEK under `kek`.
    ///
    /// Layout:
    /// ```text
    /// "FDKR" [format:1=0x01] [count:u32 BE]
    /// repeat count times:
    ///   [key_id:4] [purpose:1] [active:1] [wrapped_len:u32 BE] [wrapped]
    /// ```
    pub fn serialize(&self, kek: &[u8; 32]) -> CryptoResult<Vec<u8>> {
        let count: u32 = self.entries.len().try_into().map_err(|_| {
            CryptoError::EncryptionFailed("keyring has more entries than u32 can index".to_string())
        })?;

        let mut out = Vec::new();
        out.extend_from_slice(KEYRING_MAGIC);
        out.push(KEYRING_FORMAT_VERSION);
        out.extend_from_slice(&count.to_be_bytes());

        for e in &self.entries {
            let wrapped =
                encrypt_envelope_v2(kek, e.key_id, e.dek.as_bytes(), &e.purpose.wrap_aad())?;
            let wrapped_len: u32 = wrapped.len().try_into().map_err(|_| {
                CryptoError::EncryptionFailed("wrapped DEK longer than u32 can index".to_string())
            })?;

            out.extend_from_slice(e.key_id.as_bytes());
            out.push(e.purpose.wire_tag());
            out.push(u8::from(e.active));
            out.extend_from_slice(&wrapped_len.to_be_bytes());
            out.extend_from_slice(&wrapped);
        }

        Ok(out)
    }

    /// Reconstruct a keyring from `keyring.enc` bytes, unwrapping every
    /// DEK under `kek`.
    pub fn deserialize(bytes: &[u8], kek: &[u8; 32]) -> CryptoResult<Self> {
        let mut cur = bytes;

        let magic = take(&mut cur, 4, "magic")?;
        if magic != KEYRING_MAGIC {
            return Err(CryptoError::InvalidFormat(
                "keyring file: bad magic (not a keyring.enc)".to_string(),
            ));
        }
        let version = take(&mut cur, 1, "format version")?[0];
        if version != KEYRING_FORMAT_VERSION {
            return Err(CryptoError::UnsupportedVersion(version));
        }
        let count = u32::from_be_bytes(
            take(&mut cur, 4, "entry count")?
                .try_into()
                .expect("took exactly 4 bytes"),
        );

        let max_possible = cur.len() / MIN_ENTRY_OVERHEAD;
        if count as usize > max_possible {
            return Err(CryptoError::InvalidFormat(format!(
                "keyring declares {count} entries but only {} byte(s) remain \
                 (at most {max_possible} possible) — truncated or tampered count",
                cur.len()
            )));
        }

        let mut entries: Vec<Entry> = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key_id = {
                let mut id = [0u8; 4];
                id.copy_from_slice(take(&mut cur, 4, "key_id")?);
                KeyId::from_bytes(id)
            };
            let purpose = KeyPurpose::from_wire_tag(take(&mut cur, 1, "purpose")?[0])?;
            let active = take(&mut cur, 1, "active flag")?[0] != 0;
            let wrapped_len = u32::from_be_bytes(
                take(&mut cur, 4, "wrapped length")?
                    .try_into()
                    .expect("took exactly 4 bytes"),
            ) as usize;
            let wrapped = take(&mut cur, wrapped_len, "wrapped DEK")?;

            let dek_bytes = decrypt_envelope_v2(kek, wrapped, &purpose.wrap_aad())?;
            if dek_bytes.len() != 32 {
                return Err(CryptoError::InvalidFormat(format!(
                    "keyring entry {}: unwrapped DEK is {} bytes (expected 32)",
                    key_id.to_u32(),
                    dek_bytes.len()
                )));
            }
            let mut dek = [0u8; 32];
            dek.copy_from_slice(&dek_bytes);

            if entries.iter().any(|e| e.key_id == key_id) {
                return Err(CryptoError::InvalidFormat(format!(
                    "keyring has duplicate key_id {}",
                    key_id.to_u32()
                )));
            }
            entries.push(Entry {
                key_id,
                purpose,
                active,
                dek: Dek::from_bytes(dek),
            });
        }

        if !cur.is_empty() {
            return Err(CryptoError::InvalidFormat(format!(
                "keyring file has {} trailing byte(s) after {count} entries",
                cur.len()
            )));
        }

        for purpose in KeyPurpose::ALL {
            if entries
                .iter()
                .filter(|e| e.purpose == purpose && e.active)
                .count()
                > 1
            {
                return Err(CryptoError::InvalidFormat(format!(
                    "keyring has more than one active DEK for purpose {purpose:?}"
                )));
            }
        }

        Ok(Self { entries })
    }
}

/// Split `n` bytes off the front of `cur`, advancing it; error if short.
fn take<'a>(cur: &mut &'a [u8], n: usize, field: &str) -> CryptoResult<&'a [u8]> {
    if cur.len() < n {
        return Err(CryptoError::InvalidFormat(format!(
            "keyring file truncated reading {field}: need {n} bytes, have {}",
            cur.len()
        )));
    }
    let (head, tail) = cur.split_at(n);
    *cur = tail;
    Ok(head)
}
