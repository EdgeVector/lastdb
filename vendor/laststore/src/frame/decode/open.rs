//! Native AES-GCM for large ordinary reads; plaintext never escapes before the tag check.

use super::*;
use crate::read_receipt::TemporaryBytes;
use openssl::symm::{Cipher, Crypter, Mode};
use zeroize::Zeroize;

// Tiny frames retain the prepared Rust cipher and avoid a native context setup.
const NATIVE_MIN_BYTES: usize = 4096;

pub(super) struct Input<'a> {
    pub nonce: [u8; 12],
    pub header: &'a [u8],
    pub ciphertext: &'a [u8],
    pub tag: &'a [u8],
}

pub(super) fn payload(
    key: &PreparedFrameKey,
    input: Input<'_>,
    ordinary: bool,
) -> Result<(Vec<u8>, TemporaryBytes)> {
    let native = ordinary && input.ciphertext.len() >= NATIVE_MIN_BYTES;
    let extra = if native {
        Cipher::aes_256_gcm().block_size()
    } else {
        0
    };
    let capacity = input
        .ciphertext
        .len()
        .checked_add(extra)
        .ok_or_else(|| Error::Corrupt("frame output length overflow".into()))?;
    let mut payload = vec![0u8; capacity];
    let charge = TemporaryBytes::new(payload.capacity());
    if !native {
        payload.copy_from_slice(input.ciphertext);
    }
    let timer = Timer::start();
    let opened = if native {
        open_native(key, &input, &mut payload)
    } else {
        key.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&input.nonce),
                input.header,
                &mut payload,
                Tag::from_slice(input.tag),
            )
            .map_err(|_| Error::AeadAuthFail)
    };
    let bytes = (input.header.len() + input.ciphertext.len() + input.tag.len()) as u64;
    timer.finish(|receipt, nanos| {
        receipt.frame_auth_nanos = receipt.frame_auth_nanos.saturating_add(nanos);
        receipt.frame_authenticated_bytes = receipt.frame_authenticated_bytes.saturating_add(bytes);
        if native {
            receipt.frame_native_opens = receipt.frame_native_opens.saturating_add(1);
            receipt.frame_native_bytes = receipt.frame_native_bytes.saturating_add(bytes);
        }
    });
    if let Err(error) = opened {
        payload.zeroize();
        return Err(error);
    }
    Ok((payload, charge))
}

fn open_native(key: &PreparedFrameKey, input: &Input<'_>, output: &mut Vec<u8>) -> Result<()> {
    let mut cipher = Crypter::new(
        Cipher::aes_256_gcm(),
        Mode::Decrypt,
        key.raw_key.as_ref(),
        Some(&input.nonce),
    )
    .map_err(|_| Error::AeadAuthFail)?;
    cipher
        .aad_update(input.header)
        .map_err(|_| Error::AeadAuthFail)?;
    cipher.set_tag(input.tag).map_err(|_| Error::AeadAuthFail)?;
    let count = cipher
        .update(input.ciphertext, output)
        .map_err(|_| Error::AeadAuthFail)?;
    let rest = cipher
        .finalize(&mut output[count..])
        .map_err(|_| Error::AeadAuthFail)?;
    if count.checked_add(rest) != Some(input.ciphertext.len()) {
        return Err(Error::Corrupt("frame GCM output length differs".into()));
    }
    output.truncate(count + rest);
    Ok(())
}
