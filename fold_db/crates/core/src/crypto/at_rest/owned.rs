//! Explicit bounds and buffer lifetimes for inactive backup/restore work.
use super::*;

/// Consume the stored buffer before decrypt/inflate. Existing formats and
/// primitives stay unchanged; ordinary serving reads do not call this helper.
pub fn open_at_rest_bounded_owned(
    key: &[u8; 32],
    mut stored: Vec<u8>,
    stored_max: u64,
    plain_max: u64,
) -> CryptoResult<Vec<u8>> {
    if stored_max == 0
        || plain_max == 0
        || plain_max.checked_add(1).is_none()
        || stored.len() as u64 > stored_max
        || usize::try_from(plain_max).is_err()
    {
        return Err(CryptoError::InvalidFormat(
            "owned encrypted value bound".into(),
        ));
    }
    let (ciphertext, deflated) = if let Some((deflated, _)) = decode_binary_ciphertext(&stored)? {
        stored.drain(..AT_REST_ENC_BINARY_PREFIX.len() + 1);
        (stored, deflated)
    } else {
        let deflated = stored.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes());
        if !deflated && !stored.starts_with(AT_REST_ENC_PREFIX.as_bytes()) {
            return Err(CryptoError::InvalidFormat(
                "encrypted value required".into(),
            ));
        }
        let ciphertext = B64
            .decode(&stored[AT_REST_ENC_PREFIX.len()..])
            .map_err(|_| CryptoError::InvalidFormat("owned encrypted base64".into()))?;
        drop(stored);
        (ciphertext, deflated)
    };
    let plain = decrypt_envelope(key, &ciphertext)?;
    drop(ciphertext);
    let plain = if deflated {
        inflate_owned(&plain, plain_max)?
    } else {
        plain
    };
    if plain.len() as u64 > plain_max {
        return Err(CryptoError::InvalidFormat(
            "owned decoded value bound".into(),
        ));
    }
    Ok(plain)
}

fn inflate_owned(compressed: &[u8], maximum: u64) -> CryptoResult<Vec<u8>> {
    use std::io::Read;
    let mut decoder = flate2::read::DeflateDecoder::new(compressed);
    // Fixed capacity avoids read_to_end doubling at the max+1 refusal byte.
    let mut output = Vec::with_capacity(maximum as usize);
    let mut buffer = [0; 65_536];
    loop {
        let count = decoder
            .read(&mut buffer)
            .map_err(|_| CryptoError::InvalidFormat("owned encrypted inflate".into()))?;
        if count == 0 {
            return Ok(output);
        }
        if output.len() as u64 + count as u64 > maximum {
            return Err(CryptoError::InvalidFormat(
                "owned decoded value bound".into(),
            ));
        }
        output.extend_from_slice(&buffer[..count]);
    }
}
