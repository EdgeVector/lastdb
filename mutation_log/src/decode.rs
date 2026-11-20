//! Decode `signed_bytes` payloads (bundle body today).

use crate::entry::BundleBody;
use crate::ids::{DeviceId, Hex32};
use crate::kinds::domain;
use thiserror::Error;

/// Decode failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Truncated buffer.
    #[error("truncated signed bytes")]
    Truncated,
    /// Unknown domain string.
    #[error("unknown domain")]
    UnknownDomain,
    /// Bundle body malformed.
    #[error("malformed bundle body")]
    BadBundleBody,
    /// grant_at flag without complete fields.
    #[error("grant_at requires both device and seq")]
    GrantAtIncomplete,
}

/// Decode a bundle entry's body tail (after the common header + seen_heads).
pub fn decode_bundle_body(body: &[u8]) -> Result<BundleBody, DecodeError> {
    let mut off = 0;
    let blob_sha256 = take_hash(body, &mut off)?;
    let blob_size = take_u64(body, &mut off)?;
    let change_count = take_u32(body, &mut off)?;
    let flags = take_u8(body, &mut off)?;
    let (grant_at_device, grant_at_seq) = if flags & 0x04 != 0 {
        let device: DeviceId = take_hash(body, &mut off)?;
        let seq = take_u64(body, &mut off)?;
        (Some(device), Some(seq))
    } else {
        (None, None)
    };
    if off != body.len() || flags & !0x04 != 0 {
        return Err(DecodeError::BadBundleBody);
    }
    Ok(BundleBody {
        blob_sha256,
        blob_size,
        change_count,
        grant_at_device,
        grant_at_seq,
    })
}

/// Strip domain prefix and decode bundle `BundleBody` from full `signed_bytes`.
pub fn decode_bundle_signed_bytes(bytes: &[u8]) -> Result<BundleBody, DecodeError> {
    let domain = domain::BUNDLE;
    let need = domain.len() + 1;
    if bytes.len() < need {
        return Err(DecodeError::Truncated);
    }
    if &bytes[..domain.len()] != domain.as_bytes() || bytes[domain.len()] != 0 {
        return Err(DecodeError::UnknownDomain);
    }
    let header_len = need + 32 + 32 + 32 + 8 + 32 + 8 + 8;
    if bytes.len() < header_len + 2 {
        return Err(DecodeError::Truncated);
    }
    let seen_count = u16::from_be_bytes([bytes[header_len], bytes[header_len + 1]]) as usize;
    let tail_start = header_len + 2 + seen_count * 32;
    if bytes.len() < tail_start {
        return Err(DecodeError::Truncated);
    }
    decode_bundle_body(&bytes[tail_start..])
}

fn take_slice<'a>(buf: &'a [u8], off: &mut usize, len: usize) -> Result<&'a [u8], DecodeError> {
    if buf.len() < *off + len {
        return Err(DecodeError::Truncated);
    }
    let slice = &buf[*off..*off + len];
    *off += len;
    Ok(slice)
}

fn take_u8(buf: &[u8], off: &mut usize) -> Result<u8, DecodeError> {
    let s = take_slice(buf, off, 1)?;
    Ok(s[0])
}

fn take_u32(buf: &[u8], off: &mut usize) -> Result<u32, DecodeError> {
    let s = take_slice(buf, off, 4)?;
    Ok(u32::from_be_bytes(
        s.try_into().map_err(|_| DecodeError::Truncated)?,
    ))
}

fn take_u64(buf: &[u8], off: &mut usize) -> Result<u64, DecodeError> {
    let s = take_slice(buf, off, 8)?;
    Ok(u64::from_be_bytes(
        s.try_into().map_err(|_| DecodeError::Truncated)?,
    ))
}

fn take_hash(buf: &[u8], off: &mut usize) -> Result<Hex32, DecodeError> {
    let s = take_slice(buf, off, 32)?;
    let raw: [u8; 32] = s.try_into().map_err(|_| DecodeError::Truncated)?;
    Ok(Hex32::from_bytes(raw))
}
