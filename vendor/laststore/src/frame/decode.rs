//! A run reader shares its key schedule; every frame still authenticates itself.

use super::*;
use crate::read_receipt::Timer;

/// One lookup owns this workspace; no decoder or decoded block survives it.
#[derive(Default)]
pub(crate) struct DecodeScratch {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl DecodeScratch {
    fn decompress(&mut self, stored: &[u8], maximum: usize) -> std::io::Result<Vec<u8>> {
        if self.decompressor.is_none() {
            crate::read_receipt::update(|receipt| {
                receipt.frame_decompressor_setups =
                    receipt.frame_decompressor_setups.saturating_add(1);
            });
            self.decompressor = Some(zstd::bulk::Decompressor::new()?);
        }
        self.decompressor
            .as_mut()
            .expect("decoder initialized")
            .decompress(stored, maximum)
    }
}

impl Drop for DecodeScratch {
    fn drop(&mut self) {
        if self.decompressor.is_some() {
            let timer = Timer::start();
            drop(self.decompressor.take());
            // The old one-shot decoder included destruction in these timers.
            timer.finish(|receipt, nanos| {
                receipt.frame_decompress_nanos =
                    receipt.frame_decompress_nanos.saturating_add(nanos);
                receipt.decode_nanos = receipt.decode_nanos.saturating_add(nanos);
            });
        }
    }
}

pub(crate) struct PreparedFrameKey {
    chunk: Uuid,
    cipher: Aes256Gcm,
}

impl PreparedFrameKey {
    pub(crate) fn new(data_key: &[u8; 32], chunk: Uuid) -> Result<Self> {
        // Keep one-time key setup inside the aggregate decode metric too.
        crate::read_receipt_io::decode(
            || {
                Ok(Self {
                    chunk,
                    cipher: make_cipher(data_key, chunk)?,
                })
            },
            |_| (0, 0),
        )
    }

    pub(crate) fn decode(
        &self,
        data_key: &[u8; 32],
        frame: &[u8],
        maximum: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<DecodedFrame> {
        observed_decode(data_key, frame, Some(maximum), Some(self), scratch)
    }
}

/// Decode and authenticate one AES-256-GCM frame.
/// Authentication failure returns AeadAuthFail before plaintext leaves the call.
pub fn decode_frame(data_key: &[u8; 32], frame: &[u8]) -> Result<DecodedFrame> {
    observed_decode(data_key, frame, None, None, None)
}

pub(crate) fn decode_frame_bounded(
    data_key: &[u8; 32],
    frame: &[u8],
    maximum: usize,
) -> Result<DecodedFrame> {
    observed_decode(data_key, frame, Some(maximum), None, None)
}

fn observed_decode(
    data_key: &[u8; 32],
    frame: &[u8],
    maximum: Option<usize>,
    prepared: Option<&PreparedFrameKey>,
    scratch: Option<&mut DecodeScratch>,
) -> Result<DecodedFrame> {
    crate::read_receipt_io::decode(
        || decode_inner(data_key, frame, maximum, prepared, scratch),
        |decoded| (decoded.payload.len(), decoded.payload.capacity()),
    )
}

fn make_cipher(data_key: &[u8; 32], chunk: Uuid) -> Result<Aes256Gcm> {
    let timer = Timer::start();
    let result = chunk_key(data_key, chunk).and_then(|key| {
        Aes256Gcm::new_from_slice(&key).map_err(|_| Error::Config("invalid aead key length".into()))
    });
    timer.finish(|receipt, nanos| {
        receipt.frame_key_setups = receipt.frame_key_setups.saturating_add(1);
        receipt.frame_key_setup_nanos = receipt.frame_key_setup_nanos.saturating_add(nanos);
    });
    result
}

fn decode_inner(
    data_key: &[u8; 32],
    frame: &[u8],
    max_payload: Option<usize>,
    prepared: Option<&PreparedFrameKey>,
    scratch: Option<&mut DecodeScratch>,
) -> Result<DecodedFrame> {
    if frame.len() < HEADER_SIZE + TAG_SIZE {
        return Err(Error::Corrupt("frame too short".into()));
    }
    let header_bytes = &frame[..HEADER_SIZE];
    let (header, payload_len, flags) = decode_header(header_bytes)?;
    let expected_len = HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|n| n.checked_add(TAG_SIZE))
        .ok_or_else(|| Error::Corrupt("frame length overflow".into()))?;
    if frame.len() != expected_len {
        return Err(Error::Corrupt("frame length mismatch".into()));
    }
    if max_payload.is_some_and(|limit| payload_len > limit.saturating_add(UNCOMPRESSED_LEN_SIZE)) {
        return Err(Error::Corrupt("stored frame exceeds block limit".into()));
    }
    let tag_offset = HEADER_SIZE + payload_len;
    let mut payload = frame[HEADER_SIZE..tag_offset].to_vec();
    let mut payload_charge = crate::read_receipt::TemporaryBytes::new(payload.capacity());
    let tag = Tag::from_slice(&frame[tag_offset..]);
    let generated;
    let cipher = if let Some(key) = prepared.filter(|key| key.chunk == header.chunk_uuid) {
        &key.cipher
    } else {
        // Preserve authentication and the original error for a foreign frame.
        generated = make_cipher(data_key, header.chunk_uuid)?;
        &generated
    };
    let nonce_bytes = counter_nonce(header.counter);
    let timer = Timer::start();
    let opened = cipher.decrypt_in_place_detached(
        Nonce::from_slice(&nonce_bytes),
        header_bytes,
        &mut payload,
        tag,
    );
    timer.finish(|receipt, nanos| {
        receipt.frame_auth_nanos = receipt.frame_auth_nanos.saturating_add(nanos);
    });
    opened.map_err(|_| Error::AeadAuthFail)?;
    if flags & FLAG_ZSTD != 0 {
        payload = decompress_bounded(
            &payload,
            max_payload.unwrap_or(MAX_DECOMPRESSED_FRAME_BYTES),
            scratch,
        )?;
        payload_charge.resize(payload.capacity());
    } else if max_payload.is_some_and(|limit| payload.len() > limit) {
        return Err(Error::Corrupt("decoded frame exceeds block limit".into()));
    }
    Ok(DecodedFrame { header, payload })
}

fn decompress_bounded(
    stored: &[u8],
    max_payload: usize,
    scratch: Option<&mut DecodeScratch>,
) -> Result<Vec<u8>> {
    if stored.len() < UNCOMPRESSED_LEN_SIZE {
        return Err(Error::Corrupt("compressed frame missing length".into()));
    }
    let expected_u64 = u64::from_le_bytes(stored[..8].try_into().unwrap());
    let expected = usize::try_from(expected_u64)
        .map_err(|_| Error::Corrupt("decompressed frame length too large".into()))?;
    let limit = max_payload.min(MAX_DECOMPRESSED_FRAME_BYTES);
    if expected > limit {
        return Err(Error::Corrupt(format!(
            "decompressed frame exceeds {limit} bytes"
        )));
    }
    let timer = Timer::start();
    let decoded = match scratch {
        Some(scratch) => scratch.decompress(&stored[8..], expected),
        None => {
            crate::read_receipt::update(|receipt| {
                receipt.frame_decompressor_setups =
                    receipt.frame_decompressor_setups.saturating_add(1);
            });
            zstd::bulk::decompress(&stored[8..], expected)
        }
    };
    timer.finish(|receipt, nanos| {
        receipt.frame_decompress_nanos = receipt.frame_decompress_nanos.saturating_add(nanos);
    });
    let decoded =
        decoded.map_err(|e| Error::Corrupt(format!("frame zstd decompression failed: {e}")))?;
    let _decoded_charge = crate::read_receipt::TemporaryBytes::new(decoded.capacity());
    if decoded.len() != expected {
        return Err(Error::Corrupt("decompressed frame length mismatch".into()));
    }
    Ok(decoded)
}
