//! Binary segment records (compact on disk, fast to parse).
//! put: `0x01 | u16le id_len | id | u32le body_len | body`
//! del: `0x02 | u16le id_len | id`

use crate::{Error, Result};

pub const OP_PUT: u8 = 0x01;
pub const OP_DEL: u8 = 0x02;

/// Largest record id a segment can hold — the `u16le id_len` prefix.
///
/// Callers that build ids out of caller data (LastDB's `mk:` molecule keys
/// embed a hash and a raw range segment) must check against this **before**
/// taking any destructive step, because a rewrite that trips it fails at the
/// very last moment, after the old records are already gone.
pub const MAX_ID_LEN: usize = u16::MAX as usize;

/// Largest record body a segment can hold — the `u32le body_len` prefix.
pub const MAX_BODY_LEN: usize = u32::MAX as usize;

#[derive(Debug, Clone)]
pub struct Rec {
    pub is_put: bool,
    pub id: String,
    pub body: Option<Vec<u8>>,
    pub raw_len: usize,
}

pub fn encode_put(id: &str, body: &[u8]) -> Result<Vec<u8>> {
    // Name which limit was hit and by how much. The old message ("id/body too
    // large") could not distinguish a 65 KiB key from a 4 GiB body, so a
    // caller that hit it had no way to know what to measure. Ids are caller
    // data — a storage key can carry record content in its segments — so the
    // message reports lengths only and never the id itself.
    if id.len() > MAX_ID_LEN {
        return Err(Error::Corrupt(format!(
            "id too large: {} bytes, {} over the {MAX_ID_LEN}-byte segment id limit",
            id.len(),
            id.len() - MAX_ID_LEN
        )));
    }
    if body.len() > MAX_BODY_LEN {
        return Err(Error::Corrupt(format!(
            "body too large: {} bytes, {} over the {MAX_BODY_LEN}-byte segment body limit",
            body.len(),
            body.len() - MAX_BODY_LEN
        )));
    }
    let mut out = Vec::with_capacity(1 + 2 + id.len() + 4 + body.len());
    out.push(OP_PUT);
    out.extend_from_slice(&(id.len() as u16).to_le_bytes());
    out.extend_from_slice(id.as_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

pub fn encode_del(id: &str) -> Result<Vec<u8>> {
    if id.len() > MAX_ID_LEN {
        return Err(Error::Corrupt(format!(
            "id too large: {} bytes, {} over the {MAX_ID_LEN}-byte segment id limit",
            id.len(),
            id.len() - MAX_ID_LEN
        )));
    }
    let mut out = Vec::with_capacity(1 + 2 + id.len());
    out.push(OP_DEL);
    out.extend_from_slice(&(id.len() as u16).to_le_bytes());
    out.extend_from_slice(id.as_bytes());
    Ok(out)
}

pub fn parse_at(data: &[u8], offset: usize) -> Result<Option<Rec>> {
    if offset >= data.len() {
        return Ok(None);
    }
    if offset + 3 > data.len() {
        return Ok(None);
    }
    let op = data[offset];
    let id_len = u16::from_le_bytes([data[offset + 1], data[offset + 2]]) as usize;
    let id0 = offset + 3;
    let id1 = id0 + id_len;
    if id1 > data.len() {
        return Ok(None);
    }
    let id = std::str::from_utf8(&data[id0..id1])
        .map_err(|e| Error::Corrupt(format!("id utf8: {e}")))?
        .to_string();
    match op {
        OP_PUT => {
            if id1 + 4 > data.len() {
                return Ok(None);
            }
            let blen = u32::from_le_bytes(data[id1..id1 + 4].try_into().unwrap()) as usize;
            let b0 = id1 + 4;
            let b1 = b0 + blen;
            if b1 > data.len() {
                return Ok(None);
            }
            Ok(Some(Rec {
                is_put: true,
                id,
                body: Some(data[b0..b1].to_vec()),
                raw_len: b1 - offset,
            }))
        }
        OP_DEL => Ok(Some(Rec {
            is_put: false,
            id,
            body: None,
            raw_len: id1 - offset,
        })),
        other => Err(Error::Corrupt(format!("bad op {other:#x}"))),
    }
}
