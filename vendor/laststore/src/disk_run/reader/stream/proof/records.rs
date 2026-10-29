//! One validation pass and one forward pass over an authenticated block.
use super::corrupt;
use crate::disk_run::bloom::{key_hash, Filter};
use crate::disk_run::{MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::{Error, Result};
use std::ops::Range;

pub(super) fn next(
    payload: &[u8],
    at: &mut usize,
    key: &mut Vec<u8>,
) -> Result<Option<Range<usize>>> {
    let shared = length(payload, at, key.len())?;
    let suffix = length(payload, at, MAX_RUN_KEY_BYTES - shared)?;
    let end = at
        .checked_add(suffix)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| corrupt("proof key exceeds block"))?;
    let tail = &payload[*at..end];
    if shared + suffix == 0 || !key.is_empty() && key[shared..].cmp(tail).is_ge() {
        return Err(corrupt("proof block key order"));
    }
    key.truncate(shared);
    key.try_reserve_exact(suffix)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    key.extend_from_slice(tail);
    if key.capacity() > MAX_RUN_KEY_BYTES {
        return Err(Error::ReadBudgetExceeded);
    }
    *at = end;
    let tag = length(payload, at, MAX_RUN_VALUE_BYTES + 1)?;
    if tag == 0 {
        return Ok(None);
    }
    let start = *at;
    *at = at
        .checked_add(tag - 1)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| corrupt("proof body exceeds block"))?;
    Ok(Some(start..*at))
}

pub(super) fn validate(
    payload: &[u8],
    first: &[u8],
    following: Option<&[u8]>,
    last: Option<&[u8]>,
    filter: &Filter<'_>,
) -> Result<Vec<u8>> {
    let mut at = 0;
    let mut key = Vec::new();
    let maximum = payload.len().min(MAX_RUN_KEY_BYTES);
    key.try_reserve_exact(maximum)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    if key.capacity() > MAX_RUN_KEY_BYTES {
        return Err(Error::ReadBudgetExceeded);
    }
    if payload.is_empty() {
        return Err(corrupt("proof data block is empty"));
    }
    while at < payload.len() {
        let initial = at == 0;
        next(payload, &mut at, &mut key)?;
        if initial && key != first {
            return Err(corrupt("proof block first key differs"));
        }
        if !filter.may_hold(key_hash(&key)) {
            return Err(corrupt("proof block key excluded by its index filter"));
        }
    }
    if following.is_some_and(|upper| key.as_slice() >= upper)
        || last.is_some_and(|last| key.as_slice() != last)
    {
        return Err(corrupt("proof block final key differs from its range"));
    }
    key.clear();
    Ok(key)
}

fn length(payload: &[u8], at: &mut usize, maximum: usize) -> Result<usize> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *payload
            .get(*at)
            .ok_or_else(|| corrupt("proof varint ends early"))?;
        *at += 1;
        if shift == 63 && byte > 1 {
            return Err(corrupt("proof varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return usize::try_from(value)
                .ok()
                .filter(|n| *n <= maximum)
                .ok_or_else(|| corrupt("proof record length"));
        }
    }
    Err(corrupt("proof varint overflow"))
}
