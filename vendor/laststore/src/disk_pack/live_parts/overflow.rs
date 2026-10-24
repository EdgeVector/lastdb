use super::changes::OverflowKey;
use super::{add_delta, Resolver};
use crate::disk_pack::corrupt;
use crate::disk_pack::dependency::Coordinate;
use crate::Result;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn resolve(
    resolver: &mut Resolver<'_>,
    key: OverflowKey,
    delta: i64,
    parts: &mut BTreeMap<Coordinate, i64>,
) -> Result<()> {
    if delta == 0 {
        return Ok(());
    }
    let maximum = resolver
        .call
        .current
        .storage_key_byte_limit()
        .saturating_sub(128) as u64;
    if !matches!(key.kind, 1 | 2) || key.first == 0 || !(1..=maximum).contains(&key.suffix_bytes) {
        return Err(corrupt("live overflow reference bounds"));
    }
    let namespace = if key.kind == 1 {
        "directory"
    } else {
        "locator"
    };
    let directory = resolver
        .call
        .root
        .join(namespace)
        .join(key.object.to_string());
    let mut seen = BTreeSet::new();
    let mut number = key.first;
    let mut bytes = 0u64;
    while number != 0 {
        resolver.step()?;
        if seen.len() as u64
            >= key
                .suffix_bytes
                .div_ceil(crate::disk_pack::PAGE_PAYLOAD as u64)
            || !seen.insert(number)
        {
            return Err(corrupt("live overflow chain cycle or limit"));
        }
        let coordinate = Coordinate {
            kind: key.kind,
            object: key.object,
            part: u32::try_from(number / crate::disk_pack::PAGES_PER_PART)
                .map_err(|_| corrupt("live overflow part exceeds format"))?,
        };
        resolver.allow(coordinate)?;
        let page = resolver.call.page(&directory, key.object, number)?;
        if page.kind() != 4 || page.auxiliary() != key.suffix_bytes {
            return Err(corrupt("live overflow page identity"));
        }
        bytes = bytes
            .checked_add(page.payload().len() as u64)
            .filter(|bytes| *bytes <= key.suffix_bytes)
            .ok_or_else(|| corrupt("live overflow chain length"))?;
        add_delta(parts, coordinate, delta)?;
        number = page.next();
    }
    if bytes != key.suffix_bytes {
        return Err(corrupt("live overflow chain is incomplete"));
    }
    Ok(())
}
