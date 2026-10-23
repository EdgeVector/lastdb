use super::super::bytes::u32_at;
use super::super::dependency::{Coordinate, MAX_DEPENDENCIES};
use super::super::page::Page;
use super::super::{corrupt, key_layout, Current, DirectoryEntry, Location, Source, TreeKind};
use super::MAX_REFERENCE_CHANGES;
use crate::Result;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct OverflowKey {
    pub(super) kind: u8,
    pub(super) object: Uuid,
    pub(super) first: u64,
    pub(super) suffix_bytes: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Span {
    pub(super) object: Uuid,
    pub(super) first_block: u64,
    pub(super) blocks: u32,
}

/// Parts admit 65536 distinct keys, with a conservative 256-byte entry quote
/// of 16 MiB including B-tree node overhead. Pending references belong to one
/// metadata page and drain before another page enters. Their separate 4096-key
/// ceiling quotes at most 1 MiB. Zero deltas leave all maps.
#[derive(Default)]
pub(in crate::disk_pack) struct Changes {
    pub(super) parts: BTreeMap<Coordinate, i64>,
    pub(super) overflows: BTreeMap<OverflowKey, i64>,
    pub(super) locations: BTreeMap<Span, i64>,
}

impl Changes {
    pub(in crate::disk_pack) fn observe_page(
        &mut self,
        kind: TreeKind,
        object: Uuid,
        number: u64,
        page: &Page,
        current: &Current,
        sign: i64,
    ) -> Result<()> {
        if !matches!(sign, -1 | 1) {
            return Err(corrupt("invalid live-page reference delta"));
        }
        if !self.overflows.is_empty() || !self.locations.is_empty() {
            return Err(corrupt("unresolved prior live-page references"));
        }
        if page.kind() == 4 || (page.level() == 0 && page.count() == 0) {
            return Ok(());
        }
        let dependency_kind = match (kind, page.kind()) {
            (TreeKind::Directory, 1 | 3) => 1,
            (TreeKind::Locator, super::super::packed_page::LEAF)
            | (TreeKind::Locator, super::super::packed_page::INTERNAL) => 2,
            _ => return Err(corrupt("live-page tree kind differs")),
        };
        self.add_part(
            Coordinate {
                kind: dependency_kind,
                object,
                part: u32::try_from(number / super::super::PAGES_PER_PART)
                    .map_err(|_| corrupt("live-page part exceeds format"))?,
            },
            sign,
        )?;
        for at in 0..page.count() {
            let entry = page.entry(at);
            let first = key_layout::overflow(entry);
            if first != 0 {
                let suffix_bytes = (u32_at(entry, 0) as usize)
                    .checked_sub(key_layout::inline(entry).len())
                    .ok_or_else(|| corrupt("live-page key suffix length"))?;
                self.add_overflow(
                    OverflowKey {
                        kind: dependency_kind,
                        object,
                        first,
                        suffix_bytes: suffix_bytes as u64,
                    },
                    sign,
                )?;
            }
            if page.level() == 0 {
                self.observe_value(kind, key_layout::value(entry), current, sign)?;
            }
        }
        Ok(())
    }

    pub(in crate::disk_pack) fn observe_complete_page(
        &mut self,
        kind: TreeKind,
        object: Uuid,
        number: u64,
        page: &Page,
        current: &Current,
        sign: i64,
    ) -> Result<()> {
        self.observe_page(kind, object, number, page, current, sign)?;
        if kind == TreeKind::Locator && page.level() == 0 && page.kind() != 4 {
            for at in 0..page.count() {
                self.observe_location(key_layout::value(page.entry(at)), current, sign)?;
            }
        }
        Ok(())
    }

    fn observe_value(
        &mut self,
        kind: TreeKind,
        value: &[u8],
        current: &Current,
        sign: i64,
    ) -> Result<()> {
        let value = value
            .try_into()
            .map_err(|_| corrupt("live-page value width"))?;
        if kind == TreeKind::Directory {
            let entry = DirectoryEntry::decode(value)?;
            if entry.converted_frontier > current.frontier {
                return Err(corrupt("live-page directory beyond selected frontier"));
            }
            // New groups reuse this object's page-zero allocator template even
            // when every selected nonempty root lives in a later part.
            self.add_part(
                Coordinate {
                    kind: 2,
                    object: entry.locator.object,
                    part: 0,
                },
                sign,
            )?;
        } else {
            checked_location(value, current)?;
        }
        Ok(())
    }

    /// Ordinary edits resolve only replaced or inserted locator values. A copied
    /// unchanged leaf slot therefore requires no payload block-map access.
    pub(in crate::disk_pack) fn observe_location(
        &mut self,
        value: &[u8],
        current: &Current,
        sign: i64,
    ) -> Result<()> {
        if !matches!(sign, -1 | 1) {
            return Err(corrupt("invalid live-location reference delta"));
        }
        let value = value
            .try_into()
            .map_err(|_| corrupt("live-location value width"))?;
        let location = checked_location(value, current)?;
        if location.source == Source::Pack {
            self.add_location(
                Span {
                    object: location.object,
                    first_block: location.first_block,
                    blocks: location.blocks,
                },
                sign,
            )?;
        }
        Ok(())
    }

    pub(in crate::disk_pack) fn merge(&mut self, other: Self) -> Result<()> {
        if !self.overflows.is_empty()
            || !self.locations.is_empty()
            || !other.overflows.is_empty()
            || !other.locations.is_empty()
        {
            return Err(corrupt("unresolved references in live-page merge"));
        }
        for (key, delta) in other.parts {
            self.add_part(key, delta)?;
        }
        Ok(())
    }

    pub(super) fn add_part(&mut self, key: Coordinate, delta: i64) -> Result<()> {
        let entries = self.parts.len();
        adjust(&mut self.parts, key, delta, entries, MAX_DEPENDENCIES)
    }

    fn add_overflow(&mut self, key: OverflowKey, delta: i64) -> Result<()> {
        let entries = self.overflows.len() + self.locations.len();
        adjust(
            &mut self.overflows,
            key,
            delta,
            entries,
            MAX_REFERENCE_CHANGES,
        )
    }

    fn add_location(&mut self, key: Span, delta: i64) -> Result<()> {
        let entries = self.overflows.len() + self.locations.len();
        adjust(
            &mut self.locations,
            key,
            delta,
            entries,
            MAX_REFERENCE_CHANGES,
        )
    }
}

fn checked_location(value: &[u8; 128], current: &Current) -> Result<Location> {
    let location = Location::decode_for_current(value, current)?;
    if location.version.batch > current.frontier {
        return Err(corrupt("live-page locator beyond selected frontier"));
    }
    Ok(location)
}

fn adjust<K: Ord>(
    map: &mut BTreeMap<K, i64>,
    key: K,
    delta: i64,
    entries: usize,
    limit: usize,
) -> Result<()> {
    if delta == 0 {
        return Ok(());
    }
    let previous = map.get(&key).copied().unwrap_or(0);
    let next = previous
        .checked_add(delta)
        .ok_or_else(|| corrupt("live-part reference delta overflow"))?;
    if next == 0 {
        map.remove(&key);
    } else {
        if previous == 0 && entries >= limit {
            return Err(super::super::resource());
        }
        map.insert(key, next);
    }
    Ok(())
}
