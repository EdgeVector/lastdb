use super::changes::Span;
use super::{add_delta, Resolver};
use crate::disk_pack::bytes::u32_at;
use crate::disk_pack::dependency::Coordinate;
use crate::disk_pack::{corrupt, Location, MutationVersion, Source, MAP_ENTRIES};
use crate::Result;
use std::collections::BTreeMap;

pub(super) fn resolve(
    resolver: &mut Resolver<'_>,
    span: Span,
    delta: i64,
    parts: &mut BTreeMap<Coordinate, i64>,
) -> Result<()> {
    if delta == 0 {
        return Ok(());
    }
    let location = Location {
        source: Source::Pack,
        version: MutationVersion::default(),
        record_bytes: 96,
        object: span.object,
        first_block: span.first_block,
        first_position: 0,
        blocks: span.blocks,
        digest: [0; 32],
    };
    let end = span
        .first_block
        .checked_add(u64::from(span.blocks))
        .filter(|_| span.blocks > 0 && !span.object.is_nil())
        .ok_or_else(|| corrupt("live payload span bounds"))?;
    for ordinal in span.first_block..end {
        resolver.step()?;
        let first_map = map_coordinate(span.object, span.first_block / MAP_ENTRIES)?;
        resolver.allow(first_map)?;
        let object = resolver
            .call
            .object_metadata(&location)?
            .ok_or_else(|| corrupt("live payload requires complete object bounds"))?;
        if ordinal >= object.blocks {
            return Err(corrupt("live payload block outside object"));
        }
        let map_number = ordinal / MAP_ENTRIES;
        let map_coordinate = map_coordinate(span.object, map_number)?;
        resolver.allow(map_coordinate)?;
        let base = resolver.call.data_path(&location);
        let map = resolver.call.map_page(&base, span.object, map_number)?;
        let index = (ordinal % MAP_ENTRIES) as usize;
        if map.object_bounds() != Some((object.logical_bytes, object.data_parts))
            || map.generation() != object.generation
            || index >= map.count()
        {
            return Err(corrupt("live payload map identity or bounds"));
        }
        let entry = &map.payload()[index * 24..index * 24 + 24];
        let part = u32_at(entry, 0);
        object.check_block(ordinal, part, u32_at(entry, 12) as usize, map.count())?;
        let data_coordinate = Coordinate {
            kind: 5,
            object: span.object,
            part,
        };
        resolver.allow(data_coordinate)?;
        resolver.call.verify_part(
            &base.join(format!("part-{part:08x}")),
            &location,
            part,
            ordinal,
        )?;
        add_delta(parts, map_coordinate, delta)?;
        add_delta(parts, data_coordinate, delta)?;
    }
    Ok(())
}

fn map_coordinate(object: uuid::Uuid, page: u64) -> Result<Coordinate> {
    Ok(Coordinate {
        kind: 3,
        object,
        part: u32::try_from(page / 512)
            .map_err(|_| corrupt("live payload map part exceeds format"))?,
    })
}
