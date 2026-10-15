use super::bytes::*;
use super::dependency::Coordinate;
use super::page::Page;
use super::page_write::{directory, PageFields, PageWriter};
use super::{artifact, corrupt, Current, PAGE_BYTES};
use crate::read_receipt_io::ReadClass;
use crate::Result;
use std::path::Path;
use uuid::Uuid;

/// Complete generation identity; ordinary reads do not enumerate this receipt.
pub(super) struct Generation {
    pub(super) object: Uuid,
    pub(super) dependencies: u64,
    pub(super) coordinates: Vec<Coordinate>,
}

pub(super) fn write(
    root: &Path,
    current: &Current,
    dependencies: &[Coordinate],
) -> Result<Generation> {
    if dependencies.len() > super::dependency::MAX_DEPENDENCIES {
        return Err(super::resource());
    }
    let parent = root.join("generations");
    if !parent.try_exists()? {
        directory(&parent)?;
    }
    let path = parent.join(current.generation.to_string());
    if path.try_exists()? {
        return verify(root, current);
    }
    let object = Uuid::new_v4();
    let mut pages = PageWriter::new(path, object, current.generation)?;
    let current_bytes = current.encode()?;
    let count = dependencies.len();
    let page_count = 1 + count.saturating_sub(150).div_ceil(201);
    let mut ordinal = 0;
    for number in 0..page_count {
        let entries = (count - ordinal).min(if number == 0 { 150 } else { 201 });
        let mut payload = prefix(current, count as u64, page_count as u64, &current_bytes);
        put64(&mut payload, 24, ordinal as u64);
        put32(&mut payload, 32, entries as u32);
        if number == 0 {
            put32(&mut payload, 36, 4096);
            payload.extend_from_slice(&current_bytes);
        }
        for dependency in &dependencies[ordinal..ordinal + entries] {
            payload.extend_from_slice(&dependency.capture(root, current)?);
        }
        pages.append(
            PageFields {
                kind: 11,
                level: 0,
                count: entries as u16,
                width: 80,
                left: 0,
                next: if number + 1 == page_count {
                    0
                } else {
                    number as u64 + 1
                },
                auxiliary: page_count as u64,
            },
            &payload,
        )?;
        ordinal += entries;
    }
    pages.finish()?;
    Ok(Generation {
        object,
        dependencies: count as u64,
        coordinates: dependencies.to_vec(),
    })
}

fn prefix(current: &Current, count: u64, pages: u64, bytes: &[u8; 4096]) -> Vec<u8> {
    let mut prefix = vec![0; 128];
    put64(&mut prefix, 0, current.generation);
    put64(&mut prefix, 8, current.frontier);
    put64(&mut prefix, 16, count);
    prefix[40..72].copy_from_slice(&digest(bytes));
    put64(&mut prefix, 72, pages);
    put64(&mut prefix, 80, current.published_ms);
    put64(&mut prefix, 88, current.required_features);
    prefix
}

pub(super) fn verify(root: &Path, current: &Current) -> Result<Generation> {
    let path = root
        .join("generations")
        .join(current.generation.to_string());
    let mut call = artifact::metadata_call(root, current.clone());
    let buffer = call.read_extent(
        &path.join("part-00000000"),
        0,
        PAGE_BYTES,
        true,
        ReadClass::FormatProbe,
    )?;
    let object = nonnil(uuid_at(&buffer.bytes, 24))?;
    let first = Page::decode(
        buffer,
        object,
        0,
        current.generation,
        current.required_features,
    )?;
    if first.payload().len() < 128 {
        return Err(corrupt("generation prefix length"));
    }
    let count = u64_at(first.payload(), 16);
    if count > super::dependency::MAX_DEPENDENCIES as u64 {
        return Err(corrupt(
            "generation dependency count exceeds artifact limit",
        ));
    }
    let pages = 1 + count.saturating_sub(150).div_ceil(201);
    let current_bytes = current.encode()?;
    let expected_prefix = prefix(current, count, pages, &current_bytes);
    let (mut number, mut ordinal) = (0, 0);
    let mut page = first;
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(count as usize)
        .map_err(|_| super::resource())?;
    loop {
        let first_bytes = if number == 0 { 4096 } else { 0 };
        let entries = (count - ordinal).min(if number == 0 { 150 } else { 201 });
        let mut expected = expected_prefix.clone();
        put64(&mut expected, 24, ordinal);
        put32(&mut expected, 32, entries as u32);
        put32(&mut expected, 36, first_bytes);
        let next = if number + 1 == pages { 0 } else { number + 1 };
        if page.kind() != 11
            || page.generation() != current.generation
            || page.count() as u64 != entries
            || page.next() != next
            || page.auxiliary() != pages
            || page.payload().len() != 128 + first_bytes as usize + entries as usize * 80
            || page.payload()[..128] != expected
            || (number == 0 && page.payload()[128..4224] != current_bytes)
        {
            return Err(corrupt("generation receipt boundary or CURRENT mismatch"));
        }
        for entry in page.payload()[128 + first_bytes as usize..].chunks_exact(80) {
            coordinates.push(verify_dependency(root, current, entry)?);
        }
        ordinal += entries;
        if next == 0 {
            break;
        }
        number = next;
        let part = number / 512;
        let bytes = call.read_extent(
            &path.join(format!("part-{part:08x}")),
            number % 512 * PAGE_BYTES as u64,
            PAGE_BYTES,
            true,
            ReadClass::FormatProbe,
        )?;
        page = Page::decode(
            bytes,
            object,
            number,
            current.generation,
            current.required_features,
        )?;
    }
    Ok(Generation {
        object,
        dependencies: count,
        coordinates,
    })
}

fn verify_dependency(root: &Path, current: &Current, entry: &[u8]) -> Result<Coordinate> {
    // This first publisher emits no whole-part hashes; unknown flags never pass.
    zero(&entry[1..4])?;
    zero(&entry[40..72])?;
    let coordinate = Coordinate {
        kind: entry[0],
        object: nonnil(uuid_at(entry, 8))?,
        batch: u64_at(entry, 24),
        part: u32_at(entry, 4),
    };
    if coordinate.capture(root, current)? != entry {
        return Err(corrupt("generation dependency differs from immutable part"));
    }
    Ok(coordinate)
}
