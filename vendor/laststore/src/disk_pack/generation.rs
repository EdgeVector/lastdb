use super::bytes::*;
use super::dependency::Coordinate;
use super::page::Page;
use super::page_write::{directory, PageFields, PageWriter};
use super::{artifact, corrupt, Current, PAGE_BYTES};
use crate::read_receipt_io::ReadClass;
use crate::Result;
use std::path::Path;
use uuid::Uuid;

mod carried;
mod dependency_record;

/// Complete generation identity; ordinary reads do not enumerate this receipt.
pub(super) struct Generation {
    pub(super) object: Uuid,
    pub(super) dependencies: u64,
    pub(super) coordinates: Vec<Coordinate>,
    pub(super) records: Vec<[u8; 80]>,
}

pub(super) fn write(
    root: &Path,
    current: &Current,
    dependencies: &[Coordinate],
) -> Result<Generation> {
    write_with_prior(root, current, dependencies, None)
}

pub(super) fn write_carried(
    root: &Path,
    current: &Current,
    dependencies: &[Coordinate],
    prior: &Generation,
) -> Result<Generation> {
    write_with_prior(root, current, dependencies, Some(prior))
}

fn write_with_prior(
    root: &Path,
    current: &Current,
    dependencies: &[Coordinate],
    prior: Option<&Generation>,
) -> Result<Generation> {
    if dependencies.len() > super::dependency::MAX_DEPENDENCIES {
        return Err(super::resource());
    }
    if dependencies.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(corrupt("generation coordinates are not strictly ordered"));
    }
    let parent = root.join("generations");
    if !parent.try_exists()? {
        directory(&parent)?;
    }
    let path = parent.join(current.generation.to_string());
    if path.try_exists()? {
        return verify(root, current);
    }
    write_at(root, current, dependencies, prior, path)
}

pub(in crate::disk_pack) fn write_at(
    root: &Path,
    current: &Current,
    dependencies: &[Coordinate],
    prior: Option<&Generation>,
    path: std::path::PathBuf,
) -> Result<Generation> {
    if dependencies.len() > super::dependency::MAX_DEPENDENCIES
        || dependencies.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(corrupt("invalid prepared generation coordinates"));
    }
    let object = Uuid::new_v4();
    let mut pages = PageWriter::new(path, object, current.generation)?;
    let current_bytes = current.encode()?;
    let count = dependencies.len();
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|_| super::resource())?;
    let page_count = 1 + count.saturating_sub(150).div_ceil(201);
    let base_prefix = prefix(current, count as u64, page_count as u64, &current_bytes);
    let mut carried = carried::Carried::new(prior);
    let mut ordinal = 0;
    for number in 0..page_count {
        let entries = (count - ordinal).min(if number == 0 { 150 } else { 201 });
        let mut payload = base_prefix.clone();
        put64(&mut payload, 24, ordinal as u64);
        put32(&mut payload, 32, entries as u32);
        if number == 0 {
            put32(&mut payload, 36, 4096);
            payload.extend_from_slice(&current_bytes);
        }
        for dependency in &dependencies[ordinal..ordinal + entries] {
            let entry = match carried.get(dependency) {
                Some(entry) => entry,
                None => dependency.capture(root, current)?,
            };
            dependency_record::decode(current, &entry)?;
            payload.extend_from_slice(&entry);
            records.push(entry);
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
        records,
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
    read(root, current, true)
}

/// Read only the root identity first; callers validate its receipt or hold it.
pub(super) fn load_current(root: &Path, number: u64) -> Result<Current> {
    use std::io::Read;
    let path = root
        .join("generations")
        .join(number.to_string())
        .join("part-00000000");
    let _class = ReadClass::FormatProbe.enter();
    let mut file = crate::read_receipt_io::open(path)?;
    let mut prefix = [0; 4352];
    file.read_exact(&mut prefix)?;
    let current = Current::decode(prefix[256..4352].try_into().expect("embedded CURRENT"))?;
    if current.generation != number {
        return Err(corrupt("generation number differs from root"));
    }
    Ok(current)
}

/// Read checksummed receipt bytes for an already verified generation under the
/// same exclusive lock. The caller must prove the complete CURRENT identity.
pub(super) fn read_receipt(root: &Path, current: &Current) -> Result<Generation> {
    read(root, current, false)
}

fn read(root: &Path, current: &Current, verify_parts: bool) -> Result<Generation> {
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
    let mut records = Vec::new();
    records
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
            let coordinate = dependency_record::decode(current, entry)?;
            if coordinates.last().is_some_and(|prior| *prior >= coordinate) {
                return Err(corrupt("generation coordinates are not strictly ordered"));
            }
            if verify_parts && coordinate.capture(root, current)? != entry {
                return Err(corrupt("generation dependency differs from immutable part"));
            }
            coordinates.push(coordinate);
            records.push(entry.try_into().expect("fixed dependency record"));
        }
        ordinal += entries;
        if next == 0 {
            break;
        }
        number = next;
        let part = number / crate::disk_pack::PAGES_PER_PART;
        let bytes = call.read_extent(
            &path.join(format!("part-{part:08x}")),
            number % crate::disk_pack::PAGES_PER_PART * PAGE_BYTES as u64,
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
        records,
    })
}
