//! Byte-bounded locator splits, retaining seven separators in non-root parents.

use super::tree_output::{Child, LeafEntry};
use super::{corrupt, TreeKind, PAGE_PAYLOAD};
use crate::Result;
use std::ops::Range;

pub(super) fn leaves(kind: TreeKind, entries: &[LeafEntry]) -> Result<Vec<Range<usize>>> {
    if kind == TreeKind::Directory {
        return Ok(fixed(entries.len(), super::LEAF_ENTRIES));
    }
    let mut ranges = Vec::new();
    let mut first = 0;
    let mut bytes = 0;
    for (index, (key, _)) in entries.iter().enumerate() {
        let width = key.len() + 128 + 4;
        if width > PAGE_PAYLOAD {
            return Err(corrupt("locator key cannot fit a leaf"));
        }
        if bytes + width > PAGE_PAYLOAD {
            ranges.push(first..index);
            first = index;
            bytes = 0;
        }
        bytes += width;
    }
    if first < entries.len() {
        ranges.push(first..entries.len());
    }
    Ok(ranges)
}

pub(super) fn parents(kind: TreeKind, children: &[Child]) -> Result<Vec<Range<usize>>> {
    if children.len() < 2 {
        return Err(corrupt("parent requires two children"));
    }
    if kind == TreeKind::Directory {
        return Ok(fixed(children.len(), 89));
    }
    let mut ranges = Vec::new();
    let mut first = 0;
    let mut bytes = 0;
    for (index, child) in children.iter().enumerate().skip(1) {
        let width = child
            .minimum
            .as_ref()
            .ok_or_else(|| corrupt("missing separator"))?
            .len()
            + 12;
        // Seven maximum-width separators fit, so a split can retain eight children.
        if width > 1076 {
            return Err(corrupt("separator exceeds packed height admission"));
        }
        if bytes + width > PAGE_PAYLOAD {
            ranges.push(first..index);
            first = index;
            bytes = 0;
        } else {
            bytes += width;
        }
    }
    ranges.push(first..children.len());
    if ranges.len() > 1 && ranges.last().expect("nonempty ranges").len() < 8 {
        let boundary = children.len() - 8;
        let last = ranges.len() - 1;
        ranges[last - 1].end = boundary;
        ranges[last].start = boundary;
    }
    if ranges.len() > 1 && ranges.iter().any(|range| range.len() < 8) {
        return Err(corrupt("packed parent split violates minimum occupancy"));
    }
    Ok(ranges)
}

fn fixed(count: usize, maximum: usize) -> Vec<Range<usize>> {
    if count == 0 {
        return Vec::new();
    }
    let width = count.div_ceil(count.div_ceil(maximum));
    (0..count)
        .step_by(width)
        .map(|first| first..(first + width).min(count))
        .collect()
}
