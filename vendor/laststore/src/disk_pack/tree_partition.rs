//! Byte-bounded locator splits, retaining seven separators in non-root parents.

use super::tree_output::{Child, LeafEntry};
use super::{corrupt, TreeKind, PAGE_PAYLOAD};
use crate::Result;
use std::ops::Range;

pub(super) fn leaves(
    kind: TreeKind,
    entries: &[LeafEntry],
    middle_insert: bool,
) -> Result<Vec<Range<usize>>> {
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
    if middle_insert && ranges.len() == 2 {
        balance_small_tail(entries, &mut ranges);
    }
    Ok(ranges)
}

/// Random inserts into a full leaf must not repeatedly produce tiny neighbors.
/// Pure tail growth retains the full-left append layout. Reuse the same entries
/// and two output pages; only the separator position changes.
fn balance_small_tail(entries: &[LeafEntry], ranges: &mut [Range<usize>]) {
    let width = |entry: &LeafEntry| entry.0.len() + 132;
    let tail: usize = entries[ranges[1].clone()].iter().map(width).sum();
    if tail * 4 >= PAGE_PAYLOAD {
        return;
    }
    let total: usize = entries.iter().map(width).sum();
    let mut left = 0;
    let mut boundary = ranges[0].end;
    let mut distance = usize::MAX;
    for (index, entry) in entries.iter().take(entries.len() - 1).enumerate() {
        left += width(entry);
        let right = total - left;
        if left <= PAGE_PAYLOAD && right <= PAGE_PAYLOAD && left.abs_diff(right) < distance {
            boundary = index + 1;
            distance = left.abs_diff(right);
        }
    }
    ranges[0].end = boundary;
    ranges[1].start = boundary;
    crate::commit_timing::count("locator_leaf_balanced_splits", 1);
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
