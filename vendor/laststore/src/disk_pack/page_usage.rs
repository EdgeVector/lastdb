//! Numeric fullness of publication output and tracked edit inputs, never a live-home census.

use crate::commit_timing as timing;
use std::collections::BTreeMap;

macro_rules! names {
    ($prefix:literal) => {
        [
            concat!($prefix, "_pages"),
            concat!($prefix, "_payload_bytes"),
            concat!($prefix, "_entries"),
            concat!($prefix, "_empty_pages"),
            concat!($prefix, "_fill_le25"),
            concat!($prefix, "_fill_le50"),
            concat!($prefix, "_fill_le75"),
            concat!($prefix, "_fill_gt75"),
        ]
    };
}

const WRITTEN: [[&str; 8]; 6] = [
    names!("locator_leaf_output"),
    names!("locator_parent_output"),
    names!("directory_leaf_output"),
    names!("directory_parent_output"),
    names!("key_overflow_output"),
    names!("block_map_output"),
];
const INPUT: [[&str; 8]; 6] = [
    names!("locator_root_leaf_input"),
    names!("locator_nonroot_leaf_input"),
    names!("directory_root_leaf_input"),
    names!("directory_nonroot_leaf_input"),
    names!("locator_parent_input"),
    names!("directory_parent_input"),
];
const MAP_REWRITE: [&str; 8] = names!("block_map_header_rewrite");
const SPLITS: [[&str; 3]; 2] = [
    [
        "locator_leaf_splits",
        "locator_leaf_split_pages",
        "locator_leaf_splits_with_middle_insert",
    ],
    [
        "directory_leaf_splits",
        "directory_leaf_split_pages",
        "directory_leaf_splits_with_middle_insert",
    ],
];

/// Explicit zeros prevent conditional-report means from hiding absent page classes.
pub(crate) fn initial_counts() -> BTreeMap<&'static str, u64> {
    let mut counts: BTreeMap<_, _> = WRITTEN
        .iter()
        .chain(INPUT.iter())
        .chain(std::iter::once(&MAP_REWRITE))
        .flatten()
        .map(|&name| (name, 0))
        .collect();
    counts.insert("snapshot_page_usage_version", 1);
    counts.insert("snapshot_page_bytes", crate::disk_pack::PAGE_BYTES as u64);
    counts.insert(
        "snapshot_page_payload_bytes",
        crate::disk_pack::PAGE_PAYLOAD as u64,
    );
    counts.insert("snapshot_private_input_pages", 0);
    counts.insert("snapshot_invalid_usage_pages", 0);
    counts.extend(SPLITS.iter().flatten().map(|&name| (name, 0)));
    counts
}

/// Call only after a complete encoded page reaches its private output file.
pub(crate) fn written(bytes: &[u8]) {
    if !timing::enabled() {
        return;
    }
    let class = match bytes[10] {
        12 => 0,
        13 => 1,
        3 => 2,
        1 => 3,
        4 => 4,
        5 => 5,
        _ => return,
    };
    image(&WRITTEN[class], bytes);
}

/// Header finalization writes an existing private map page, not a new allocation.
pub(crate) fn map_rewritten(bytes: &[u8]) {
    if timing::enabled() {
        image(&MAP_REWRITE, bytes);
    }
}

pub(crate) fn input(kind: u8, used: usize, entries: usize, root: bool, private: bool) {
    if !timing::enabled() {
        return;
    }
    let class = match (kind, root) {
        (12, true) => 0,
        (12, false) => 1,
        (3, true) => 2,
        (3, false) => 3,
        (13, _) => 4,
        (1, _) => 5,
        _ => return,
    };
    record(&INPUT[class], used, entries);
    timing::count("snapshot_private_input_pages", u64::from(private));
}

fn image(names: &[&'static str; 8], bytes: &[u8]) {
    let used = u32::from_le_bytes(bytes[48..52].try_into().expect("encoded payload length"));
    let entries = u16::from_le_bytes(bytes[52..54].try_into().expect("encoded entry count"));
    record(names, used as usize, entries as usize);
}

pub(crate) fn split(locator: bool, parts: usize, middle_insert: bool) {
    if timing::enabled() && parts > 1 {
        let names = &SPLITS[usize::from(!locator)];
        timing::count(names[0], 1);
        timing::count(names[1], parts as u64);
        timing::count(names[2], u64::from(middle_insert));
    }
}

fn record(names: &[&'static str; 8], used: usize, entries: usize) {
    if used > super::PAGE_PAYLOAD {
        timing::count("snapshot_invalid_usage_pages", 1);
        return;
    }
    timing::count(names[0], 1);
    timing::count(names[1], used as u64);
    timing::count(names[2], entries as u64);
    timing::count(names[3], u64::from(used == 0));
    let quarter = (used * 4).div_ceil(crate::disk_pack::PAGE_PAYLOAD).max(1);
    timing::count(names[3 + quarter], 1);
}
