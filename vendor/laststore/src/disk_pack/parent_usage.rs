//! Bounded numeric observations of incremental parent reconstruction, not a page census.

use super::page::Page;
use super::TreeKind;
use crate::commit_timing as timing;
use std::collections::BTreeMap;

macro_rules! modes {
    ($p:literal) => {
        [
            concat!($p, "_edit_started"),
            concat!($p, "_edit_completed"),
            concat!($p, "_edit_keys"),
            concat!($p, "_root_leaf_visits"),
            concat!($p, "_root_parent_visits"),
            concat!($p, "_root_leaf_promotions"),
            concat!($p, "_root_parent_to_leaf"),
            concat!($p, "_parent_input_pages"),
            concat!($p, "_parent_rewrite_inputs"),
            concat!($p, "_parent_split_extra_pages"),
            concat!($p, "_parent_added_level_pages"),
            concat!($p, "_parent_output_pages"),
            concat!($p, "_parent_added_level_calls"),
        ]
    };
}
macro_rules! inputs {
    ($p:literal) => {
        [
            concat!($p, "_pages"),
            concat!($p, "_payload_bytes"),
            concat!($p, "_children"),
            concat!($p, "_same_generation_pages"),
            concat!($p, "_older_generation_pages"),
            concat!($p, "_children_le2"),
            concat!($p, "_children_le4"),
            concat!($p, "_children_le8"),
            concat!($p, "_children_le16"),
            concat!($p, "_children_le32"),
            concat!($p, "_children_gt32"),
        ]
    };
}
macro_rules! levels {
    ($p:literal) => {
        [
            concat!($p, "_1_pages"),
            concat!($p, "_2_pages"),
            concat!($p, "_3_pages"),
            concat!($p, "_4_pages"),
            concat!($p, "_5_pages"),
            concat!($p, "_6_pages"),
            concat!($p, "_7_pages"),
        ]
    };
}
macro_rules! outputs {
    ($p:literal) => {
        [
            concat!($p, "_from_root_input_output_pages"),
            concat!($p, "_from_nonroot_input_output_pages"),
            concat!($p, "_added_level_output_pages"),
        ]
    };
}

const MODES: [[&str; 13]; 4] = [
    modes!("locator_tracked"),
    modes!("locator_untracked"),
    modes!("directory_tracked"),
    modes!("directory_untracked"),
];
const INPUTS: [[&str; 11]; 4] = [
    inputs!("locator_parent_root_input"),
    inputs!("locator_parent_nonroot_input"),
    inputs!("directory_parent_root_input"),
    inputs!("directory_parent_nonroot_input"),
];
const LEVELS: [[&str; 7]; 2] = [
    levels!("locator_parent_input_level"),
    levels!("directory_parent_input_level"),
];
const OUTPUTS: [[&str; 3]; 2] = [outputs!("locator_parent"), outputs!("directory_parent")];
const VALUES: [[&str; 3]; 2] = [
    [
        "locator_edit_values_inserted",
        "locator_edit_values_changed",
        "locator_edit_values_identical",
    ],
    [
        "directory_edit_values_inserted",
        "directory_edit_values_changed",
        "directory_edit_values_identical",
    ],
];
const CONTROL: [&str; 6] = [
    "snapshot_parent_usage_version",
    "parent_invalid_inputs",
    "parent_invalid_outputs",
    "publisher_batch_calls",
    "locator_template_create_calls",
    "locator_bulk_create_calls",
];

pub(super) fn initial_counts() -> BTreeMap<&'static str, u64> {
    let names = MODES
        .iter()
        .flatten()
        .chain(INPUTS.iter().flatten())
        .chain(LEVELS.iter().flatten())
        .chain(OUTPUTS.iter().flatten())
        .chain(VALUES.iter().flatten())
        .chain(CONTROL.iter());
    let mut counts: BTreeMap<_, _> = names.map(|&name| (name, 0)).collect();
    counts.insert("snapshot_parent_usage_version", 1);
    counts
}

fn kind_index(kind: TreeKind) -> usize {
    usize::from(kind == TreeKind::Directory)
}

fn mode(kind: TreeKind, tracked: bool) -> &'static [&'static str; 13] {
    &MODES[kind_index(kind) * 2 + usize::from(!tracked)]
}

pub(super) fn started(kind: TreeKind, tracked: bool, keys: usize) {
    if timing::enabled() {
        let names = mode(kind, tracked);
        timing::count(names[0], 1);
        timing::count(names[2], keys as u64);
    }
}

pub(super) fn completed(kind: TreeKind, tracked: bool, before: u8, after: u8) {
    if timing::enabled() {
        let names = mode(kind, tracked);
        timing::count(names[1], 1);
        timing::count(names[5], u64::from(before == 1 && after > 1));
        timing::count(names[6], u64::from(before > 1 && after == 1));
    }
}

/// Generation equality is not a private/published classification. Tracked is
/// lifetime-reference accounting mode, not foreground/compaction identity.
pub(super) fn input(kind: TreeKind, page: &Page, root: bool, generation: u64, tracked: bool) {
    if !timing::enabled() {
        return;
    }
    let names = mode(kind, tracked);
    if root {
        timing::count(names[if page.level() == 0 { 3 } else { 4 }], 1);
    }
    if page.level() == 0 {
        return;
    }
    let children = page.count() + 1;
    if !(1..=7).contains(&page.level())
        || children < 2
        || page.payload().len() > super::PAGE_PAYLOAD
    {
        timing::count("parent_invalid_inputs", 1);
        return;
    }
    let tree = kind_index(kind);
    let input = &INPUTS[tree * 2 + usize::from(!root)];
    timing::count(names[7], 1);
    timing::count(input[0], 1);
    timing::count(input[1], page.payload().len() as u64);
    timing::count(input[2], children as u64);
    timing::count(
        input[if page.generation() == generation {
            3
        } else {
            4
        }],
        1,
    );
    let band = [2, 4, 8, 16, 32].partition_point(|&limit| children > limit);
    timing::count(input[5 + band], 1);
    timing::count(LEVELS[tree][page.level() as usize - 1], 1);
}

#[derive(Clone, Copy)]
pub(super) enum Origin {
    Root,
    Nonroot,
    AddedLevel,
}

impl Origin {
    pub(super) fn from_root(root: bool) -> Self {
        if root {
            Self::Root
        } else {
            Self::Nonroot
        }
    }
}

/// Counts emitted pages after every page in this reconstruction succeeds.
/// From-root output may split; it does not necessarily describe final roots.
pub(super) fn output(kind: TreeKind, tracked: bool, origin: Origin, pages: usize) {
    if !timing::enabled() {
        return;
    }
    let Some(extra) = pages.checked_sub(1) else {
        timing::count("parent_invalid_outputs", 1);
        return;
    };
    let names = mode(kind, tracked);
    timing::count(names[11], pages as u64);
    match origin {
        Origin::AddedLevel => {
            timing::count(names[10], pages as u64);
            timing::count(names[12], 1);
        }
        _ => {
            timing::count(names[8], 1);
            timing::count(names[9], extra as u64);
        }
    }
    timing::count(OUTPUTS[kind_index(kind)][origin as usize], pages as u64);
}

/// Only explicit TreeEdit keys, not unchanged copied leaf entries or bulk keys.
pub(super) fn value(kind: TreeKind, old: Option<&[u8]>, new: &[u8; 128]) {
    if timing::enabled() {
        let index = match old {
            None => 0,
            Some(old) if old == new.as_slice() => 2,
            Some(_) => 1,
        };
        timing::count(VALUES[kind_index(kind)][index], 1);
    }
}
