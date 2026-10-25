//! Streaming key validation under the writer's already held generation lease.

use super::{
    artifact, key_layout, ReadCall, TreeRoot, FEATURE_EXTENDED_KEYS, PAGE_BYTES, PAGE_PAYLOAD,
};
use crate::Result;
use std::borrow::Cow;

/// A page can contain many external keys. Its occupancy and tree height bound
/// total administrative work; an ordinary query budget must not make an
/// already accepted leaf permanently unwritable. Retain only one key's pages.
pub(super) fn key_bytes<'a>(
    call: &mut ReadCall,
    tree: &TreeRoot,
    entry: &'a [u8],
) -> Result<Cow<'a, [u8]>> {
    if call.current.required_features & FEATURE_EXTENDED_KEYS == 0
        || key_layout::overflow(entry) == 0
    {
        return call.key_bytes(tree, entry);
    }
    let maximum = call.current.storage_key_byte_limit();
    let mut selected = artifact::metadata_call(&call.root, call.current.clone());
    selected.budget.pages = maximum.saturating_sub(128).div_ceil(PAGE_PAYLOAD) as u64 + 1;
    selected.budget.bytes = 3 * maximum as u64 + 16 * PAGE_BYTES as u64;
    selected.budget.blocks = 1;
    selected.budget.keys = 1;
    let key = selected.full_key(tree, entry)?;
    crate::commit_timing::count(
        "extended_mutation_validation_pages",
        selected.costs.overflow_pages,
    );
    crate::commit_timing::count(
        "extended_mutation_validation_bytes",
        selected.costs.file_bytes,
    );
    Ok(Cow::Owned(key))
}
