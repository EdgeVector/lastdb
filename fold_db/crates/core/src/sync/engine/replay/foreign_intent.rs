//! A foreign share intent can write only its exact retained receiver scope.

use crate::sync::{SyncResult, SyncTarget};

pub(super) fn receiver_mutations(
    mutations: &[crate::sync::log::MutationEnvelope],
    target: Option<&SyncTarget>,
) -> SyncResult<Vec<crate::sync::log::MutationEnvelope>> {
    let Some((target, binding)) = target.and_then(|target| {
        target
            .read_binding
            .as_ref()
            .map(|binding| (target, binding))
    }) else {
        return Ok(mutations.to_vec());
    };
    if mutations
        .iter()
        .any(|mutation| mutation.storage_prefix.as_deref() != Some(target.prefix.as_str()))
    {
        return Err(crate::sync::cloud_share::held(
            "a foreign mutation intent escaped its signed share scope",
        ));
    }
    Ok(mutations
        .iter()
        .map(|mutation| {
            let mut mutation = mutation.clone();
            mutation.storage_prefix = Some(binding.storage_prefix.clone());
            mutation
        })
        .collect())
}
