//! Select one fixed-encoding range window before batched current-tip resolution.

use super::*;
use crate::atom::MoleculeKeyCodec;
use crate::db_operations::atom_store::filter::HashTipSelection;
use crate::resident::core_keys::CoreKeyRange;
use std::ops::Bound;

type SelectedWindow = (Vec<CoreStoredKey>, Option<String>);

impl AtomStore {
    pub(crate) async fn preloaded_window(
        &self,
        molecule: &str,
        selection: &HashTipSelection<'_>,
        scope: Option<&str>,
        include_tombstones: bool,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let (mut skip, limit, mut after) =
            (selection.skip, selection.limit, selection.after.clone());
        let Some(runtime) = self.schema_keys_runtime() else {
            return Ok(None);
        };
        let codec = self.key_codec_for_molecule(molecule)?;
        let Some(lease) = self.ready_key_lease(&runtime, scope, molecule) else {
            return Ok(None);
        };
        let mut output = Vec::new();
        while output.len() < limit {
            let ask = skip.saturating_add(limit - output.len()).clamp(1, 1024);
            let Some(selected) = runtime.with_schema_key_selection(|| {
                select_window(&lease, selection, &codec, molecule, after.as_deref(), ask)
            }) else {
                return Ok(None);
            };
            let (selected, frontier) = selected?;
            let selected = selected
                .iter()
                .map(|key| ReadyTipKey::new(molecule, key, Some((selection.api_hash, &codec))))
                .collect::<Result<_, SchemaError>>()?;
            let rows = self
                .resolve_preloaded_tip_keys(&runtime, scope, selected)
                .await?;
            if runtime.schema_key_lease(scope, molecule).is_none() {
                return Ok(None);
            }
            for (base, record) in rows {
                let (_, range) = molecule_key_codec::decode_hash_range_any(&base)
                    .ok_or_else(|| key_error("invalid ready tip coordinates"))?;
                let range = codec.api_range(molecule, &range).map_err(key_error)?;
                if frontier.as_ref().is_some_and(|last| range > *last) {
                    break;
                }
                if after.as_ref().is_some_and(|cursor| range <= *cursor) {
                    continue;
                }
                if !include_tombstones && record.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                } else {
                    output.push((base, record));
                }
                if output.len() == limit {
                    break;
                }
            }
            let Some(frontier) = frontier else { break };
            if after.as_ref().is_some_and(|cursor| frontier <= *cursor) {
                break;
            }
            after = Some(frontier);
        }
        Ok(Some(output))
    }
}

fn select_window(
    lease: &CoreKeyLease,
    selection: &HashTipSelection<'_>,
    codec: &MoleculeKeyCodec,
    molecule: &str,
    after: Option<&str>,
    ask: usize,
) -> Result<SelectedWindow, SchemaError> {
    let encoded_after = after
        .map(|range| codec.storage_range(molecule, range))
        .transpose()
        .map_err(key_error)?;
    let mut request = CoreKeyRange::all(&selection.storage_hash, ask);
    request.start = Bound::Included(&selection.range_start);
    request.end = selection
        .range_end
        .as_deref()
        .map_or(Bound::Unbounded, Bound::Excluded);
    request.prefix = selection.range_prefix.as_deref();
    request.after = encoded_after.as_deref();
    request.include_tombstones = true;
    let page = lease.select(request).map_err(key_error)?;
    let frontier = if page.exhausted {
        None
    } else {
        let last = page
            .keys
            .last()
            .ok_or_else(|| key_error("ready range window does not advance"))?;
        Some(codec.api_range(molecule, &last.range).map_err(key_error)?)
    };
    Ok((page.keys, frontier))
}
