//! Select one fixed-encoding range window before batched current-tip resolution.

use super::*;
use crate::atom::MoleculeKeyCodec;
use crate::resident::core_keys::CoreKeyRange;
use crate::schema::types::field::KeyWindow;

type SelectedWindow = (Vec<CoreStoredKey>, Option<String>);

impl AtomStore {
    pub(crate) async fn preloaded_window(
        &self,
        molecule: &str,
        hash: &str,
        scope: Option<&str>,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        self.preloaded_hash_tip_window(molecule, hash, scope, Some(window), include_tombstones)
            .await
    }

    pub(crate) async fn preloaded_hash_tip_window(
        &self,
        molecule: &str,
        api_hash: &str,
        scope: Option<&str>,
        window: Option<&KeyWindow>,
        include_tombstones: bool,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let (mut skip, limit, mut after) = match window {
            Some(KeyWindow::Offset { offset, limit }) => (*offset, *limit, None),
            Some(KeyWindow::After { after, limit }) if after.hash.as_deref() == Some(api_hash) => {
                (0, *limit, Some(after.range.clone().unwrap_or_default()))
            }
            Some(KeyWindow::After { .. }) => return Ok(None),
            None => (0, usize::MAX, None),
        };
        let Some(runtime) = self.schema_keys_runtime() else {
            return Ok(None);
        };
        let codec = self.key_codec_for_molecule(molecule)?;
        let Some(lease) = self.ready_key_lease(&runtime, scope, molecule) else {
            return Ok(None);
        };
        let hash = codec.storage_hash(molecule, api_hash).map_err(key_error)?;
        let mut output = Vec::new();
        while output.len() < limit {
            let ask = skip.saturating_add(limit - output.len()).clamp(1, 1024);
            let Some(selected) = runtime.with_schema_key_selection(|| {
                select_window(&lease, &hash, &codec, molecule, after.as_deref(), ask)
            }) else {
                return Ok(None);
            };
            let (selected, frontier) = selected?;
            let selected = selected
                .into_iter()
                .map(|key| {
                    ReadyTipKey::new(
                        molecule,
                        key,
                        Some(api_hash),
                        codec.range_encoding().writes_ope(),
                    )
                })
                .collect();
            let rows = self
                .resolve_preloaded_tip_keys(&runtime, scope, selected)
                .await?;
            if runtime.schema_key_lease(scope, molecule).is_none() {
                return Ok(None);
            }
            for (base, record) in rows {
                let (_, range) = molecule_key_codec::decode_hash_range_any(&base)
                    .ok_or_else(|| key_error("invalid ready tip coordinates"))?;
                let range = decode_range(&codec, range);
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
    hash: &str,
    codec: &MoleculeKeyCodec,
    molecule: &str,
    after: Option<&str>,
    ask: usize,
) -> Result<SelectedWindow, SchemaError> {
    let encoded_after = after
        .map(|range| codec.storage_range(molecule, range))
        .transpose()
        .map_err(key_error)?;
    let mut request = CoreKeyRange::all(hash, ask);
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
        Some(decode_range(codec, last.range.clone()))
    };
    Ok((page.keys, frontier))
}

fn decode_range(codec: &MoleculeKeyCodec, range: String) -> String {
    if codec.range_encoding().writes_ope() {
        crate::crypto::E2eKeys::ope_decode_range_plaintext(&range).unwrap_or(range)
    } else {
        range
    }
}
