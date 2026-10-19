//! Merge compatible encoded key lanes in API range order before tip resolution.

use super::*;
use crate::atom::MoleculeKeyCodec;
use crate::resident::core_keys::CoreKeyRange;
use crate::schema::types::field::KeyWindow;
use std::collections::BTreeMap;

struct ReadyLane {
    lease: CoreKeyLease,
    hash: String,
    prefix: Option<String>,
    encoding_index: usize,
}
type SelectedLanes = (Vec<CoreStoredKey>, Option<String>);

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
        let codec = self.key_codec_for_molecule(molecule);
        let Some(lanes) = self.ready_hash_lanes(&runtime, molecule, api_hash, scope, &codec)?
        else {
            return Ok(None);
        };
        let mut output = Vec::new();
        while output.len() < limit {
            let ask = skip.saturating_add(limit - output.len()).clamp(1, 1024);
            let Some(selected) = runtime.with_schema_key_selection(|| {
                select_lanes(&lanes, &codec, molecule, after.as_deref(), ask)
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
            let mut union = BTreeMap::new();
            for (base, record) in rows {
                let (_, range) = molecule_key_codec::decode_hash_range_any(&base)
                    .ok_or_else(|| key_error("invalid ready tip coordinates"))?;
                let range = decode_range(&codec, range);
                union.entry(range).or_insert((base, record));
            }
            for (range, row) in union {
                if frontier.as_ref().is_some_and(|last| range > *last) {
                    break;
                }
                if after.as_ref().is_some_and(|cursor| range <= *cursor) {
                    continue;
                }
                if !include_tombstones && row.1.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                } else {
                    output.push(row);
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

    fn ready_hash_lanes(
        &self,
        runtime: &Arc<PackRuntime>,
        molecule: &str,
        api_hash: &str,
        scope: Option<&str>,
        codec: &MoleculeKeyCodec,
    ) -> Result<Option<Vec<ReadyLane>>, SchemaError> {
        let mut lanes = Vec::new();
        let Some(lease) = self.ready_key_lease(runtime, scope, molecule) else {
            return Ok(None);
        };
        let families = if codec.range_encoding().writes_ope() {
            let ranges = codec
                .storage_range_read_candidates(molecule, "\0")
                .map_err(key_error)?;
            let families: Vec<_> = ranges
                .iter()
                .map(|range| Some(range[..6].to_string()))
                .collect();
            if families
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != families.len()
            {
                return Ok(None);
            }
            families
        } else {
            vec![None]
        };
        for hash in codec
            .storage_hash_read_candidates(molecule, api_hash)
            .map_err(key_error)?
        {
            for (encoding_index, prefix) in families.iter().enumerate() {
                lanes.push(ReadyLane {
                    lease: lease.clone(),
                    hash: hash.clone(),
                    prefix: prefix.clone(),
                    encoding_index,
                });
            }
        }
        Ok(Some(lanes))
    }
}

fn select_lanes(
    lanes: &[ReadyLane],
    codec: &MoleculeKeyCodec,
    molecule: &str,
    after: Option<&str>,
    ask: usize,
) -> Result<SelectedLanes, SchemaError> {
    let mut selected = Vec::new();
    let mut frontier: Option<String> = None;
    for lane in lanes {
        if after.is_none() && lane.prefix.is_some() && lane.encoding_index == 0 {
            let mut empty = CoreKeyRange::all(&lane.hash, 1);
            empty.start = std::ops::Bound::Included("");
            empty.end = std::ops::Bound::Included("");
            empty.include_tombstones = true;
            selected.extend(lane.lease.select(empty).map_err(key_error)?.keys);
        }
        let encoded_after = after
            .filter(|range| !range.is_empty() || lane.prefix.is_none())
            .map(|range| codec.storage_range_read_candidates(molecule, range))
            .transpose()
            .map_err(key_error)?;
        let mut request = CoreKeyRange::all(&lane.hash, ask);
        request.after = encoded_after
            .as_ref()
            .and_then(|forms| forms.get(lane.encoding_index))
            .map(String::as_str);
        request.prefix = lane.prefix.as_deref();
        request.include_tombstones = true;
        let page = lane.lease.select(request).map_err(key_error)?;
        if !page.exhausted {
            if let Some(last) = page.keys.last() {
                let range = decode_range(codec, last.range.clone());
                frontier = Some(frontier.map_or(range.clone(), |current| current.min(range)));
            }
        }
        selected.extend(page.keys);
    }
    Ok((selected, frontier))
}

fn decode_range(codec: &MoleculeKeyCodec, range: String) -> String {
    if codec.range_encoding().writes_ope() {
        crate::crypto::E2eKeys::ope_decode_range_plaintext(&range).unwrap_or(range)
    } else {
        range
    }
}
