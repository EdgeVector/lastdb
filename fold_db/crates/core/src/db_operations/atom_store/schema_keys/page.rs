//! Global preview order comes from complete membership, without disk discovery.

use super::*;

impl AtomStore {
    pub(crate) async fn preloaded_hash_range_keys_page_window(
        &self,
        molecule: &str,
        scope: Option<&str>,
        take: Option<usize>,
        after: Option<(&str, &str)>,
    ) -> Result<Option<Vec<(String, String)>>, SchemaError> {
        let Some(runtime) = self.schema_keys_runtime() else {
            return Ok(None);
        };
        let Some(lease) = self.ready_key_lease(&runtime, scope, molecule) else {
            return Ok(None);
        };
        // The existing return type cannot carry another physical molecule
        // spelling into its later exact fetch. Such homes use the disk route.
        for uid in crate::atom::molecule_uuid_read_candidates(molecule) {
            if uid == molecule {
                continue;
            }
            let Some(alias) = self.ready_key_lease(&runtime, scope, &uid) else {
                return Ok(None);
            };
            if !alias
                .page(1, None, true)
                .map_err(key_error)?
                .keys
                .is_empty()
            {
                return Ok(None);
            }
        }
        let want = take.unwrap_or(usize::MAX);
        let mut output = Vec::new();
        let mut cursor = after.map(|(range, hash)| (range.to_string(), hash.to_string()));
        while output.len() < want {
            let ask = (want - output.len()).min(1024);
            let page = lease
                .page(
                    ask,
                    cursor
                        .as_ref()
                        .map(|(range, hash)| (range.as_str(), hash.as_str())),
                    true,
                )
                .map_err(key_error)?;
            if let Some(last) = page.keys.last() {
                cursor = Some((last.range.clone(), last.hash.clone()));
            } else if !page.exhausted {
                return Err(key_error("ready preview page does not advance"));
            }
            let selected = page
                .keys
                .into_iter()
                .map(|key| ReadyTipKey::new(molecule, key, None, false))
                .collect();
            let visible = self
                .resolve_preloaded_tip_keys(&runtime, scope, selected)
                .await?;
            if crate::atom::molecule_uuid_read_candidates(molecule)
                .iter()
                .any(|uid| runtime.schema_key_lease(scope, uid).is_none())
            {
                return Ok(None);
            }
            for (base, _) in visible {
                let key = molecule_key_codec::decode_hash_range_any(&base)
                    .ok_or_else(|| key_error("invalid ready preview key"))?;
                output.push(key);
            }
            if page.exhausted {
                break;
            }
        }
        Ok(Some(output))
    }
}
