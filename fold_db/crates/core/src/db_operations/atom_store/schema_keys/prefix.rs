//! Stored prefix/range routes reuse ready keys and exact batched tip reads.

use super::*;
use crate::resident::core_keys::CoreKeyRange;
use std::ops::Bound;

enum Selection {
    All,
    Hash {
        hash: String,
        prefix: Option<String>,
        end: Option<String>,
    },
}

struct Source {
    priority: usize,
    molecule: String,
    lease: CoreKeyLease,
    selection: Selection,
    after: Option<(String, String)>,
    exhausted: bool,
    api_hash: Option<String>,
}

impl Source {
    fn next(&mut self, limit: usize) -> Result<Vec<CoreStoredKey>, SchemaError> {
        let page = match &self.selection {
            Selection::All => self.lease.page(
                limit,
                self.after
                    .as_ref()
                    .map(|(range, hash)| (range.as_str(), hash.as_str())),
                true,
            ),
            Selection::Hash { hash, prefix, end } => {
                let mut request = CoreKeyRange::all(hash, limit);
                request.after = self.after.as_ref().map(|(range, _)| range.as_str());
                request.include_tombstones = true;
                if let Some(prefix) = prefix {
                    request.start = Bound::Included(prefix);
                    if end.is_none() {
                        request.prefix = Some(prefix);
                    }
                }
                if let Some(end) = end {
                    request.end = Bound::Excluded(end);
                }
                self.lease.select(request)
            }
        }
        .map_err(key_error)?;
        self.exhausted = page.exhausted;
        if let Some(last) = page.keys.last() {
            self.after = Some((last.range.clone(), last.hash.clone()));
        } else if !page.exhausted {
            return Err(key_error("ready key page does not advance"));
        }
        Ok(page.keys)
    }
}

impl AtomStore {
    pub(crate) async fn preloaded_per_key_prefixes(
        &self,
        prefixes: &[String],
        scope: Option<&str>,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        self.preloaded_per_key_prefixes_for_api(prefixes, scope, None)
            .await
    }

    pub(crate) async fn preloaded_per_key_prefixes_for_api(
        &self,
        prefixes: &[String],
        scope: Option<&str>,
        api_hash: Option<&str>,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let Some(runtime) = self.schema_keys_runtime() else {
            return Ok(None);
        };
        let mut sources = Vec::new();
        for (priority, prefix) in prefixes.iter().enumerate() {
            let Some(molecule) = molecule_key_codec::molecule_uuid_from_storage_key(prefix) else {
                return Ok(None);
            };
            let Some(lease) = self.ready_key_lease(&runtime, scope, molecule) else {
                return Ok(None);
            };
            let selection = if prefix == &molecule_key_codec::molecule_record_prefix(molecule) {
                Selection::All
            } else if let Some((hash, range)) = molecule_key_codec::decode_hash_range_any(prefix) {
                Selection::Hash {
                    hash,
                    prefix: Some(range),
                    end: None,
                }
            } else {
                return Ok(None);
            };
            sources.push(Source {
                priority,
                molecule: molecule.to_string(),
                lease,
                selection,
                after: None,
                exhausted: false,
                api_hash: api_hash.map(str::to_owned),
            });
        }
        self.collect_preloaded_sources(&runtime, scope, sources)
            .await
    }

    pub(crate) async fn preloaded_per_key_range(
        &self,
        start: &str,
        end: &str,
        scope: Option<&str>,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        self.preloaded_per_key_range_for_api(start, end, scope, None)
            .await
    }

    pub(crate) async fn preloaded_per_key_range_for_api(
        &self,
        start: &str,
        end: &str,
        scope: Option<&str>,
        api_hash: Option<&str>,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let Some(molecule) = molecule_key_codec::molecule_uuid_from_storage_key(start) else {
            return Ok(None);
        };
        if molecule_key_codec::molecule_uuid_from_storage_key(end) != Some(molecule) {
            return Ok(None);
        }
        let Some((hash, range)) = molecule_key_codec::decode_hash_range_any(start) else {
            return Ok(None);
        };
        let Some((end_hash, end_range)) = molecule_key_codec::decode_hash_range_any(end) else {
            return Ok(None);
        };
        if hash != end_hash {
            return Ok(None);
        }
        let Some(runtime) = self.schema_keys_runtime() else {
            return Ok(None);
        };
        let Some(lease) = self.ready_key_lease(&runtime, scope, molecule) else {
            return Ok(None);
        };
        let source = Source {
            priority: 0,
            molecule: molecule.to_string(),
            lease,
            selection: Selection::Hash {
                hash,
                prefix: Some(range),
                end: Some(end_range),
            },
            after: None,
            exhausted: false,
            api_hash: api_hash.map(str::to_owned),
        };
        self.collect_preloaded_sources(&runtime, scope, vec![source])
            .await
    }

    async fn collect_preloaded_sources(
        &self,
        runtime: &Arc<PackRuntime>,
        scope: Option<&str>,
        mut sources: Vec<Source>,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let chunk = (4096 / sources.len().max(1)).max(1);
        let mut output = std::collections::HashMap::new();
        while sources.iter().any(|source| !source.exhausted) {
            let mut keys = Vec::new();
            let mut priorities = std::collections::HashMap::new();
            for source in sources.iter_mut().filter(|source| !source.exhausted) {
                let ope = self
                    .key_codec_for_molecule(&source.molecule)
                    .range_encoding()
                    .writes_ope();
                let Some(next) = runtime.with_schema_key_selection(|| source.next(chunk)) else {
                    return Ok(None);
                };
                for key in next? {
                    let key =
                        ReadyTipKey::new(&source.molecule, key, source.api_hash.as_deref(), ope);
                    priorities
                        .entry(key.base.clone())
                        .or_insert(source.priority);
                    keys.push(key);
                }
            }
            let rows = self
                .resolve_preloaded_tip_keys(runtime, scope, keys)
                .await?;
            if sources
                .iter()
                .any(|source| runtime.schema_key_lease(scope, &source.molecule).is_none())
            {
                return Ok(None);
            }
            for row in rows {
                let slot = molecule_key_codec::decode_hash_range_any(&row.0)
                    .ok_or_else(|| key_error("invalid ready prefix key"))?;
                let priority = priorities.get(&row.0).copied().unwrap_or(usize::MAX);
                let replace = output.get(&slot).is_none_or(|(old, _)| priority < *old);
                if replace {
                    output.insert(slot, (priority, row));
                }
            }
        }
        let mut rows: Vec<_> = output.into_values().collect();
        rows.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1 .0.cmp(&right.1 .0))
        });
        Ok(Some(rows.into_iter().map(|(_, row)| row).collect()))
    }
}
