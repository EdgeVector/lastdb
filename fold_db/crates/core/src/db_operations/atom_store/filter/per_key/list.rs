//! Bounded physical-key pages of one molecule.

use super::*;
use futures::FutureExt;

mod logical;
mod values;

type KeyPage = (Vec<(String, String)>, Option<String>, bool);

struct KeyList<'a> {
    molecule: &'a str,
    limit: usize,
    after: Option<&'a str>,
    scope: Option<&'a str>,
    hash: Option<&'a str>,
}

impl AtomStore {
    /// Keys-only page of the live physical membership of one molecule.
    /// The continuation remains an exclusive physical `mk:` key.
    pub async fn list_live_record_keys(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<KeyPage, SchemaError> {
        self.list_live_record_keys_filtered(molecule, limit, after, storage_prefix, None)
            .await
    }

    /// Physical membership, optionally restricted to the caller's API hash.
    pub async fn list_live_record_keys_filtered(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<KeyPage, SchemaError> {
        self.list_record_keys_read(KeyList {
            molecule,
            limit,
            after,
            scope: storage_prefix,
            hash: hash_filter,
        })
        .await
    }

    async fn list_record_keys_read(&self, request: KeyList<'_>) -> Result<KeyPage, SchemaError> {
        self.atom_read_scope(self.list_record_keys_captured(request).boxed())
            .await
    }

    async fn list_record_keys_captured(
        &self,
        request: KeyList<'_>,
    ) -> Result<KeyPage, SchemaError> {
        if request.limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        self.load_molecule_key_bundle(request.molecule).await?;
        let codec = self.key_codec_for_molecule(request.molecule)?;
        let prefix = match request.hash {
            Some(hash) => codec
                .api_hash_range_scan_prefix_for_hash(request.molecule, hash)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
            None => molecule_key_codec::molecule_record_prefix(request.molecule),
        };
        let scope_prefix = build_storage_key(request.scope, "");
        let after = request
            .after
            .map(|cursor| cursor.strip_prefix(&scope_prefix).unwrap_or(cursor));
        if after.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
            return Err(SchemaError::InvalidCursor(
                "cursor is not in this schema's key molecule".into(),
            ));
        }
        let want = request.limit.saturating_add(1);
        let mut cursor = after.map(str::to_owned);
        let mut selected = Vec::new();
        let mut refilling = false;
        let mut unfinished = false;
        while selected.len() < want {
            let remaining = (want - selected.len()).min(128);
            let ask = if refilling {
                remaining.max(64)
            } else {
                remaining
            };
            let rows = self
                .scan_logical_tip_page(&prefix, cursor.as_deref(), request.scope, ask)
                .await?;
            let exhausted = rows.len() < ask;
            let Some((last, _)) = rows.last() else { break };
            if cursor.as_ref().is_some_and(|cursor| last <= cursor) {
                return Err(SchemaError::InvalidData(
                    "listed molecule key page did not advance".into(),
                ));
            }
            cursor = Some(last.clone());
            let rows = self
                .filter_delete_barriers_for_records(rows, request.scope)
                .await?;
            selected.extend(self.list_key_values(&request, &codec, rows).await?);
            refilling = true;
            if exhausted {
                break;
            }
            if self
                .namespaced_store
                .as_ref()
                .and_then(|store| store.disk_pack_runtime())
                .is_some_and(|runtime| runtime.list_page_should_yield())
            {
                unfinished = true;
                break;
            }
        }
        let has_more = selected.len() > request.limit || unfinished;
        selected.truncate(request.limit);
        let next_cursor = if has_more {
            selected.last().map(|item| item.2.clone()).or(cursor)
        } else {
            None
        };
        let keys = selected
            .into_iter()
            .map(|(hash, range, _)| (hash, range))
            .collect();
        Ok((keys, next_cursor, has_more))
    }
}
