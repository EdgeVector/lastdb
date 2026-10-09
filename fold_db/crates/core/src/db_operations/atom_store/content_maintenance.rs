// lint:file-size-ok verbatim move out of the 2.6k-line atom_store/mod.rs; one AtomStore theme per file, split further when next touched
//! Bounded maintenance passes over atom content: reseal, recompress, repack.

use super::*;

/// Stats from [`AtomStore::reseal_plain_atom_content`].
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ResealAtomContentStats {
    pub scanned: usize,
    pub resealed: usize,
    pub already_sealed: usize,
    pub errors: usize,
}

/// Result of one bounded [`AtomStore::recompress_atom_content_page`] call.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RecompressAtomContentReport {
    pub scanned: usize,
    pub recompressed: usize,
    pub already_compressed: usize,
    pub already_binary: usize,
    pub compression_not_helpful: usize,
    pub not_legacy_enc: usize,
    pub errors: usize,
    pub sealed_bytes_before: u64,
    pub sealed_bytes_after: u64,
    pub bytes_saved: u64,
    pub more_remaining: bool,
    pub next_after_key: Option<String>,
    pub last_walked_key: Option<String>,
}

/// Result of one bounded binary atom-content repack page.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RepackAtomContentReport {
    pub scanned: usize,
    pub repacked: usize,
    pub already_binary: usize,
    pub errors: usize,
    pub inner_bytes_before: u64,
    pub inner_bytes_after: u64,
    pub inner_bytes_saved: u64,
    pub row_bytes_before: u64,
    pub row_bytes_after: u64,
    pub row_bytes_saved: u64,
    pub more_remaining: bool,
    pub next_after_key: Option<String>,
    pub last_walked_key: Option<String>,
}

impl AtomStore {
    /// Operation Trinity (Son): re-seal any plain atom `content` fields under
    /// the configured content key. Idempotent for already-`ENC:` content.
    ///
    /// Pages through canonical `atom:` rows (not schema index copies) so large
    /// homes do not materialize the full prefix in RAM. Requires
    /// [`Self::content_key`]. After this pass, `LASTDB_ATOM_CONTENT_STRICT=1`
    /// can fail closed.
    pub async fn reseal_plain_atom_content(
        &self,
    ) -> Result<ResealAtomContentStats, crate::schema::SchemaError> {
        let Some(key) = self.content_key else {
            return Err(crate::schema::SchemaError::InvalidData(
                "reseal_plain_atom_content requires atom content_key".into(),
            ));
        };

        // Keyset pagination over `atom\0{uuid}` and leftover `atom:{uuid}`.
        const PAGE: usize = 256;
        let (plane_start, plane_end) =
            crate::kind_partition::colon_plane_bounds(crate::atom::atom_key_codec::ATOM_PREFIX);
        let mut stats = ResealAtomContentStats::default();
        let mut start = plane_start.clone();

        loop {
            let page = self
                .main_store
                .inner()
                .scan_range_paged(start.as_bytes(), plane_end.as_bytes(), PAGE)
                .await
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!("reseal range page: {e}"))
                })?;

            if page.is_empty() {
                break;
            }

            let mut last_key = None;
            for (storage_key_bytes, stored) in page {
                let storage_key = String::from_utf8_lossy(&storage_key_bytes).into_owned();
                if crate::kind_partition::rest_of(&storage_key, "atom").is_none() {
                    continue;
                }
                stats.scanned += 1;
                last_key = Some(storage_key.clone());
                if stored.starts_with(crate::atom::ATOM_BINARY_ROW_PREFIX) {
                    stats.already_sealed += 1;
                    continue;
                }
                let mut value: serde_json::Value =
                    serde_json::from_slice(&stored).map_err(|e| {
                        crate::schema::SchemaError::InvalidData(format!(
                            "reseal decode {storage_key}: {e}"
                        ))
                    })?;
                match crate::atom::reseal_atom_json_if_plain(&key, &mut value) {
                    Ok(true) => {
                        let encoded = serde_json::to_vec(&value).map_err(|e| {
                            crate::schema::SchemaError::InvalidData(format!(
                                "reseal encode {storage_key}: {e}"
                            ))
                        })?;
                        if let Err(e) = self
                            .main_store
                            .inner()
                            .put(&storage_key_bytes, encoded)
                            .await
                        {
                            stats.errors += 1;
                            tracing::warn!(%storage_key, error = %e, "reseal put failed");
                            continue;
                        }
                        stats.resealed += 1;
                    }
                    Ok(false) => stats.already_sealed += 1,
                    Err(e) => {
                        stats.errors += 1;
                        tracing::warn!(%storage_key, error = %e, "reseal skipped");
                    }
                }
            }

            let Some(last) = last_key else {
                break;
            };
            // Next page starts strictly after `last` (inclusive start would re-read it).
            start = format!("{last}\0");
            if start.as_bytes() >= plane_end.as_bytes() {
                break;
            }

            if stats.scanned % 2000 == 0 {
                tracing::info!(
                    scanned = stats.scanned,
                    resealed = stats.resealed,
                    already = stats.already_sealed,
                    "reseal progress"
                );
                let _ = self.flush().await;
            }
        }

        self.flush()
            .await
            .map_err(|e| crate::schema::SchemaError::InvalidData(format!("reseal flush: {e}")))?;
        Ok(stats)
    }

    /// Rewrite one bounded page of legacy `ENC:` atom content through the
    /// compress-before-seal codec.
    ///
    /// `after_key` is an exclusive keyset cursor returned by a prior page. The
    /// storage range itself starts at that key (inclusive) and drops the cursor
    /// row, so resume work is proportional to the remaining suffix and does not
    /// depend on synthesizing a backend-specific successor key.
    ///
    /// Already-compressed `ENZ:` rows are idempotent skips. Legacy `ENC:` rows
    /// are only rewritten when the new envelope is actually `ENZ:`; small or
    /// incompressible content keeps its original ciphertext byte-for-byte.
    // lint:fn-size-ok verbatim move from atom_store/mod.rs; splitting this function is separate work
    pub async fn recompress_atom_content_page(
        &self,
        after_key: Option<&str>,
        max_atoms: usize,
    ) -> Result<RecompressAtomContentReport, crate::schema::SchemaError> {
        const MAX_PAGE: usize = 100_000;
        let Some(key) = self.content_key else {
            return Err(crate::schema::SchemaError::InvalidData(
                "recompress_atom_content_page requires atom content_key".into(),
            ));
        };
        if max_atoms == 0 {
            return Err(crate::schema::SchemaError::InvalidData(
                "recompress max_atoms must be at least 1".into(),
            ));
        }
        if max_atoms > MAX_PAGE {
            return Err(crate::schema::SchemaError::InvalidData(format!(
                "recompress max_atoms must not exceed {MAX_PAGE}"
            )));
        }

        let (plane_start, plane_end) =
            crate::kind_partition::colon_plane_bounds(crate::atom::atom_key_codec::ATOM_PREFIX);
        if let Some(after_key) = after_key {
            if crate::kind_partition::rest_of(after_key, "atom").is_none()
                || after_key.as_bytes() >= plane_end.as_bytes()
            {
                return Err(crate::schema::SchemaError::InvalidCursor(
                    "recompress after_key must be an atom storage key".into(),
                ));
            }
        }
        let start = after_key.unwrap_or(plane_start.as_str());
        // One extra row proves whether more work remains. A resumed page may
        // also contain its inclusive cursor row, so reserve a second slot.
        let raw_limit = max_atoms
            .saturating_add(1)
            .saturating_add(usize::from(after_key.is_some()));
        let raw = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), plane_end.as_bytes(), raw_limit)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("recompress atom range page: {e}"))
            })?;

        let mut rows = raw.into_iter().filter(|(storage_key, _)| {
            after_key.map(str::as_bytes) != Some(storage_key.as_slice())
        });
        let page: Vec<_> = rows.by_ref().take(max_atoms).collect();
        let more_remaining = rows.next().is_some();
        let mut report = RecompressAtomContentReport::default();
        let mut rewrites = Vec::new();

        for (storage_key_bytes, stored_bytes) in page {
            let storage_key = String::from_utf8_lossy(&storage_key_bytes).into_owned();
            if crate::kind_partition::rest_of(&storage_key, "atom").is_none() {
                continue;
            }
            report.scanned += 1;
            report.last_walked_key = Some(storage_key.clone());

            if stored_bytes.starts_with(crate::atom::ATOM_BINARY_ROW_PREFIX) {
                report.already_binary += 1;
                continue;
            }
            let mut value: serde_json::Value = match serde_json::from_slice(&stored_bytes) {
                Ok(value) => value,
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%storage_key, %error, "recompress decode skipped");
                    continue;
                }
            };

            let Some(stored) = value.get("content").and_then(serde_json::Value::as_str) else {
                report.not_legacy_enc += 1;
                continue;
            };
            let stored_len = stored.len() as u64;
            report.sealed_bytes_before += stored_len;

            if stored.starts_with(crate::crypto::AT_REST_ENC_DEFLATE_PREFIX) {
                report.already_compressed += 1;
                report.sealed_bytes_after += stored_len;
                continue;
            }
            if !stored.starts_with(crate::crypto::AT_REST_ENC_PREFIX) {
                report.not_legacy_enc += 1;
                report.sealed_bytes_after += stored_len;
                continue;
            }

            let opened = match crate::atom::open_content_value(
                &key,
                serde_json::Value::String(stored.to_owned()),
            ) {
                Ok(opened) => opened,
                Err(e) => {
                    report.errors += 1;
                    report.sealed_bytes_after += stored_len;
                    tracing::warn!(%storage_key, error = %e, "recompress open skipped");
                    continue;
                }
            };
            let resealed = match crate::atom::seal_content_value(&key, &opened) {
                Ok(resealed) => resealed,
                Err(e) => {
                    report.errors += 1;
                    report.sealed_bytes_after += stored_len;
                    tracing::warn!(%storage_key, error = %e, "recompress seal skipped");
                    continue;
                }
            };
            let Some(resealed_str) = resealed.as_str() else {
                report.errors += 1;
                report.sealed_bytes_after += stored_len;
                tracing::warn!(%storage_key, "recompress seal returned non-string content");
                continue;
            };
            if !resealed_str.starts_with(crate::crypto::AT_REST_ENC_DEFLATE_PREFIX) {
                // Do not churn AES-GCM nonces when compression does not help.
                report.compression_not_helpful += 1;
                report.sealed_bytes_after += stored_len;
                continue;
            }

            let new_len = resealed_str.len() as u64;
            report.sealed_bytes_after += new_len;
            value
                .as_object_mut()
                .expect("content lookup proved atom JSON is an object")
                .insert("content".to_string(), resealed);
            let encoded = serde_json::to_vec(&value).map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "encode recompressed atom {storage_key}: {e}"
                ))
            })?;
            rewrites.push((storage_key_bytes, encoded));
        }

        report.recompressed = rewrites.len();
        if !rewrites.is_empty() {
            self.main_store
                .inner()
                .batch_put(rewrites)
                .await
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "write recompressed atom page: {e}"
                    ))
                })?;
            self.flush().await.map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "flush recompressed atom page: {e}"
                ))
            })?;
        }
        report.bytes_saved = report
            .sealed_bytes_before
            .saturating_sub(report.sealed_bytes_after);
        report.more_remaining = more_remaining;
        report.next_after_key = more_remaining
            .then(|| report.last_walked_key.clone())
            .flatten();
        Ok(report)
    }

    /// Rewrite one bounded atom page from JSON-string content to the binary
    /// `ATB:` row container.
    ///
    /// The read side accepts both forms. This offline helper forces the new
    /// form independently of the live writer switch so a CoW home can measure
    /// the exact inner-container and logical-row reductions before rollout.
    // lint:fn-size-ok verbatim move from atom_store/mod.rs; splitting this function is separate work
    pub async fn repack_atom_content_binary_page(
        &self,
        after_key: Option<&str>,
        max_atoms: usize,
    ) -> Result<RepackAtomContentReport, crate::schema::SchemaError> {
        const MAX_PAGE: usize = 100_000;
        if self.content_key.is_none() && self.molecule_keys.is_none() {
            return Err(crate::schema::SchemaError::InvalidData(
                "binary atom repack requires an atom content key".into(),
            ));
        }
        if max_atoms == 0 || max_atoms > MAX_PAGE {
            return Err(crate::schema::SchemaError::InvalidData(format!(
                "binary atom repack max_atoms must be in 1..={MAX_PAGE}"
            )));
        }

        let (plane_start, plane_end) =
            crate::kind_partition::colon_plane_bounds(crate::atom::atom_key_codec::ATOM_PREFIX);
        if let Some(after_key) = after_key {
            if crate::kind_partition::rest_of(after_key, "atom").is_none()
                || after_key.as_bytes() >= plane_end.as_bytes()
            {
                return Err(crate::schema::SchemaError::InvalidCursor(
                    "binary atom repack after_key must be an atom storage key".into(),
                ));
            }
        }
        let start = after_key.unwrap_or(plane_start.as_str());
        let raw_limit = max_atoms
            .saturating_add(1)
            .saturating_add(usize::from(after_key.is_some()));
        let raw = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), plane_end.as_bytes(), raw_limit)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "binary atom repack range page: {e}"
                ))
            })?;
        let mut rows = raw.into_iter().filter(|(storage_key, _)| {
            after_key.map(str::as_bytes) != Some(storage_key.as_slice())
        });
        let page: Vec<_> = rows.by_ref().take(max_atoms).collect();
        let more_remaining = rows.next().is_some();
        let mut report = RepackAtomContentReport::default();
        let mut rewrites = Vec::new();

        for (storage_key, stored) in page {
            report.scanned += 1;
            report.last_walked_key = String::from_utf8(storage_key.clone()).ok();
            if stored.starts_with(crate::atom::ATOM_BINARY_ROW_PREFIX) {
                report.already_binary += 1;
                continue;
            }
            let raw_value: serde_json::Value = match serde_json::from_slice(&stored) {
                Ok(value) => value,
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%error, "binary atom repack JSON decode skipped");
                    continue;
                }
            };
            let Some(content) = raw_value.get("content") else {
                report.errors += 1;
                continue;
            };
            let inner_before = serde_json::to_vec(content).map_or(0, |bytes| bytes.len() as u64);
            let molecule_uuid = raw_value
                .get("molecule_key_bundle")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let atom = match self.decode_atom_bytes(&stored).await {
                Ok(atom) => atom,
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%error, "binary atom repack open skipped");
                    continue;
                }
            };
            let encoded = match self
                .encode_atom_bytes_with_binary(&atom, molecule_uuid.as_deref(), true)
                .await
            {
                Ok(encoded) => encoded,
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%error, "binary atom repack seal skipped");
                    continue;
                }
            };
            let inner_after = crate::atom::parse_atom_binary_row(&encoded)
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "binary atom repack encoded row invalid: {e}"
                    ))
                })?
                .map_or(0, |(_, sealed)| sealed.len() as u64);
            report.inner_bytes_before += inner_before;
            report.inner_bytes_after += inner_after;
            report.row_bytes_before += stored.len() as u64;
            report.row_bytes_after += encoded.len() as u64;
            rewrites.push((storage_key, encoded));
        }

        report.repacked = rewrites.len();
        if !rewrites.is_empty() {
            self.main_store
                .inner()
                .batch_put(rewrites)
                .await
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "write binary atom repack page: {e}"
                    ))
                })?;
            self.flush().await.map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "flush binary atom repack page: {e}"
                ))
            })?;
        }
        report.inner_bytes_saved = report
            .inner_bytes_before
            .saturating_sub(report.inner_bytes_after);
        report.row_bytes_saved = report
            .row_bytes_before
            .saturating_sub(report.row_bytes_after);
        report.more_remaining = more_remaining;
        report.next_after_key = more_remaining
            .then(|| report.last_walked_key.clone())
            .flatten();
        Ok(report)
    }
}
