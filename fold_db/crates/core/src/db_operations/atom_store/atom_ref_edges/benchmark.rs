use super::*;

impl AtomStore {
    /// Compare paired v1 and v2 lookups on a bounded real-data sample.
    ///
    /// This proof helper scans only the isolated copy's compact plane. Product
    /// requests never call it. Alternating the lookup order limits cache bias.
    #[cfg(feature = "cloud-sync")]
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub async fn benchmark_atom_ref_v2_lookups_on_isolated_copy(
        &self,
        storage_prefix: Option<&str>,
        sample_limit: usize,
    ) -> Result<AtomRefV2LookupBenchmark, SchemaError> {
        let sample_limit = sample_limit.max(1);
        let row_limit = sample_limit.saturating_mul(64).min(1_000_000);
        let edge_prefix = build_storage_key(storage_prefix, ATOM_REF_V2_PREFIX);
        let edge_end = FilterUtils::create_prefix_end(&edge_prefix);
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(edge_prefix.as_bytes(), edge_end.as_bytes(), row_limit)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "sample compact atom reverse edges for lookup proof: {error}"
                ))
            })?;
        let mut active_atoms = BTreeSet::new();
        for (key, value) in rows {
            if is_atom_live_ref_count_key(&key) {
                continue;
            }
            if value.as_slice() != ATOM_REF_V2_ACTIVE_MARKER {
                return Err(SchemaError::InvalidData(
                    "compact lookup sample contains an invalid marker".to_string(),
                ));
            }
            let full_key = String::from_utf8_lossy(&key);
            let base_key = strip_storage_prefix(storage_prefix, &full_key).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "compact lookup sample has the wrong storage prefix: {full_key}"
                ))
            })?;
            let partition = base_key.split_once('\0').map_or(base_key, |(key, _)| key);
            let token = partition.strip_prefix(ATOM_REF_V2_PREFIX).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "compact lookup sample has an invalid key: {full_key}"
                ))
            })?;
            let digest = URL_SAFE_NO_PAD.decode(token).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "decode compact lookup sample atom token: {error}"
                ))
            })?;
            if digest.len() != 32 {
                return Err(SchemaError::InvalidData(format!(
                    "compact lookup sample atom token has {} bytes, expected 32",
                    digest.len()
                )));
            }
            let atom = hex_lower(&digest);
            active_atoms.insert(atom);
            if active_atoms.len() == sample_limit {
                break;
            }
        }
        if active_atoms.is_empty() {
            return Err(SchemaError::InvalidData(
                "compact lookup proof found no active atoms".to_string(),
            ));
        }

        let mut v1_ns = Vec::with_capacity(active_atoms.len() * 2);
        let mut v2_ns = Vec::with_capacity(active_atoms.len() * 2);
        for (index, atom) in active_atoms.iter().enumerate() {
            self.measure_atom_ref_lookup_pair(
                atom,
                storage_prefix,
                true,
                index % 2 == 1,
                &mut v1_ns,
                &mut v2_ns,
            )
            .await?;

            let missing_atom = sha256_hex(format!("lastdb:aref:v2:missing:{index}"));
            self.measure_atom_ref_lookup_pair(
                &missing_atom,
                storage_prefix,
                false,
                index % 2 == 0,
                &mut v1_ns,
                &mut v2_ns,
            )
            .await?;
        }

        let v1_p50_ns = percentile_ns(&mut v1_ns, 50);
        let v1_p95_ns = percentile_ns(&mut v1_ns, 95);
        let v2_p50_ns = percentile_ns(&mut v2_ns, 50);
        let v2_p95_ns = percentile_ns(&mut v2_ns, 95);
        Ok(AtomRefV2LookupBenchmark {
            active_samples: active_atoms.len() as u64,
            missing_samples: active_atoms.len() as u64,
            v1_p50_ns,
            v1_p95_ns,
            v2_p50_ns,
            v2_p95_ns,
            v2_to_v1_p95_basis_points: v2_p95_ns
                .saturating_mul(10_000)
                .checked_div(v1_p95_ns.max(1))
                .unwrap_or(u64::MAX),
        })
    }

    #[cfg(feature = "cloud-sync")]
    pub(super) async fn measure_atom_ref_lookup_pair(
        &self,
        atom_content_sha256: &str,
        storage_prefix: Option<&str>,
        expected_live: bool,
        v2_first: bool,
        v1_ns: &mut Vec<u64>,
        v2_ns: &mut Vec<u64>,
    ) -> Result<(), SchemaError> {
        let (v1_live, v1_elapsed, v2_live, v2_elapsed) = if v2_first {
            let (v2_live, v2_elapsed) = self
                .timed_atom_ref_v2_lookup(atom_content_sha256, storage_prefix)
                .await?;
            let (v1_live, v1_elapsed) = self
                .timed_atom_ref_v1_lookup(atom_content_sha256, storage_prefix)
                .await?;
            (v1_live, v1_elapsed, v2_live, v2_elapsed)
        } else {
            let (v1_live, v1_elapsed) = self
                .timed_atom_ref_v1_lookup(atom_content_sha256, storage_prefix)
                .await?;
            let (v2_live, v2_elapsed) = self
                .timed_atom_ref_v2_lookup(atom_content_sha256, storage_prefix)
                .await?;
            (v1_live, v1_elapsed, v2_live, v2_elapsed)
        };
        if v1_live != expected_live || v2_live != expected_live {
            return Err(SchemaError::InvalidData(format!(
                "paired atom lookup disagrees: expected_live={expected_live} v1={v1_live} v2={v2_live}"
            )));
        }
        v1_ns.push(v1_elapsed);
        v2_ns.push(v2_elapsed);
        Ok(())
    }

    #[cfg(feature = "cloud-sync")]
    pub(super) async fn timed_atom_ref_v1_lookup(
        &self,
        atom_content_sha256: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(bool, u64), SchemaError> {
        let started = std::time::Instant::now();
        let live = !self
            .active_atom_ref_edges_for_atom(atom_content_sha256, storage_prefix)
            .await?
            .is_empty();
        Ok((live, elapsed_nanos(started)))
    }

    #[cfg(feature = "cloud-sync")]
    pub(super) async fn timed_atom_ref_v2_lookup(
        &self,
        atom_content_sha256: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(bool, u64), SchemaError> {
        let started = std::time::Instant::now();
        let live = self
            .has_active_atom_refs(atom_content_sha256, storage_prefix)
            .await?;
        Ok((live, elapsed_nanos(started)))
    }
}
