use super::*;
// lint:file-size-ok moved verbatim from add_schema.rs

impl SchemaServiceState {
    /// Read-only classifier — would the incoming schema round-trip as a
    /// pure [`SchemaAddOutcome::AlreadyExists`] against the current registry?
    /// Returns `Some(active_schema)` only when yes. The handler uses this to
    /// allow cert-free idempotent re-POSTs of an already-published namespaced
    /// schema (the `fbrain init` onboarding path that PR #5xx fixes); any
    /// outcome that would create, expand, or conflict still has to run
    /// through the cert gate.
    ///
    /// Mirrors the minimum prefix of [`Self::add_schema_inner`] needed to
    /// compute the identity hash — canonicalize fields, dedup, hash, then
    /// look the slot up under a read lock and verify it isn't a
    /// cross-`schema_type` collision and that the incoming fields are a
    /// subset of the existing ones. Nothing about the registry is mutated.
    ///
    /// On any structural mismatch (bad descriptive_name, blank field,
    /// reserved-prefix collision, owner_app_id namespace mismatch, …)
    /// returns `None`. The handler then runs the cert gate + full
    /// `add_schema`, which will produce the proper error response.
    ///
    /// Safety: the canonical hash of every registered schema is already
    /// publicly readable via unauthenticated `GET /v1/schemas`, so returning
    /// the existing body without a cert leaks nothing beyond what
    /// enumeration already exposes. The cert gate continues to protect
    /// `Added`/`Expanded`/`DescriptiveNameConflict` — i.e. every state
    /// transition.
    pub fn classify_idempotent_repost(
        &self,
        schema: &Schema,
        mutation_mappers: &HashMap<String, String>,
    ) -> Option<Schema> {
        self.classify_idempotent_repost_with_descriptive_name(schema, mutation_mappers, None)
    }

    pub(super) fn classify_idempotent_repost_with_descriptive_name(
        &self,
        schema: &Schema,
        mutation_mappers: &HashMap<String, String>,
        descriptive_name_override: Option<&str>,
    ) -> Option<Schema> {
        // lint:fn-size-ok moved verbatim from the original impl; splitting is a separate change
        let mut probe = schema.clone();
        if let Some(desc_name) = descriptive_name_override {
            probe.descriptive_name = Some(desc_name.to_string());
            probe.identity_hash = None;
        }
        let incoming_owner = probe.owner_app_id.clone().unwrap_or_default();
        let incoming_dn = probe.descriptive_name.clone().unwrap_or_default();

        // Defensive subset of `add_schema_inner`'s up-front validation.
        // Any failure here means the full pipeline would error — fall
        // through so the caller (cert gate + `add_schema`) produces the
        // proper error response rather than silently no-op-ing.
        if probe
            .descriptive_name
            .as_ref()
            .is_none_or(|dn| dn.trim().is_empty())
        {
            tracing::debug!(
                target: "schema_service::schema",
                owner_app_id = %incoming_owner,
                reason = "descriptive_name_missing",
                "classify_idempotent_repost: miss",
            );
            return None;
        }
        if probe
            .owner_app_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .is_none()
            && probe
                .descriptive_name
                .as_deref()
                .is_some_and(|dn| dn.starts_with("app:"))
        {
            tracing::debug!(
                target: "schema_service::schema",
                descriptive_name = %incoming_dn,
                reason = "app_prefix_without_owner",
                "classify_idempotent_repost: miss",
            );
            return None;
        }
        if let Some(ref fields) = probe.fields {
            if fields
                .iter()
                .any(|f| f.trim().is_empty() || f.contains(',') || f.contains(':'))
            {
                tracing::debug!(
                    target: "schema_service::schema",
                    owner_app_id = %incoming_owner,
                    descriptive_name = %incoming_dn,
                    reason = "field_name_invalid",
                    "classify_idempotent_repost: miss",
                );
                return None;
            }
        }

        // Apply the same canonicalization the writer would so the
        // computed identity hash matches the registry's slot. `canonicalize_fields`
        // is read-only against `self` (mutates only the local rename
        // map / mutation_mappers clone).
        if let Some(fields) = probe.fields.clone() {
            let mut mappers = mutation_mappers.clone();
            let rename_map = self.canonicalize_fields(&fields, &probe, &mut mappers);
            if !rename_map.is_empty() {
                Self::apply_field_renames(&mut probe, &rename_map, &mut mappers);
                probe.identity_hash = None;
            }
        }
        probe.dedup_fields();
        probe.compute_identity_hash();
        let identity_hash = probe.get_identity_hash()?.clone();

        let schemas = read_lock(&self.schemas, "schemas").ok()?;
        let (active, matched_by_name) = if let Some(existing) = schemas.get(&identity_hash) {
            let (active, _active_name) = self
                .resolve_active_schema(existing, &identity_hash, &schemas)
                .unwrap_or_else(|| (existing.clone(), identity_hash.clone()));
            (active, false)
        } else {
            // No identity slot. The writer would still answer
            // `AlreadyExists` when the exact `(owner_app_id, descriptive_name)`
            // row already holds every incoming field (`expand_schema`'s
            // subset branch), so classify that shape as a repost too.
            // Otherwise a fresh node re-declaring a catalog schema whose row
            // grew a field pays the mutation-gate quota for a no-op add
            // (2026-09-13: `brain init` spent the whole 10/hour node quota
            // this way and `kanban init` then 409'd).
            let by_name = self
                .lookup_descriptive_name_in_namespace(
                    probe.owner_app_id.as_deref(),
                    probe.descriptive_name.as_deref().unwrap_or_default(),
                )
                .ok()
                .flatten()
                .and_then(|hash| {
                    schemas.get(&hash).map(|existing| {
                        self.resolve_active_schema(existing, &hash, &schemas)
                            .map_or_else(|| existing.clone(), |(active, _)| active)
                    })
                });
            let Some(active) = by_name else {
                // Normal create path — the schema isn't published yet. Logged
                // at debug for forensic CW Logs Insights queries when a
                // re-POST that *should* have hit didn't (caller can pivot
                // on the computed identity_hash).
                tracing::debug!(
                    target: "schema_service::schema",
                    owner_app_id = %incoming_owner,
                    descriptive_name = %incoming_dn,
                    identity_hash = %identity_hash,
                    reason = "no_existing_slot",
                    "classify_idempotent_repost: miss",
                );
                return None;
            };
            if crate::state_expansion::is_cross_key_layout(&probe, &active) {
                // The writer never subset-reuses across key layouts; it
                // registers a sibling instead, which is a state transition.
                tracing::info!(
                    target: "schema_service::schema",
                    owner_app_id = %incoming_owner,
                    descriptive_name = %incoming_dn,
                    identity_hash = %identity_hash,
                    existing_identity_hash = %active.name,
                    reason = "cross_key_layout",
                    "classify_idempotent_repost: miss",
                );
                return None;
            }
            (active, true)
        };

        // identity_hash includes `owner_app_id`, so a mismatch here would
        // indicate a hash-input bug. Bail out conservatively rather than
        // echoing a cross-namespace schema to an un-certed caller.
        fn norm(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        if norm(active.owner_app_id.as_deref()) != norm(probe.owner_app_id.as_deref()) {
            tracing::info!(
                target: "schema_service::schema",
                owner_app_id = %incoming_owner,
                existing_owner_app_id = %active.owner_app_id.as_deref().unwrap_or(""),
                identity_hash = %identity_hash,
                reason = "owner_app_id_mismatch",
                "classify_idempotent_repost: miss",
            );
            return None;
        }

        if active.descriptive_name.as_deref() != probe.descriptive_name.as_deref() {
            tracing::info!(
                target: "schema_service::schema",
                owner_app_id = %incoming_owner,
                descriptive_name = %incoming_dn,
                existing_descriptive_name = %active.descriptive_name.as_deref().unwrap_or(""),
                identity_hash = %identity_hash,
                reason = "descriptive_name_mismatch",
                "classify_idempotent_repost: miss",
            );
            return None;
        }

        // Same `schema_type` is part of the writer's AlreadyExists branch
        // (the hash itself does not bind `schema_type`). A cross-type
        // collision would resolve to 409, not 200, and must keep its cert
        // gate.
        if crate::state_expansion::is_cross_schema_type_expansion(&probe, &active) {
            tracing::info!(
                target: "schema_service::schema",
                owner_app_id = %incoming_owner,
                descriptive_name = %incoming_dn,
                identity_hash = %identity_hash,
                reason = "cross_schema_type",
                "classify_idempotent_repost: miss",
            );
            return None;
        }

        // Incoming fields ⊆ existing fields, otherwise the writer would
        // Expand (state-mutating) and the cert gate must apply.
        let existing_fields: HashSet<String> = active
            .fields
            .as_ref()
            .map(|f| f.iter().cloned().collect())
            .unwrap_or_default();
        let incoming_fields: HashSet<String> = probe
            .fields
            .as_ref()
            .map(|f| f.iter().cloned().collect())
            .unwrap_or_default();
        if !incoming_fields.is_subset(&existing_fields) {
            tracing::info!(
                target: "schema_service::schema",
                owner_app_id = %incoming_owner,
                descriptive_name = %incoming_dn,
                identity_hash = %identity_hash,
                reason = "incoming_fields_not_subset",
                "classify_idempotent_repost: miss",
            );
            return None;
        }

        tracing::info!(
            target: "schema_service::schema",
            owner_app_id = %incoming_owner,
            descriptive_name = %incoming_dn,
            identity_hash = %identity_hash,
            existing_identity_hash = %active.name,
            matched_by = if matched_by_name { "owner_name_subset" } else { "identity_hash" },
            "classify_idempotent_repost: hit",
        );
        Some(active)
    }
}
