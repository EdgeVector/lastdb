use super::*;

impl SchemaServiceState {
    /// Exact `identity_hash` row in the active registry, if any.
    pub(crate) fn exact_identity_reuse_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let Some(identity_hash) = proposal
            .identity_hash
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let Some(existing) = schemas.get(identity_hash) else {
            return Ok(None);
        };
        let (active_schema, active_name) =
            match self.resolve_active_schema(existing, identity_hash, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), identity_hash.to_string()),
            };
        let matched_desc = active_schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| active_name.clone());
        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            matched_desc,
            true,
        )))
    }

    /// Exact `(owner_app_id, descriptive_name)` row through the namespaced
    /// `descriptive_name_index`, if any. No semantic name matching here.
    pub(crate) fn exact_owner_name_reuse_match(
        &self,
        proposal: &SchemaResolveProposal,
    ) -> FoldDbResult<Option<SchemaReuseMatch>> {
        let owner_app_id = proposal
            .owner_app_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let desc_name = proposal.descriptive_name.trim();
        if desc_name.is_empty() {
            return Ok(None);
        }
        let Some(matched_hash) =
            self.lookup_descriptive_name_in_namespace(owner_app_id, desc_name)?
        else {
            return Ok(None);
        };
        let schemas = read_lock(&self.schemas, "schemas_cache")?;
        let Some(existing) = schemas.get(&matched_hash) else {
            return Ok(None);
        };
        let (active_schema, _active_name) =
            match self.resolve_active_schema(existing, &matched_hash, &schemas) {
                Some(pair) => pair,
                None => (existing.clone(), matched_hash.clone()),
            };
        Ok(Some(self.schema_reuse_match_from_proposal(
            proposal,
            active_schema,
            desc_name.to_string(),
            true,
        )))
    }
}
