//! Persisted proposal-to-catalog aliases.

use super::*;

pub(crate) const PROPOSAL_CATALOG_ALIAS_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProposalCatalogAlias {
    pub(crate) version: u8,
    pub(crate) owner_app_id: String,
    pub(crate) proposal_identity_hash: String,
    pub(crate) catalog_hash: String,
}

pub(crate) fn proposal_catalog_alias_key(
    owner_app_id: &str,
    proposal_identity_hash: &str,
) -> String {
    format!("schema_proposal_catalog_alias:v1:{owner_app_id}:{proposal_identity_hash}")
}

pub(crate) fn is_catalog_hash(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit())
}

pub(crate) async fn load_persisted_proposal_catalog_aliases<'a>(
    host: &Host,
    owner_app_id: Option<&str>,
    schemas: &HashMap<String, Schema>,
    candidates: impl IntoIterator<Item = &'a Schema>,
) -> Result<HashMap<String, String>, String> {
    let Some(owner_app_id) = owner_app_id.filter(|owner| !owner.is_empty()) else {
        return Ok(HashMap::new());
    };
    let mut sources = HashSet::new();
    for schema in candidates {
        if let Some(mappers) = schema.field_mappers.as_ref() {
            sources.extend(
                mappers
                    .values()
                    .map(FieldMapper::source_schema)
                    .filter(|source| !schemas.contains_key(*source))
                    .map(str::to_string),
            );
        }
    }

    let mut aliases = HashMap::new();
    for proposal_identity_hash in sources {
        let key = proposal_catalog_alias_key(owner_app_id, &proposal_identity_hash);
        let Some(alias) = host
            .db
            .db_ops()
            .metadata()
            .get_typed::<ProposalCatalogAlias>(&key)
            .await
            .map_err(|e| format!("failed to read proposal catalog alias {key}: {e}"))?
        else {
            continue;
        };
        if alias.version != PROPOSAL_CATALOG_ALIAS_VERSION
            || alias.owner_app_id != owner_app_id
            || alias.proposal_identity_hash != proposal_identity_hash
            || !is_catalog_hash(&alias.catalog_hash)
        {
            return Err(format!("invalid proposal catalog alias at {key}"));
        }
        aliases.insert(proposal_identity_hash, alias.catalog_hash);
    }
    Ok(aliases)
}

pub(crate) fn recover_proposal_catalog_aliases_from_exact_target(
    schema: &DeclarativeSchemaDefinition,
    schemas: &HashMap<String, Schema>,
    aliases: &mut HashMap<String, String>,
) -> Result<(), String> {
    let Some(loaded_target) = schemas.get(&schema.name) else {
        return Ok(());
    };
    if loaded_target.owner_app_id != schema.owner_app_id
        || loaded_target.descriptive_name != schema.descriptive_name
    {
        return Err(format!(
            "loaded catalog target {} does not match its exact catalog envelope",
            schema.name
        ));
    }
    let Some(remote_mappers) = schema.field_mappers.as_ref() else {
        return Ok(());
    };
    let Some(loaded_mappers) = loaded_target.field_mappers.as_ref() else {
        return Ok(());
    };
    for (field, remote) in remote_mappers {
        if schemas.contains_key(remote.source_schema()) {
            continue;
        }
        let Some(loaded) = loaded_mappers.get(field) else {
            continue;
        };
        if loaded.source_field() != remote.source_field()
            || !schemas.contains_key(loaded.source_schema())
            || !is_catalog_hash(loaded.source_schema())
        {
            continue;
        }
        if let Some(existing) = aliases.insert(
            remote.source_schema().to_string(),
            loaded.source_schema().to_string(),
        ) {
            if existing != loaded.source_schema() {
                return Err(format!(
                    "proposal {} maps to both {existing} and {} for exact catalog target {}",
                    remote.source_schema(),
                    loaded.source_schema(),
                    schema.name
                ));
            }
        }
    }
    Ok(())
}

/// Read-only half of [`persist_proposal_catalog_alias`]. `Ok(true)` means the
/// alias row is absent and a sync would write it; `Ok(false)` means there is
/// nothing to write; an error is a conflict a sync would also refuse.
pub(crate) async fn check_proposal_catalog_alias(
    host: &Host,
    owner_app_id: &str,
    proposal_identity_hash: &str,
    catalog_hash: &str,
) -> Result<bool, String> {
    if proposal_identity_hash == catalog_hash {
        return Ok(false);
    }
    if !is_catalog_hash(proposal_identity_hash) || !is_catalog_hash(catalog_hash) {
        return Err("proposal and catalog aliases must use 64-character hashes".into());
    }
    let key = proposal_catalog_alias_key(owner_app_id, proposal_identity_hash);
    if let Some(existing) = host
        .db
        .db_ops()
        .metadata()
        .get_typed::<ProposalCatalogAlias>(&key)
        .await
        .map_err(|e| format!("failed to inspect proposal catalog alias {key}: {e}"))?
    {
        if existing.owner_app_id != owner_app_id
            || existing.proposal_identity_hash != proposal_identity_hash
            || existing.catalog_hash != catalog_hash
        {
            return Err(format!(
                "proposal {proposal_identity_hash} is already bound to catalog {} for app {}",
                existing.catalog_hash, existing.owner_app_id
            ));
        }
        return Ok(false);
    }
    Ok(true)
}

pub(crate) async fn persist_proposal_catalog_alias(
    host: &Host,
    owner_app_id: &str,
    proposal_identity_hash: &str,
    catalog_hash: &str,
) -> Result<(), String> {
    if !check_proposal_catalog_alias(host, owner_app_id, proposal_identity_hash, catalog_hash)
        .await?
    {
        return Ok(());
    }
    let key = proposal_catalog_alias_key(owner_app_id, proposal_identity_hash);
    host.db
        .db_ops()
        .metadata()
        .put_typed_durable(
            &key,
            &ProposalCatalogAlias {
                version: PROPOSAL_CATALOG_ALIAS_VERSION,
                owner_app_id: owner_app_id.to_string(),
                proposal_identity_hash: proposal_identity_hash.to_string(),
                catalog_hash: catalog_hash.to_string(),
            },
        )
        .await
        .map_err(|e| format!("failed to persist proposal catalog alias {key}: {e}"))
}
