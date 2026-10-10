//! Complete current schema/protein ownership; ownership never removes a hold.

use super::*;
use fold_db::atom::deterministic_molecule_uuid;
use fold_db::db_operations::SchemaStore;
use fold_db::protein::{protein_fold_job_key, Protein, ProteinFoldJob};
use fold_db::record_molecule::{is_record_molecule_field, record_molecule_uuid};
use fold_db::schema::Schema;
use serde_json::Value;

fn molecule_id(molecule: &str) -> String {
    fold_db::hex::hex_lower(crate::reap::keys::mol_key(molecule))
}

pub(super) async fn schemas(
    home: &Path,
    opened: &HomeStore,
    facts: &mut Facts,
) -> Result<(), String> {
    let (keys, _) = lastdb_node::offline_home::load_e2e_keys(home)?;
    let store = SchemaStore::open_for_offline_read(&*opened.store, Some(keys.encryption_key()))
        .await
        .map_err(err)?;
    let schemas = store.get_all_schemas_strict().await.map_err(err)?;
    let raw = opened.base.open_namespace("schemas").await.map_err(err)?;
    let mut walker = crate::reap::walk::Walker::new(raw);
    let mut physical = Vec::new();
    while let Some(page) = walker.next_page().await.map_err(err)? {
        physical.extend(page.rows.into_iter().map(|row| row.0));
    }
    crate::reap::catalog::check_complete(&physical, &schemas).map_err(err)?;
    for schema in schemas.values() {
        schema_owners(schema, facts);
    }
    *facts.counts.entry("current_schemas".into()).or_default() = schemas.len() as u64;
    Ok(())
}

fn schema_owners(schema: &Schema, facts: &mut Facts) {
    let mut ids = Vec::new();
    for field in schema.runtime_fields.values() {
        ids.extend(field.common().molecule_uuid().cloned());
    }
    ids.extend(
        schema
            .field_molecule_uuids
            .iter()
            .flat_map(|m| m.values())
            .cloned(),
    );
    ids.extend(schema.molecule_uuid.iter().cloned());
    ids.push(record_molecule_uuid(&schema.name));
    let fields = schema
        .fields
        .iter()
        .flatten()
        .cloned()
        .chain(
            schema
                .transform_fields
                .iter()
                .flat_map(|m| m.keys().cloned()),
        )
        .chain(
            schema
                .runtime_fields
                .keys()
                .filter(|name| !is_record_molecule_field(name))
                .cloned(),
        )
        .collect::<BTreeSet<String>>();
    ids.extend(
        fields
            .into_iter()
            .map(|field| deterministic_molecule_uuid(&schema.name, &field)),
    );
    for id in ids {
        facts
            .molecule_owners
            .entry(molecule_id(&id))
            .or_default()
            .insert(schema.name.clone());
    }
}

pub(super) fn join(facts: &mut Facts) {
    for (uuid, holds) in &facts.holds {
        for hold in holds {
            let Some(molecule) = &hold.molecule else {
                continue;
            };
            let molecule = molecule_id(molecule);
            if let Some(owners) = facts.molecule_owners.get(&molecule) {
                facts
                    .retained_schema_owners
                    .entry(uuid.clone())
                    .or_default()
                    .extend(owners.iter().cloned());
            }
            if let Some(owners) = facts.molecule_owners.get(&molecule) {
                for (identity, entry) in &facts.database_catalog {
                    if owners.contains(&entry.schema_name)
                        && entry.instance_id.as_deref().unwrap_or_default() == hold.scope
                    {
                        facts
                            .retained_database_paths
                            .entry(uuid.clone())
                            .or_default()
                            .insert(identity.clone());
                    }
                }
                for (org, route) in &facts.org_routes {
                    let scoped =
                        !hold.scope.is_empty() && route.storage_prefixes.contains(&hold.scope);
                    let unprefixed = hold.scope.is_empty()
                        && route
                            .storage_prefixes
                            .contains(fold_db::db_operations::UNPREFIXED_INSTANCE_ID)
                        && (route.legacy_all_unprefixed
                            || !owners.is_disjoint(&route.unprefixed_schema_names));
                    if route.active && (scoped || unprefixed) {
                        facts
                            .retained_org_targets
                            .entry(uuid.clone())
                            .or_default()
                            .insert(org.clone());
                    }
                }
            }
            for (protein, members) in &facts.proteins {
                if members.contains(&molecule) {
                    facts
                        .retained_proteins
                        .entry(uuid.clone())
                        .or_default()
                        .insert(protein.clone());
                }
            }
        }
    }
}

pub(super) fn row(
    collection: &str,
    key: &[u8],
    scope: &str,
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    if let Some(id) = bare.strip_prefix("pfq:") {
        let job: ProteinFoldJob = serde_json::from_slice(plain).map_err(err)?;
        if job.job_id != id || bare != protein_fold_job_key(&job.job_id) || job.atom_uuid.is_empty()
        {
            return Err("protein job key differs from its value".into());
        }
        facts.count("pending_protein_jobs");
        facts.hold(
            targets,
            &job.atom_uuid,
            model::hold(
                collection,
                key,
                scope,
                "pending_protein_job",
                Some(&job.entry_mol),
            ),
        );
        return Ok(());
    }
    if let Some(id) = bare.strip_prefix("protein:") {
        let protein: Protein = serde_json::from_slice(plain).map_err(err)?;
        if protein.uuid != id
            || protein
                .members
                .iter()
                .any(|member| member.molecule_uuid.is_empty())
        {
            return Err("protein key differs from its value".into());
        }
        facts.count("protein_rows");
        let identity = format!("{scope}\0{}", protein.uuid);
        facts.proteins.entry(identity).or_default().extend(
            protein
                .members
                .into_iter()
                .map(|member| molecule_id(&member.molecule_uuid)),
        );
        return Ok(());
    }
    if bare.starts_with("molprot:") || bare.starts_with("fldprot:") {
        let value: String = serde_json::from_slice(plain).map_err(err)?;
        if value.is_empty() {
            return Err("protein backref is empty".into());
        }
        facts.count("protein_backrefs");
        return Ok(());
    }
    routing_row(collection, key, scope, bare, plain, targets, facts)
}

fn routing_row(
    collection: &str,
    key: &[u8],
    scope: &str,
    bare: &str,
    plain: &[u8],
    targets: &BTreeSet<String>,
    facts: &mut Facts,
) -> Result<(), String> {
    if collection == "org_sync_targets" {
        return org_route(key, plain, facts);
    }
    if collection == "share_delivery_outbox" {
        share_outbox(bare, plain)?;
        facts.count("unresolved_share_outbox_rows");
        facts.hold_all(
            targets,
            model::hold(collection, key, scope, "unresolved_share_delivery", None),
        );
        return Ok(());
    }
    if collection == "db_catalog" {
        return db_catalog(key, plain, facts);
    }
    if bare.starts_with("ref:") {
        let _: Value = sources::json(plain)?;
        facts.count("legacy_molecule_blobs");
        facts.hold_all(
            targets,
            model::hold(
                collection,
                key,
                scope,
                "unresolved_legacy_molecule_blob",
                bare.strip_prefix("ref:"),
            ),
        );
        return Ok(());
    }
    auxiliary::metadata(collection, key, scope, bare, plain, targets, facts)
}

fn org_route(key: &[u8], plain: &[u8], facts: &mut Facts) -> Result<(), String> {
    use base64::Engine as _;
    let route: fold_db::sharing::OrgSyncTarget =
        serde_json::from_slice(plain).map_err(|_| "org registry record does not decode")?;
    if key != format!("org_sync:{}", route.org_hash).as_bytes() || !valid_hash(&route.org_hash) {
        return Err("org route key differs from its value".into());
    }
    let secret = base64::engine::general_purpose::STANDARD
        .decode(route.e2e_key_b64.trim())
        .map_err(|_| "org route has an invalid encryption key")?;
    if secret.len() != 32 {
        return Err("org route has an invalid encryption key length".into());
    }
    if route
        .storage_prefixes
        .iter()
        .any(|scope| !valid_hash(scope) && scope != fold_db::db_operations::UNPREFIXED_INSTANCE_ID)
        || route
            .unprefixed_schema_names
            .iter()
            .any(|name| name.trim().is_empty())
    {
        return Err("org route has an unsupported storage/schema selection".into());
    }
    chrono::DateTime::parse_from_rfc3339(&route.registered_at).map_err(err)?;
    let prefixes = if route.storage_prefixes.is_empty() {
        BTreeSet::from([route.org_hash.clone()])
    } else {
        route.storage_prefixes.into_iter().collect()
    };
    let legacy_all_unprefixed = prefixes.contains(fold_db::db_operations::UNPREFIXED_INSTANCE_ID)
        && route.unprefixed_schema_names.is_empty();
    let value = model::OrgRoute {
        active: route.active,
        storage_prefixes: prefixes,
        unprefixed_schema_names: route.unprefixed_schema_names.into_iter().collect(),
        legacy_all_unprefixed,
    };
    if facts.org_routes.insert(route.org_hash, value).is_some() {
        return Err("duplicate org route".into());
    }
    facts.count("org_routes");
    Ok(())
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn share_outbox(key: &str, plain: &[u8]) -> Result<(), String> {
    if let Some(id) = key.strip_prefix("delivery:") {
        let delivery: fold_db::sharing::types::PendingDelivery =
            serde_json::from_slice(plain).map_err(err)?;
        if id != delivery.delivery_id {
            return Err("share delivery key differs from its value".into());
        }
    } else if key.starts_with("delivery_artifact:") {
        let _: fold_db::sharing::types::StagedDeliveryArtifact =
            serde_json::from_slice(plain).map_err(|_| "share delivery artifact does not decode")?;
    } else {
        return Err("unsupported share delivery root".into());
    }
    Ok(())
}

fn db_catalog(key: &[u8], plain: &[u8], facts: &mut Facts) -> Result<(), String> {
    let entry: fold_db::db_operations::DbCatalogEntry =
        serde_json::from_slice(plain).map_err(err)?;
    let mut hash = Sha256::new();
    hash.update(entry.db_locator.as_bytes());
    hash.update([0]);
    hash.update(entry.schema_name.as_bytes());
    let expected = format!("v1:{}", fold_db::hex::hex_lower(hash.finalize()));
    if key != expected.as_bytes()
        || entry.db_locator.trim().is_empty()
        || entry.schema_name.trim().is_empty()
        || entry
            .instance_id
            .as_ref()
            .is_some_and(|scope| scope.trim().is_empty())
    {
        return Err("database catalog key differs from its value".into());
    }
    if facts.database_catalog.insert(expected, entry).is_some() {
        return Err("duplicate database catalog membership".into());
    }
    facts.count("database_catalog_rows");
    Ok(())
}

/// Complete root absence and current source explanations are separate facts.
/// A derived edge, a pending hold, or another-scope body alone is not a live owner.
pub(super) fn explanation(facts: &mut Facts) {
    for (uuid, holds) in &facts.holds {
        if holds.iter().any(|hold| {
            matches!(
                hold.kind.as_str(),
                "current_or_shadow_tip"
                    | "authoritative_version"
                    | "version_backref"
                    | "history_new_old_or_loser"
                    | "conflict_winner_or_loser"
                    | "historical_lineage"
                    | "displaced_delete_history"
                    | "pending_protein_job"
                    | "org_or_unconfirmed_cloud_intent"
                    | "org_or_unconfirmed_cloud_body"
            )
        }) {
            facts.source_explained_target_ids.insert(uuid.clone());
        } else {
            facts.reference_only_target_ids.insert(uuid.clone());
        }
    }
    facts.retained_source_references_complete =
        facts.global_holds.is_empty() && facts.reference_only_target_ids.is_empty();
    facts.counts.insert(
        "source_explained_targets".into(),
        facts.source_explained_target_ids.len() as u64,
    );
    facts.counts.insert(
        "reference_only_targets".into(),
        facts.reference_only_target_ids.len() as u64,
    );
    facts.counts.insert(
        "global_unresolved_sources".into(),
        facts.global_holds.len() as u64,
    );
}
