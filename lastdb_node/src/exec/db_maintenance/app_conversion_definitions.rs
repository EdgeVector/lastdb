//! Resolve exact published definitions before a mapped conversion can freeze data.

use fold_db::fold_db_core::fold_db::AppSchemaConversion;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn resolve(request: &mut AppSchemaConversion) -> Result<(), String> {
    let AppSchemaConversion::AppBegin { schemas, .. } = request else {
        return Ok(());
    };
    if schemas.is_empty() || schemas.len() > 64 {
        return Err("conversion requires 1 through 64 kept schemas".into());
    }
    let mut identities = BTreeSet::new();
    for schema in schemas.iter() {
        if let Some(identity) = schema.mapping_target().map_err(|error| error.to_string())? {
            identities.insert(identity.to_string());
        }
    }
    if identities.is_empty() {
        return Ok(());
    }
    let client = schema_service_client::SchemaServiceClient::new(
        &folddb_profile::endpoints::schema_service_url(),
    );
    // Independent definition reads share one bounded concurrent request set.
    let replies = futures::future::try_join_all(
        identities
            .iter()
            .map(|identity| client.find_schema(identity)),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut definitions = BTreeMap::new();
    for (identity, reply) in identities.into_iter().zip(replies) {
        let published =
            reply.ok_or("publish the exact target definition before its field conversion")?;
        if published.schema.identity_hash.as_deref() != Some(identity.as_str()) {
            return Err("Schema Service returned a different target identity".into());
        }
        definitions.insert(identity, published.schema);
    }
    for schema in schemas {
        if let Some(identity) = &schema.target_definition_identity {
            schema.resolved_definition = Some(
                definitions
                    .get(identity)
                    .ok_or("a target definition reply is absent")?
                    .clone(),
            );
        }
    }
    Ok(())
}
