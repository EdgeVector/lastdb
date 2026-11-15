//! Open an existing owner's exact personal cloud log without touching local sync state.

use std::path::Path;
use std::sync::Arc;

use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::{AccountId, Id};

use super::{cloud::CloudLog, http, registry, session};
use crate::cloud_backup_client::{AuthClient, SignedBlobClient};

pub(super) async fn personal_log(home: &Path, selector: &str) -> Result<(Id, CloudLog), String> {
    let database = registry::databases(home)
        .into_iter()
        .find(|db| {
            !db.native && db.kind == "personal" && (db.id == selector || selector == "personal")
        })
        .ok_or("select personal or its exact database id in this home")?;
    let database = Id::try_from(database.id).map_err(|error| error.to_string())?;
    let log = owner_log(home, database.clone()).await?;
    Ok((database, log))
}

async fn owner_log(home: &Path, database_id: Id) -> Result<CloudLog, String> {
    let mut saved = crate::cloud::load_saved_credentials(home)?
        .ok_or("cloud log maintenance needs the existing saved cloud connection")?;
    let selected = http::saved_service(&saved)?;
    let keypair = Arc::new(
        crate::cloud::load_home_keypair(home)?.ok_or("home has no existing owner identity")?,
    );
    let account = crate::identity::user_hash_from_pubkey(&keypair.public_key_base64())?;
    let account = AccountId::try_from(account).map_err(|error| error.to_string())?;
    if saved.connection.as_ref().map(|conn| &conn.account_id) != Some(&account) {
        return Err("the saved cloud connection belongs to a different owner identity".into());
    }
    let device = session::saved_device_id(home)?.ok_or("home has no existing sync device id")?;
    let endpoint = http::Endpoint::open(&selected.api_url, selected.label)?;
    let backup = SignedBlobClient::from_auth_and_keypair(
        AuthClient::new(saved.api_url, std::mem::take(&mut *saved.api_key)),
        Arc::clone(&keypair),
    )
    .map_err(|error| error.to_string())?;
    let context = backup
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|_| "cloud account context failed")?;
    if context.scope.account_id != account {
        return Err("the cloud returned a different owner account".into());
    }
    CloudLog::open_offline(
        endpoint,
        context.scope,
        database_id,
        &device,
        keypair,
        selected.label,
    )
}
