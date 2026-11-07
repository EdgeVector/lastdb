//! Real shared storage operations for the generated DEV staging qualification.

use super::super::recovery_home::files;
use super::{deadline, Owner};
use cloud_backup_client::shared_objects::{
    BackupTransportError, SharedObjectClient, SnapshotPublication,
};
use cloud_backup_protocol::shared::objects::{
    staging_expired, SharedObjectRequest, SharedObjectRole, SnapshotHeadRequest,
};
use cloud_backup_protocol::shared::{
    inventory_sha256, SealedObject, SnapshotManifest, SnapshotReceipt,
};
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id};
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::Cursor;
use std::path::Path;

const ACCOUNT: &str = "2a81c95623f2445854a794b5b48471d6";
const BYTES: usize = 32 * 1024;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Plan {
    Upload {
        database_id: Id,
        snapshot_id: Id,
        destination: Destination,
        incomplete: bool,
    },
    Complete {
        request: SharedObjectRequest,
    },
    Stage {
        database_id: Id,
        snapshot_id: Id,
        replaces: Id,
        encrypted_manifest: SealedObject,
        inventory: Vec<SealedObject>,
    },
    Replay {
        expired: Box<SnapshotManifest>,
        inventory: Vec<SealedObject>,
        incomplete: SharedObjectRequest,
        accepted: Box<SnapshotManifest>,
        receipt: SnapshotReceipt,
    },
}

pub(super) async fn execute(owner: &Owner, path: &Path) -> Result<(), String> {
    if owner.account.as_str() != ACCOUNT || !path.is_absolute() {
        return Err("shared staging qualification requires the generated DEV account".into());
    }
    let parent = path.parent().ok_or("shared staging plan directory")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("shared staging plan name")?;
    let directory = files::open_directory(parent)?;
    files::require_owned_directory(&directory)?;
    let bytes = files::read_private_file(&directory, name, 16 * 1024)?;
    let plan: Plan = serde_json::from_slice(&bytes).map_err(|_| "invalid shared staging plan")?;
    let client = SharedObjectClient::from_auth(
        owner.auth.clone(),
        owner.account.clone(),
        owner.device.clone(),
    )
    .map_err(|error| error.to_string())?;
    let scope = Qualification { owner, client };
    println!("{}", scope.run(plan).await?);
    Ok(())
}

struct Qualification<'a> {
    owner: &'a Owner,
    client: SharedObjectClient,
}

impl Qualification<'_> {
    fn signer(&self) -> impl Fn(&[u8]) -> Result<[u8; 64], BackupTransportError> + Sync + '_ {
        move |message| Ok(self.owner.device_signer.sign(message).to_bytes())
    }

    async fn run(&self, plan: Plan) -> Result<Value, String> {
        let sign = self.signer();
        match plan {
            Plan::Upload {
                database_id,
                snapshot_id,
                destination,
                incomplete,
            } => {
                self.upload(database_id, snapshot_id, destination, incomplete)
                    .await
            }
            Plan::Complete { request } => {
                require_request(&request)?;
                let stored = self
                    .client
                    .complete_snapshot_part(&request, deadline(120)?, &sign)
                    .await
                    .map_err(|error| error.to_string())?;
                if stored != request.object {
                    return Err("shared staging completion changed the exact object".into());
                }
                Ok(json!({"verified": true}))
            }
            Plan::Stage {
                database_id,
                snapshot_id,
                replaces,
                encrypted_manifest,
                inventory,
            } => {
                let manifest = SnapshotManifest {
                    database_id,
                    snapshot_id,
                    replaces: Some(replaces),
                    disk_format: fold_db::storage::disk_pack::HOME_FORMAT,
                    covers: Vec::new(),
                    encrypted_manifest,
                    object_count: 1,
                    inventory_sha256: inventory_sha256(&inventory)
                        .map_err(|error| error.to_string())?,
                };
                require_staged_manifest(&manifest, &inventory)?;
                self.client
                    .stage_snapshot_inventory(&manifest, &inventory, &sign)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(json!(manifest))
            }
            Plan::Replay {
                expired,
                inventory,
                incomplete,
                accepted,
                receipt,
            } => {
                self.replay(&expired, &inventory, &incomplete, &accepted, &receipt)
                    .await
            }
        }
    }

    async fn upload(
        &self,
        database_id: Id,
        snapshot_id: Id,
        destination: Destination,
        incomplete: bool,
    ) -> Result<Value, String> {
        require_database(&database_id)?;
        let mut payload = vec![0; BYTES];
        rand::thread_rng().fill_bytes(&mut payload);
        let request = SharedObjectRequest {
            database_id,
            object: SealedObject {
                version_id: super::id(uuid::Uuid::new_v4().to_string())?,
                bytes: DecimalU64::new(BYTES as u64),
                sha256: bytes_sha256(&payload),
            },
            role: SharedObjectRole::NativeSnapshot {
                snapshot_id,
                destination,
            },
        };
        let source = Box::new(Cursor::new(payload));
        let sign = self.signer();
        if incomplete {
            self.client
                .stage_snapshot_part(&request, source, deadline(120)?, &sign)
                .await
                .map_err(|error| error.to_string())?;
        } else {
            let stored = self
                .client
                .upload_exact(&request, source, deadline(120)?, &sign)
                .await
                .map_err(|error| error.to_string())?;
            if stored != request.object {
                return Err("shared staging upload changed the exact object".into());
            }
        }
        Ok(json!(request))
    }

    async fn replay(
        &self,
        expired: &SnapshotManifest,
        inventory: &[SealedObject],
        incomplete: &SharedObjectRequest,
        accepted: &SnapshotManifest,
        receipt: &SnapshotReceipt,
    ) -> Result<Value, String> {
        require_staged_manifest(expired, inventory)?;
        require_request(incomplete)?;
        if expired.database_id != accepted.database_id
            || incomplete.database_id != expired.database_id
            || expired.replaces.as_ref() != Some(&accepted.snapshot_id)
            || !matches!(&incomplete.role, SharedObjectRole::NativeSnapshot { snapshot_id, .. } if snapshot_id == &expired.snapshot_id)
            || &accepted.receipt().map_err(|error| error.to_string())? != receipt
        {
            return Err("shared staging replay scope differs".into());
        }
        let sign = self.signer();
        require_expired(
            self.client
                .stage_snapshot_inventory(expired, inventory, &sign)
                .await,
        )?;
        require_expired(self.client.publish_snapshot(expired, &sign).await)?;
        require_expired(
            self.client
                .complete_snapshot_part(incomplete, deadline(120)?, &sign)
                .await,
        )?;
        let head = self
            .client
            .snapshot_head(
                &SnapshotHeadRequest {
                    database_id: accepted.database_id.clone(),
                },
                &sign,
            )
            .await
            .map_err(|error| error.to_string())?;
        if head.as_ref() != Some(receipt) {
            return Err("shared staging accepted snapshot head changed".into());
        }
        let replayed = self
            .client
            .publish_snapshot(accepted, &sign)
            .await
            .map_err(|error| error.to_string())?;
        if !matches!(replayed, SnapshotPublication::Accepted(saved) if &saved == receipt) {
            return Err("shared staging accepted receipt changed".into());
        }
        Ok(json!({"expired_staging_refused": true, "accepted_receipt_replayed": true}))
    }
}

fn require_database(database: &Id) -> Result<(), String> {
    if !database.as_str().starts_with("c1da11d0-") {
        return Err("shared staging qualification requires an owned DEV database".into());
    }
    Ok(())
}

fn require_request(request: &SharedObjectRequest) -> Result<(), String> {
    require_database(&request.database_id)?;
    if request.object.bytes.get() != BYTES as u64 {
        return Err("shared staging object length differs".into());
    }
    Ok(())
}

fn require_staged_manifest(
    manifest: &SnapshotManifest,
    inventory: &[SealedObject],
) -> Result<(), String> {
    require_database(&manifest.database_id)?;
    if manifest.object_count != 1
        || inventory.len() != 1
        || inventory[0].bytes.get() != BYTES as u64
        || manifest.encrypted_manifest.bytes.get() != BYTES as u64
        || inventory_sha256(inventory).map_err(|error| error.to_string())?
            != manifest.inventory_sha256
    {
        return Err("shared staging manifest differs from the generated inventory".into());
    }
    Ok(())
}

fn require_expired<T>(result: Result<T, BackupTransportError>) -> Result<(), String> {
    if !matches!(result, Err(BackupTransportError::Protocol(error)) if staging_expired(&error)) {
        return Err("shared staging did not return its exact expiry refusal".into());
    }
    Ok(())
}
