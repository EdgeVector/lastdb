use super::{download, inventory::Inventory};
use super::{
    ByteBackupClient, RecoveryKeys, RestoreError, RestoreReport, RestoreSession, Result,
    VerifiedNativeContext,
};
use cloud_backup_protocol::v2::access::ReadSelector;
use fold_db::storage::disk_pack::byte_backup::{ClientRoot, CODEC_ENVELOPE_MAX_BYTES};

/// The root is a mutation log root or a snapshot client root with a run
/// profile. Each strict decode admits only its own version.
pub(super) async fn restore(
    session: &RestoreSession,
    client: &ByteBackupClient,
    keys: &RecoveryKeys,
) -> Result<RestoreReport> {
    let (inventory, bytes) = load(session, client).await?;
    let version = ClientRoot::version_of(&bytes, keys.content_key())
        .map_err(|_| RestoreError::Invalid("client native metadata"))?;
    if version == crate::byte_cloud_log::VERSION {
        let root = crate::byte_cloud_log::MutationLogRoot::decode(
            &bytes,
            keys.content_key(),
            &session.accepted().begin,
        )
        .map_err(|_| RestoreError::Invalid("client mutation log root"))?;
        drop(inventory);
        return super::logs::restore(session, client, keys, root).await;
    }
    let accepted = session.accepted();
    let root = ClientRoot::decode(&bytes, keys.content_key(), &accepted.begin)
        .map_err(|_| RestoreError::Invalid("client native metadata"))?;
    root.recovery.validate_for(&accepted.job().stream.scope)?;
    keys.validate_account(&root.recovery.account_public_key)?;
    let context = VerifiedNativeContext {
        root: &session.root,
        destination: &session.selection.destination,
        session_id: session.selection.session_id,
        commit_sha256: &accepted.commit_sha256,
        native_root: &root.native_root,
        receipt_name: "byte-cloud-restore-receipt.json",
        receipt_version: 2,
    };
    if let Some(report) = context.published()? {
        return Ok(report);
    }
    super::runs::restore(session, client, keys, &root, inventory).await
}

pub(super) async fn load(
    session: &RestoreSession,
    client: &ByteBackupClient,
) -> Result<(Inventory, Vec<u8>)> {
    let accepted = session.accepted();
    let inventory = Inventory::load(session, client).await?;
    let grant = download::one(
        session,
        client,
        ReadSelector::Receipt {
            receipt_id: accepted.root_receipt_id.clone(),
        },
    )
    .await?;
    grant.validate_for_receipt(inventory.receipt(&accepted.root_receipt_id)?)?;
    if grant.bytes.get() > CODEC_ENVELOPE_MAX_BYTES {
        return Err(RestoreError::Limit("client root metadata"));
    }
    let path = download::fetch(session, client, &grant, true).await?;
    let bytes = crate::native_cloud_restore::bounded_bytes(&path, CODEC_ENVELOPE_MAX_BYTES)?;
    if inventory
        .receipt(&accepted.root_receipt_id)?
        .object
        .destination
        != Some(cloud_backup_protocol::v2::Destination::R2)
    {
        return Err(RestoreError::Invalid("client root destination"));
    }
    Ok((inventory, bytes))
}
