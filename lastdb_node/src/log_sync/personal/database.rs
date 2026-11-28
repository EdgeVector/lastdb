//! The store and identities selected for a sync turn and its background work.
use super::birth_of;
use crate::host::Host;
use crate::log_sync::chain_author::{self, AuthorIdentity};
use crate::personal_identity;
use crate::personal_seal::Sealer;
use cloud_backup_protocol::{AccountId, CloudScope, DecimalU64};
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::disk_backup::accepted::UploadIdentity;
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{AcceptedMeta, DatabaseId, DeviceId, EntryHash, SignedEntry};
use std::sync::Arc;
use zeroize::Zeroizing;

/// A database's store, signed stream identities, sealing key and upload position.
pub(in crate::log_sync) struct SyncDatabase {
    pub(super) store: Arc<FoldDB>,
    pub(super) database_id: String,
    pub(super) database: DatabaseId,
    pub(super) identity: AuthorIdentity,
    pub(super) node_hex: String,
    pub(super) device_hex: String,
    pub(super) birth_device_hex: String,
    /// The device id of the stream that holds the `snapshot` entries, and its hex.
    pub(super) snapshot_device: DeviceId,
    pub(super) snapshot_device_hex: String,
    /// The name of this device's stream in the head rows.
    pub(super) own_stream: String,
    pub(super) sealer: Sealer,
    /// The birth entry. It is the same on every device. A device uploads it
    /// when the relay lacks it, and every personal entry cites it.
    pub(super) birth_entry: SignedEntry,
    pub(super) birth: AcceptedMeta,
    pub(super) birth_hash: EntryHash,
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) upload: Arc<UploadIdentity>,
}

impl SyncDatabase {
    pub(super) fn personal(host: &Host) -> Result<Self, String> {
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let database_id = personal_identity::database_id(&seed);
        let registered = host
            .owned
            .owner()
            .map_err(|_| "the personal database is not registered".to_owned())?
            .id;
        if registered != database_id {
            return Err("the personal database id is not the one of the recovery phrase".into());
        }
        let database = DatabaseId::from_hex(&database_id).map_err(|_| "database id is not hex")?;
        let identity =
            chain_author::identity_of(host).map_err(|error| error.reply().message.to_owned())?;
        let birth_device = DeviceId::from_bytes(personal_identity::birth_device(&seed));
        let (birth_entry, birth, birth_hash) = birth_of(&seed, &identity)?;
        let snapshot_device = DeviceId::from_bytes(personal_identity::snapshot_device(&seed));
        let node_hex = identity.node_key.to_hex();
        let device_hex = identity.device_id.to_hex();
        let account = AccountId::try_from(node_hex[..32].to_owned())
            .map_err(|_| "the writer key is not an account id".to_owned())?;
        // The upload position of the spool is bound to this identity. The relay
        // is named by a fixed word, not by its address, so moving to another
        // relay keeps the position: the stream is the same on every relay.
        let upload = Arc::new(UploadIdentity {
            scope: CloudScope {
                account_id: account,
                account_generation: DecimalU64::new(1),
            },
            environment: "relay".to_owned(),
            database_id: database_id.clone(),
            device_id: device_hex.clone(),
            writer_key: node_hex.clone(),
        });
        Ok(Self {
            store: Arc::clone(&host.db),
            sealer: Sealer::for_node_seed(&seed)?,
            birth_device_hex: birth_device.to_hex(),
            snapshot_device,
            snapshot_device_hex: snapshot_device.to_hex(),
            own_stream: format!("{node_hex}:{device_hex}"),
            runtime: host.db.db_ops().native_runtime(),
            database_id,
            database,
            identity,
            node_hex,
            device_hex,
            birth_entry,
            birth,
            birth_hash,
            upload,
        })
    }
}
