use super::{download, local, ByteBackupClient, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::v2::access::ReadSelector;
use cloud_backup_protocol::v2::inventory::{InventoryIndex, InventoryPage, InventoryVerifier};
use cloud_backup_protocol::v2::transfer::ObjectReceipt;
use cloud_backup_protocol::Digest;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Inventory {
    pub receipts: BTreeMap<[u8; 32], ObjectReceipt>,
    used: BTreeSet<[u8; 32]>,
    paths: BTreeSet<[u8; 32]>,
    charged: u64,
}

impl Inventory {
    pub async fn load(session: &RestoreSession, client: &ByteBackupClient) -> Result<Self> {
        let accepted = session.accepted();
        let grant = download::one(
            session,
            client,
            ReadSelector::InventoryIndex {
                sha256: accepted.inventory_root.sha256.clone(),
            },
        )
        .await?;
        let path = download::fetch(session, client, &grant, true).await?;
        let index: InventoryIndex = local::read_json(
            &path,
            cloud_backup_protocol::v2::limits::INVENTORY_JSON_MAX_BYTES,
        )?;
        if index.scope != accepted.job().stream.scope || index.root()? != accepted.inventory_root {
            return Err(RestoreError::Invalid("inventory root"));
        }
        let mut verifier = InventoryVerifier::new(&index, accepted.root_receipt_id.clone())?;
        let mut inventory = Self {
            receipts: BTreeMap::new(),
            used: BTreeSet::new(),
            paths: BTreeSet::new(),
            charged: 0,
        };
        for batch in index
            .pages
            .chunks(cloud_backup_protocol::v2::limits::MAX_BATCH_ITEMS)
        {
            let selectors = batch
                .iter()
                .map(|page| ReadSelector::InventoryPage {
                    sha256: page.sha256.clone(),
                })
                .collect();
            let grants = download::grants(session, client, selectors).await?;
            for (descriptor, grant) in batch.iter().zip(grants) {
                if grant.sha256 != descriptor.sha256 || grant.bytes != descriptor.bytes {
                    return Err(RestoreError::Invalid("inventory page grant"));
                }
                let path = download::fetch(session, client, &grant, true).await?;
                let page: InventoryPage = local::read_json(
                    &path,
                    cloud_backup_protocol::v2::limits::INVENTORY_JSON_MAX_BYTES,
                )?;
                verifier.push(&page)?;
                for receipt in page.objects {
                    inventory.charge(lastdb_storage_policy::backup_decode::RECEIPT_MEMORY_BYTES)?;
                    if inventory
                        .receipts
                        .insert(digest_bytes(&receipt.receipt_id)?, receipt)
                        .is_some()
                    {
                        return Err(RestoreError::Invalid("duplicate inventory receipt"));
                    }
                }
            }
        }
        accepted.validate_inventory(&verifier.finish()?)?;
        inventory.use_receipt(&accepted.root_receipt_id)?;
        Ok(inventory)
    }

    pub fn receipt(&self, id: &Digest) -> Result<&ObjectReceipt> {
        self.receipts
            .get(&digest_bytes(id)?)
            .ok_or(RestoreError::Invalid("receipt inventory membership"))
    }

    pub fn use_receipt(&mut self, id: &Digest) -> Result<()> {
        let id = digest_bytes(id)?;
        if !self.receipts.contains_key(&id) {
            return Err(RestoreError::Invalid("receipt inventory membership"));
        }
        if !self.used.contains(&id) {
            self.charge(lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
            self.used.insert(id);
        }
        Ok(())
    }

    pub fn path(&mut self, identity: &[u8]) -> Result<()> {
        self.charge(lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
        if !self
            .paths
            .insert(digest_bytes(&cloud_backup_protocol::bytes_sha256(
                identity,
            ))?)
        {
            return Err(RestoreError::Invalid("duplicate native path or dependency"));
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        if self.used.len() != self.receipts.len() {
            return Err(RestoreError::Invalid("complete used receipt set"));
        }
        Ok(())
    }

    fn charge(&mut self, bytes: u64) -> Result<()> {
        self.charged = self
            .charged
            .checked_add(bytes)
            .ok_or(RestoreError::Limit("inventory memory"))?;
        if self.charged
            > cloud_backup_protocol::limits::DECODED_VALUES_MAX_BYTES
                - lastdb_storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES
        {
            return Err(RestoreError::Limit("inventory memory"));
        }
        Ok(())
    }
}

fn digest_bytes(id: &Digest) -> Result<[u8; 32]> {
    hex::decode(id.as_str())
        .map_err(|_| RestoreError::Invalid("digest"))?
        .try_into()
        .map_err(|_| RestoreError::Invalid("digest"))
}
