//! Verified backup object inventory and recovery key derivation.
use super::{download, local, ByteBackupReader, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::v2::access::ReadSelector;
use cloud_backup_protocol::v2::inventory::{InventoryIndex, InventoryPage, InventoryVerifier};
use cloud_backup_protocol::v2::transfer::ObjectReceipt;
use cloud_backup_protocol::{AccountId, Digest};
use fold_db::crypto::E2eKeys;
use std::collections::{BTreeMap, BTreeSet};
use zeroize::Zeroizing;

/// The caller supplies the phrase again after restart; the session stores no secret.
pub struct RecoveryKeys {
    seed: Zeroizing<[u8; 32]>,
    decryption: RecoveryDecryption,
    account_id: AccountId,
}

/// Key material used to verify and decrypt a backup.
pub(super) struct RecoveryDecryption {
    content: Zeroizing<[u8; 32]>,
    account_public_key: String,
}

impl RecoveryKeys {
    /// Derive locally before the opaque encrypted root is available.
    pub fn from_scope_phrase(phrase: &str, account_id: &AccountId) -> Result<Self> {
        let keys = Self::from_phrase(phrase)?;
        if keys.account_id != *account_id {
            return Err(RestoreError::Invalid("recovery account"));
        }
        Ok(keys)
    }

    fn from_phrase(phrase: &str) -> Result<Self> {
        let seed = Zeroizing::new(
            crate::cloud::seed_from_phrase(phrase)
                .map_err(|_| RestoreError::Invalid("recovery phrase"))?,
        );
        Self::from_seed(seed)
    }

    pub(crate) fn from_seed(seed: Zeroizing<[u8; 32]>) -> Result<Self> {
        let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| RestoreError::Invalid("recovery key"))?;
        let keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| RestoreError::Invalid("recovery derivation"))?;
        let account_id = crate::identity::user_hash_from_pubkey(&pair.public_key_base64())
            .map_err(|_| RestoreError::Invalid("recovery account"))?;
        Ok(Self {
            seed,
            decryption: RecoveryDecryption {
                content: Zeroizing::new(keys.encryption_key()),
                account_public_key: fold_db::hex::hex_lower(pair.public_key_bytes()),
            },
            account_id: AccountId::try_from(account_id)
                .map_err(|_| RestoreError::Invalid("recovery account"))?,
        })
    }

    pub(super) fn decryption(&self) -> &RecoveryDecryption {
        &self.decryption
    }

    pub(crate) fn account_seed(&self) -> &[u8; 32] {
        &self.seed
    }
}

impl RecoveryDecryption {
    pub(super) fn for_publication(
        content: Zeroizing<[u8; 32]>,
        account_public_key: &Digest,
    ) -> Self {
        Self {
            content,
            account_public_key: account_public_key.as_str().to_owned(),
        }
    }

    pub(super) fn validate_account(&self, account_public_key: &Digest) -> Result<()> {
        if self.account_public_key != account_public_key.as_str() {
            return Err(RestoreError::Invalid("recovery public key"));
        }
        Ok(())
    }

    pub(super) fn content_key(&self) -> &[u8; 32] {
        &self.content
    }
}

pub(super) struct Inventory {
    pub receipts: BTreeMap<[u8; 32], ObjectReceipt>,
    used: BTreeSet<[u8; 32]>,
    paths: BTreeSet<[u8; 32]>,
    charged: u64,
}

impl Inventory {
    pub async fn load(session: &RestoreSession, client: &ByteBackupReader) -> Result<Self> {
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
                    inventory
                        .charge(fold_db::storage_policy::backup_decode::RECEIPT_MEMORY_BYTES)?;
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
            self.charge(fold_db::storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
            self.used.insert(id);
        }
        Ok(())
    }

    pub fn path(&mut self, identity: &[u8]) -> Result<()> {
        self.charge(fold_db::storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)?;
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

    pub(super) fn charge(&mut self, bytes: u64) -> Result<()> {
        self.charged = self
            .charged
            .checked_add(bytes)
            .ok_or(RestoreError::Limit("inventory memory"))?;
        if self.charged
            > cloud_backup_protocol::limits::DECODED_VALUES_MAX_BYTES
                - fold_db::storage_policy::backup_decode::JSON_PAGE_RESERVE_BYTES
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
