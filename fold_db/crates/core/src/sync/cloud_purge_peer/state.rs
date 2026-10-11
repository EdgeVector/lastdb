//! Monotonic, crash-safe observed and adopted generation receipts.

use super::{held, AdoptionHandler, AdoptionPlan, AdoptionReceipt};
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudControlGet, CloudPurgeControl};
use crate::sync::error::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

mod continuation;
mod control_files;
mod index;
mod persistence;
mod token;
use control_files::{ControlCache, ControlRef};
use persistence::validate_successor;
pub(super) use persistence::{account_key, private_directory, write_atomic};
use token::AdoptionToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAccountStatus {
    pub account: CloudAccount,
    pub observed_generation: u64,
    pub observed_revision: u64,
    pub operation_id: String,
    pub phase: String,
    pub adopted_generation: u64,
    pub adopted_manifest_sha256: Option<String>,
    pub retention_directory: Option<PathBuf>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct PeerState {
    format_version: u32,
    accounts: BTreeMap<String, PeerAccount>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PeerAccount {
    control: ControlRef,
    #[serde(default)]
    adoption: Option<AdoptionReceipt>,
}

/// One handle is shared by every publisher that uses the engine's AuthClient.
/// Adoption tokens are deliberately memory-only and never enter a journal.
pub struct CloudPurgePeer {
    pub(super) directory: PathBuf,
    pub(super) source_id: String,
    inner: Mutex<PeerState>,
    control_cache: Mutex<ControlCache>,
    tokens: Mutex<BTreeMap<String, AdoptionToken>>,
    local_accounts: Mutex<BTreeSet<String>>,
    pub(super) adoption_turn: tokio::sync::Mutex<()>,
    handler: RwLock<Option<AdoptionHandler>>,
}

impl CloudPurgePeer {
    /// The peer journal and retained homes survive replacement of the active
    /// home's data. Resolve symlinks before deriving the stable sibling path.
    pub fn directory_for_home(home: &Path) -> SyncResult<PathBuf> {
        let canonical = fs::canonicalize(home)?;
        let parent = canonical
            .parent()
            .ok_or_else(|| held("cloud peer home has no parent"))?;
        let identity = crate::hex::sha256_hex(canonical.as_os_str().as_encoded_bytes());
        Ok(parent
            .join(".lastdb-cloud-purge")
            .join(identity)
            .join("peer"))
    }

    pub fn open(directory: impl AsRef<Path>, source_id: String) -> SyncResult<Arc<Self>> {
        if source_id.is_empty() {
            return Err(held("cloud peer source identity is empty"));
        }
        let directory = directory.as_ref().join("cloud-purge-peer-v1");
        private_directory(&directory)?;
        let state = index::open(&directory, &source_id)?;
        Ok(Arc::new(Self {
            directory,
            source_id,
            inner: Mutex::new(state),
            control_cache: Mutex::new(ControlCache::default()),
            tokens: Mutex::new(BTreeMap::new()),
            local_accounts: Mutex::new(BTreeSet::new()),
            adoption_turn: tokio::sync::Mutex::new(()),
            handler: RwLock::new(None),
        }))
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Only the host's configured sync targets may replace local data.
    /// Foreign inbox/file scopes never authorize adoption of this home.
    pub fn bind_local_account(&self, account: &CloudAccount) -> SyncResult<()> {
        self.local_accounts
            .lock()
            .map_err(|_| held("cloud peer scope lock failed"))?
            .insert(account_key(account)?);
        Ok(())
    }

    pub fn is_local_account(&self, account: &CloudAccount) -> SyncResult<bool> {
        Ok(self
            .local_accounts
            .lock()
            .map_err(|_| held("cloud peer scope lock failed"))?
            .contains(&account_key(account)?))
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn set_adoption_handler(&self, handler: AdoptionHandler) -> SyncResult<()> {
        *self
            .handler
            .write()
            .map_err(|_| held("cloud adoption handler lock failed"))? = Some(handler);
        Ok(())
    }

    pub(super) fn adoption_handler(&self) -> SyncResult<Option<AdoptionHandler>> {
        Ok(self
            .handler
            .read()
            .map_err(|_| held("cloud adoption handler lock failed"))?
            .clone())
    }

    /// A failed control read never clears the receipt. A missing or older
    /// server record after an observed hold is a protocol violation.
    pub fn observe(&self, response: &CloudControlGet) -> SyncResult<()> {
        if response.notification_only || response.notification.is_some() {
            return Err(held(
                "a foreign cloud notification cannot replace account authority",
            ));
        }
        let Some(control) = response.control.as_ref() else {
            if response.protocol_required || response.generation != 0 {
                return Err(held("cloud control is required but absent"));
            }
            if let Some(account) = response.account.as_ref() {
                if self.known_control(account)?.is_some() {
                    return Err(held("cloud control disappeared after a prior purge"));
                }
            }
            return Ok(());
        };
        control.validate_identity().map_err(held)?;
        if !response.protocol_required || response.generation != control.generation {
            return Err(held("cloud control generation is inconsistent"));
        }
        if let Some(account) = response.account.as_ref() {
            if account != &control.account {
                return Err(held("cloud control resolved a different account"));
            }
        }
        let key = account_key(&control.account)?;
        let mut state = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?;
        let previous = state.accounts.get(&key);
        let previous_control = previous
            .map(|previous| self.load_control(&previous.control))
            .transpose()?;
        if let Some(previous) = previous_control.as_ref() {
            validate_successor(previous, control)?;
            if **previous == *control {
                return Ok(());
            }
        }
        let changed = previous_control.as_ref().is_some_and(|previous| {
            previous.generation != control.generation
                || previous.operation_id != control.operation_id
                || previous.replacement_manifest != control.replacement_manifest
                || previous.continuation != control.continuation
        });
        let adoption = previous.and_then(|entry| entry.adoption.clone());
        let mut next = state.clone();
        next.accounts.insert(
            key.clone(),
            PeerAccount {
                control: self.store_control(control)?,
                adoption,
            },
        );
        self.write_state(&next)?;
        *state = next;
        // A token never crosses a generation change or a renewed hold.
        if changed || !control.permits_local_adoption() {
            self.tokens
                .lock()
                .map_err(|_| held("cloud peer token lock failed"))?
                .remove(&key);
        }
        Ok(())
    }

    pub fn known_control(&self, account: &CloudAccount) -> SyncResult<Option<CloudPurgeControl>> {
        let key = account_key(account)?;
        let reference = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?
            .accounts
            .get(&key)
            .map(|entry| entry.control.clone());
        reference
            .as_ref()
            .map(|reference| {
                self.load_control(reference)
                    .map(|control| (*control).clone())
            })
            .transpose()
    }

    pub fn statuses(&self) -> SyncResult<Vec<PeerAccountStatus>> {
        let state = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?;
        state
            .accounts
            .values()
            .map(|entry| {
                Ok(PeerAccountStatus {
                    account: entry.control.account.clone(),
                    observed_generation: entry.control.generation,
                    observed_revision: entry.control.revision,
                    operation_id: entry.control.operation_id.clone(),
                    phase: serde_json::to_value(&entry.control.phase)?
                        .as_str()
                        .unwrap_or("unknown")
                        .into(),
                    adopted_generation: entry.adoption.as_ref().map_or(0, |r| r.generation),
                    adopted_manifest_sha256: entry
                        .adoption
                        .as_ref()
                        .map(|r| r.installed_manifest_sha256.clone()),
                    retention_directory: entry
                        .adoption
                        .as_ref()
                        .map(|r| r.retained_directory.clone()),
                })
            })
            .collect()
    }

    pub fn adoption_receipt(&self, plan: &AdoptionPlan) -> SyncResult<Option<AdoptionReceipt>> {
        let key = account_key(&plan.account)?;
        let state = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?;
        Ok(state
            .accounts
            .get(&key)
            .and_then(|entry| entry.adoption.as_ref())
            .filter(|receipt| receipt.matches(plan))
            .cloned())
    }

    /// Call only after the active local destination switches durably. The
    /// retention receipt proves the prior local data and queue remain available.
    pub fn mark_local_adopted(
        &self,
        plan: &AdoptionPlan,
        receipt: AdoptionReceipt,
    ) -> SyncResult<()> {
        receipt.validate(plan)?;
        super::retention::verify_retention_receipt(&receipt)?;
        let key = account_key(&plan.account)?;
        let mut state = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?;
        let current = state
            .accounts
            .get(&key)
            .ok_or_else(|| held("no observed cloud adoption control"))?;
        let control = self.load_control(&current.control)?;
        super::effective_adoption_control(&control, plan)?;
        let mut next = state.clone();
        next.accounts
            .get_mut(&key)
            .expect("entry checked above")
            .adoption = Some(receipt);
        self.write_state(&next)?;
        *state = next;
        Ok(())
    }

    fn write_state(&self, state: &PeerState) -> SyncResult<()> {
        index::write(&self.directory, state)
    }
}
