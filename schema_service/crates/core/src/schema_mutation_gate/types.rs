//! Challenge request/response types, headers and the quota store.

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMutationChallengeRequest {
    pub node_public_key: String,
    pub schema_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMutationChallengeResponse {
    pub challenge_id: String,
    pub nonce: String,
    pub challenge_mac: String,
    pub node_public_key_hash: String,
    pub schema_hash: String,
    pub difficulty_bits: u8,
    pub expires_at_unix_secs: u64,
    pub counter_start: u64,
    pub pow_input: String,
}

#[derive(Debug, Clone, Default)]
pub struct SchemaMutationGateHeaders {
    pub node_public_key: Option<String>,
    pub node_signature: Option<String>,
    pub challenge_id: Option<String>,
    pub nonce: Option<String>,
    pub challenge_mac: Option<String>,
    pub difficulty_bits: Option<String>,
    pub expires_at_unix_secs: Option<String>,
    pub counter: Option<String>,
    pub dev_pubkey: Option<String>,
}

pub(super) struct PowChallengeProof<'a> {
    pub(super) nonce: &'a str,
    pub(super) challenge_mac: &'a str,
    pub(super) counter: u64,
    pub(super) node_public_key_hash: &'a str,
    pub(super) schema_hash: &'a str,
    pub(super) difficulty_bits: u8,
    pub(super) expires_at_unix_secs: u64,
}

pub trait SchemaMutationGateQuotaStore: Send + Sync {
    fn backend_label(&self) -> &'static str;

    fn bucket_len(
        &self,
        key: &str,
        window: Duration,
        now: u64,
    ) -> Result<usize, SchemaMutationGateError>;

    fn check_quota_bucket(
        &self,
        bucket_label: &'static str,
        window_label: &'static str,
        key: &str,
        window: Duration,
        limit: usize,
        now: u64,
    ) -> Result<(), SchemaMutationGateError>;
}

#[derive(Clone)]
pub struct SchemaMutationGateStore {
    pub(super) backend: Arc<dyn SchemaMutationGateQuotaStore>,
}

impl SchemaMutationGateStore {
    pub fn new(backend: Arc<dyn SchemaMutationGateQuotaStore>) -> Self {
        Self { backend }
    }

    pub fn backend_label(&self) -> &'static str {
        self.backend.backend_label()
    }
}

impl Default for SchemaMutationGateStore {
    fn default() -> Self {
        Self::new(Arc::new(InMemorySchemaMutationGateQuotaStore::default()))
    }
}

impl fmt::Debug for SchemaMutationGateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaMutationGateStore")
            .field("backend", &self.backend_label())
            .finish()
    }
}

#[derive(Debug, Default)]
pub(super) struct InMemorySchemaMutationGateQuotaStore {
    pub(super) quota_events: RwLock<HashMap<String, VecDeque<u64>>>,
}
