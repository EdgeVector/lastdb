//! Verified immutable metadata is cached; private download grants never enter receipts.

use super::AuthClient;
use crate::sync::cloud_purge::inventory::{
    canonical_inventory, CloudInventoryRef, INVENTORY_METADATA_MAX_BYTES,
};
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudReplacementManifest};
use crate::sync::{SyncError, SyncResult};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

type CachedInventory = (CloudInventoryRef, Arc<Value>);

impl AuthClient {
    pub(super) async fn post_once_inner(&self, path: &str, body: Value) -> SyncResult<Value> {
        let mut response =
            tokio::time::timeout(self.request_timeout, self.post_http_once(path, body))
                .await
                .map_err(|_| SyncError::Network("auth Lambda request timed out".into()))??;
        // The small API response retains its deadline. The bounded inventory
        // GET uses a connection/stall deadline, so slow large metadata remains usable.
        self.hydrate_cloud_inventory_response(&mut response).await?;
        Ok(response)
    }

    pub(super) async fn hydrate_cloud_inventory_response(
        &self,
        response: &mut Value,
    ) -> SyncResult<()> {
        let control = if response.get("control").is_some() {
            &mut response["control"]
        } else {
            response
        };
        if !control.is_object() || control.get("operation_id").is_none() {
            return Ok(());
        }
        self.hydrate_one_control(control).await?;
        if let Some(previous) = control
            .get_mut("previous_ready_control")
            .filter(|value| !value.is_null())
        {
            if previous
                .get("previous_ready_control")
                .is_some_and(|value| !value.is_null())
            {
                return Err(failure("nested historical cloud controls are unsupported"));
            }
            self.hydrate_one_control(previous).await?;
        }
        Ok(())
    }

    async fn hydrate_one_control(&self, control: &mut Value) -> SyncResult<()> {
        let Some(raw_ref) = control
            .get("replacement_manifest_ref")
            .filter(|value| !value.is_null())
        else {
            return Ok(());
        };
        let account: CloudAccount = serde_json::from_value(control["account"].clone())
            .map_err(|_| failure("cloud inventory account is absent"))?;
        let operation = control["operation_id"]
            .as_str()
            .ok_or_else(|| failure("cloud inventory operation is absent"))?
            .to_string();
        let reference: CloudInventoryRef = serde_json::from_value(raw_ref.clone())
            .map_err(|_| failure("invalid cloud inventory reference"))?;
        reference.validate(&account, &operation, "replacement")?;
        let manifest = if let Some(cached) = cached_inventory(&reference)? {
            validate_control(control, &cached, &reference)?;
            cached
        } else {
            let _download = download_lock().lock().await;
            if let Some(cached) = cached_inventory(&reference)? {
                validate_control(control, &cached, &reference)?;
                control["replacement_manifest"] = (*cached).clone();
                control["replacement_manifest_ref"] = serde_json::to_value(reference)?;
                return Ok(());
            }
            let bytes = download_inventory(raw_ref, &reference).await?;
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|_| failure("invalid cloud inventory JSON"))?;
            if canonical_inventory(&value, INVENTORY_METADATA_MAX_BYTES)? != bytes {
                return Err(failure("cloud inventory JSON is not canonical"));
            }
            validate_manifest(control, &value, &reference)?;
            let value = Arc::new(value);
            cache_inventory(reference.clone(), Arc::clone(&value))?;
            value
        };
        control["replacement_manifest"] = (*manifest).clone();
        // Retain only the immutable identity in the validated control response.
        control["replacement_manifest_ref"] = serde_json::to_value(reference)?;
        Ok(())
    }
}

async fn download_inventory(grant: &Value, reference: &CloudInventoryRef) -> SyncResult<Vec<u8>> {
    let url = grant
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| failure("cloud inventory download grant absent"))?;
    let parsed =
        reqwest::Url::parse(url).map_err(|_| failure("invalid cloud inventory download grant"))?;
    if parsed.scheme() != "https"
        || grant["method"].as_str() != Some("GET")
        || grant["expires_in_secs"]
            .as_u64()
            .is_none_or(|seconds| seconds == 0 || seconds > 900)
    {
        return Err(failure("invalid cloud inventory download grant"));
    }
    // trace-egress: skip-s3 — the immutable inventory grant is a signed provider URL.
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(60))
        .build()
        .map_err(|_| failure("cloud inventory transport unavailable"))?;
    let mut response = client
        .get(parsed)
        .send()
        .await
        .map_err(|_| failure("cloud inventory download failed"))?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|length| length != reference.bytes)
    {
        return Err(failure("cloud inventory download length or status differs"));
    }
    let mut bytes = Vec::with_capacity(reference.bytes as usize);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| failure("cloud inventory download interrupted"))?
    {
        if chunk.len() > (reference.bytes as usize).saturating_sub(bytes.len()) {
            return Err(failure("cloud inventory download exceeds its exact length"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != reference.bytes || crate::hex::sha256_hex(&bytes) != reference.sha256 {
        return Err(failure("cloud inventory download checksum differs"));
    }
    Ok(bytes)
}

fn validate_manifest(
    control: &Value,
    manifest: &Value,
    reference: &CloudInventoryRef,
) -> SyncResult<()> {
    validate_control(control, manifest, reference)?;
    let typed: CloudReplacementManifest = serde_json::from_value(manifest.clone())
        .map_err(|_| failure("invalid replacement inventory"))?;
    if typed.format_version != 1
        || typed.generation == 0
        || typed.roots.is_empty()
        || typed.sha256().map_err(SyncError::Storage)? != reference.sha256
    {
        return Err(failure(
            "replacement inventory differs from its cloud control",
        ));
    }
    Ok(())
}

fn validate_control(
    control: &Value,
    manifest: &Value,
    reference: &CloudInventoryRef,
) -> SyncResult<()> {
    if control["format_version"].as_u64() != Some(1)
        || control["phase"].as_str() != Some("ready")
        || control["deletion_receipt"]["complete"].as_bool() != Some(true)
        || manifest["account"] != control["account"]
        || manifest["operation_id"] != control["operation_id"]
        || manifest["generation"] != control["generation"]
        || control
            .get("replacement_manifest_sha256")
            .and_then(Value::as_str)
            .is_some_and(|sha| sha != reference.sha256)
    {
        return Err(failure(
            "replacement inventory differs from its cloud control",
        ));
    }
    Ok(())
}

fn download_lock() -> &'static tokio::sync::Mutex<()> {
    static DOWNLOAD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &DOWNLOAD
}

fn cache() -> &'static Mutex<VecDeque<CachedInventory>> {
    static CACHE: OnceLock<Mutex<VecDeque<CachedInventory>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(VecDeque::new()))
}

fn cached_inventory(reference: &CloudInventoryRef) -> SyncResult<Option<Arc<Value>>> {
    Ok(cache()
        .lock()
        .map_err(|_| failure("cloud inventory cache unavailable"))?
        .iter()
        .find(|(identity, _)| identity == reference)
        .map(|(_, value)| Arc::clone(value)))
}

fn cache_inventory(reference: CloudInventoryRef, value: Arc<Value>) -> SyncResult<()> {
    let mut entries = cache()
        .lock()
        .map_err(|_| failure("cloud inventory cache unavailable"))?;
    if entries.iter().any(|(identity, _)| identity == &reference) {
        return Ok(());
    }
    while !entries.is_empty()
        && (entries.len() >= 8
            || entries
                .iter()
                .map(|(identity, _)| identity.bytes)
                .sum::<u64>()
                + reference.bytes
                > INVENTORY_METADATA_MAX_BYTES as u64)
    {
        entries.pop_front();
    }
    entries.push_back((reference, value));
    Ok(())
}

fn failure(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
