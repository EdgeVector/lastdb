//! Canonical metadata pages stay below the service request limit.

use super::super::AuthClient;
use crate::sync::cloud_purge::inventory::{
    canonical_inventory, inventory_key, CloudInventoryRef, INVENTORY_METADATA_MAX_BYTES,
    INVENTORY_PAGE_MAX_BYTES,
};
use crate::sync::cloud_purge::protocol::CloudAccount;
use crate::sync::{SyncError, SyncResult};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Serialize;
use serde_json::{json, Value};

impl AuthClient {
    pub async fn stage_cloud_inventory<T: Serialize>(
        &self,
        account: &CloudAccount,
        operation: &str,
        capability: &str,
        kind: &str,
        inventory: &T,
    ) -> SyncResult<CloudInventoryRef> {
        let bytes = canonical_inventory(inventory, INVENTORY_METADATA_MAX_BYTES)?;
        let reference = CloudInventoryRef {
            key: inventory_key(account, operation, kind, &crate::hex::sha256_hex(&bytes)),
            sha256: crate::hex::sha256_hex(&bytes),
            bytes: bytes.len() as u64,
        };
        reference.validate(account, operation, kind)?;
        let identity = json!({"account":account,"cloud_operation_id":operation,
            "cloud_capability":capability,"inventory_kind":kind,
            "inventory_sha256":reference.sha256,"inventory_bytes":reference.bytes});
        let mut receipts = Vec::new();
        for (batch_index, batch) in bytes.chunks(INVENTORY_PAGE_MAX_BYTES * 2).enumerate() {
            let mut pages = Vec::new();
            for (offset, page) in batch.chunks(INVENTORY_PAGE_MAX_BYTES).enumerate() {
                let page_index = batch_index * 2 + offset;
                let spec = json!({"page_index":page_index,"sha256":crate::hex::sha256_hex(page),
                    "bytes":page.len()});
                let mut payload = spec.clone();
                payload["payload_base64"] = json!(STANDARD.encode(page));
                pages.push(payload);
                receipts.push(spec);
            }
            let mut request = identity.clone();
            request["pages"] = json!(pages);
            let response: Value = self
                .purge_call(request, "cloud_purge_inventory_stage")
                .await?;
            validate_pages(&reference, &receipts[batch_index * 2..], &response)?;
        }
        let mut request = identity;
        request["pages"] = json!(receipts);
        let response: Value = self
            .purge_call(request, "cloud_purge_inventory_seal")
            .await?;
        let sealed: CloudInventoryRef = serde_json::from_value(
            response
                .get("inventory_ref")
                .cloned()
                .unwrap_or(Value::Null),
        )
        .map_err(|_| SyncError::Storage("cloud inventory reference absent".into()))?;
        sealed.validate(account, operation, kind)?;
        if sealed != reference {
            return Err(SyncError::Storage(
                "cloud inventory seal differs from its exact pages".into(),
            ));
        }
        Ok(sealed)
    }
}

fn validate_pages(
    reference: &CloudInventoryRef,
    expected: &[Value],
    response: &Value,
) -> SyncResult<()> {
    let pages = response
        .get("pages")
        .and_then(Value::as_array)
        .filter(|pages| pages.len() == expected.len())
        .ok_or_else(|| SyncError::Storage("cloud inventory page receipts differ".into()))?;
    let directory = reference
        .key
        .strip_suffix(".json")
        .ok_or_else(|| SyncError::Storage("invalid cloud inventory key".into()))?;
    for (spec, receipt) in expected.iter().zip(pages) {
        let index = spec["page_index"].as_u64().unwrap_or(u64::MAX);
        let sha = spec["sha256"].as_str().unwrap_or("");
        let key = format!("{directory}/pages/{index:06}-{sha}.bin");
        if receipt["page_index"] != spec["page_index"]
            || receipt["sha256"] != spec["sha256"]
            || receipt["bytes"] != spec["bytes"]
            || receipt["key"].as_str() != Some(key.as_str())
        {
            return Err(SyncError::Storage(
                "cloud inventory page identity differs".into(),
            ));
        }
    }
    Ok(())
}
