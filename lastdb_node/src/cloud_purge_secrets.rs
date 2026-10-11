//! Private operation capabilities live in LastSecrets, never in job receipts.

use async_trait::async_trait;
use fold_db::sync::cloud_purge::{CloudPurgeCapability, CloudPurgeCapabilityStore};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub(crate) struct LastSecretsCapabilities;

fn secret_slug(operation_id: &str) -> Result<String, String> {
    uuid::Uuid::parse_str(operation_id).map_err(|_| "invalid purge operation ID")?;
    Ok(format!("lastdb-cloud-purge-{operation_id}"))
}

#[async_trait]
impl CloudPurgeCapabilityStore for LastSecretsCapabilities {
    async fn put(&self, operation_id: &str, value: &str) -> Result<String, String> {
        let slug = secret_slug(operation_id)?;
        let mut child = tokio::process::Command::new("lastsecrets")
            .args([
                "put",
                &slug,
                "--label",
                "LastDB cloud purge operation",
                "--provider",
                "lastdb",
                "--purpose",
                "selected-device cloud replacement",
                "--env",
                "local",
                "--value-stdin",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| "LastSecrets is unavailable; no cloud hold was created")?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or("LastSecrets input is unavailable")?;
        stdin
            .write_all(value.as_bytes())
            .await
            .map_err(|_| "LastSecrets write failed")?;
        stdin
            .shutdown()
            .await
            .map_err(|_| "LastSecrets input close failed")?;
        drop(stdin);
        let result = tokio::time::timeout(Duration::from_secs(30), child.wait())
            .await
            .map_err(|_| "LastSecrets write timed out")?
            .map_err(|_| "LastSecrets write failed")?;
        if !result.success() {
            return Err("LastSecrets did not store the operation capability".into());
        }
        Ok(format!("lastsecrets://{slug}"))
    }

    async fn get(&self, locator: &str) -> Result<CloudPurgeCapability, String> {
        let slug = locator
            .strip_prefix("lastsecrets://")
            .ok_or("invalid capability locator")?;
        let operation_id = slug
            .strip_prefix("lastdb-cloud-purge-")
            .ok_or("invalid capability locator")?;
        if slug != secret_slug(operation_id)? {
            return Err("invalid capability locator".into());
        }
        let child = tokio::process::Command::new("lastsecrets")
            .args(["get", slug])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| "LastSecrets is unavailable; the job remains held")?;
        let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
            .await
            .map_err(|_| "LastSecrets read timed out")?
            .map_err(|_| "LastSecrets read failed")?;
        if !output.status.success() {
            return Err("LastSecrets could not read the operation capability".into());
        }
        let raw = CloudPurgeCapability::new(
            String::from_utf8(output.stdout)
                .map_err(|_| "LastSecrets returned invalid capability bytes")?,
        );
        Ok(CloudPurgeCapability::new(raw.trim().to_string()))
    }
}
