use super::{homes, require, Result};
use crate::db_operations::atom_store::counter::restore_validation;
use crate::storage::disk_pack::restore_safety;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde_json::{json, Value};
use std::path::Path;

pub(super) async fn run(root: &Path) -> Result<Value> {
    let home = homes::Home::open(&root.join("shared-files"))?;
    home.initialize().await?;
    let snapshot = homes::snapshot(root)?;
    snapshot.restore(home.logical.as_ref()).await?;
    let source = snapshot
        .namespaces
        .iter()
        .find(|namespace| namespace.name == "main")
        .ok_or("missing main")?;
    let mut rows = Vec::new();
    for entry in &source.entries {
        let key = BASE64.decode(&entry.key)?;
        if crate::storage::current_keys::counter_owned("main", &key)? {
            rows.push((key, BASE64.decode(&entry.value)?));
        }
    }
    let blob = format!("sha256:{}", "a".repeat(64));
    let mut altered = 0;
    for (key, value) in &mut rows {
        if altered < 2 && crate::atom::atom_key_codec::is_counter_key(std::str::from_utf8(key)?) {
            let mut counter: Value = serde_json::from_slice(value)?;
            counter["blobs"] = json!([blob]);
            *value = serde_json::to_vec(&counter)?;
            altered += 1;
        }
    }
    require(altered == 2, "generated source lacks two atom counters")?;
    let scope = format!("cloud:{}", "b".repeat(64));
    let scoped = rows.iter().map(|(key, value)| {
        (
            format!("{scope}:{}", String::from_utf8_lossy(key)).into_bytes(),
            value.clone(),
        )
    });
    let mut writes = rows.clone();
    writes.extend(scoped);
    let logical = home.logical.as_ref();
    restore_safety::exclusive(home.runtime(), async {
        let main = logical.open_namespace("main").await?;
        main.put(restore_safety::PENDING_KEY.as_bytes(), b"pending".to_vec())
            .await?;
        logical.restore_durability_barrier().await?;
        main.batch_put(writes).await?;
        let cas = logical.open_namespace("cas_blobs").await?;
        cas.put(blob.as_bytes(), b"generated file bytes".to_vec())
            .await?;
        let key = crate::db_operations::atom_store::blob_counter::blob_counter_key(&blob);
        let mut counter = json!({"v":1, "blob":blob, "refs":4});
        main.put(key.as_bytes(), serde_json::to_vec(&counter)?)
            .await?;
        logical.restore_durability_barrier().await?;
        restore_validation::validate(logical).await?;
        counter["refs"] = json!(2);
        main.put(key.as_bytes(), serde_json::to_vec(&counter)?)
            .await?;
        let error = restore_validation::validate(logical)
            .await
            .expect_err("global undercount was accepted");
        require(
            error.to_string().contains("global file references"),
            "global control failed before its exact cross-scope pass",
        )?;
        counter["refs"] = json!(4);
        main.put(key.as_bytes(), serde_json::to_vec(&counter)?)
            .await?;
        cas.delete(blob.as_bytes()).await?;
        require(
            restore_validation::validate(logical).await.is_err(),
            "missing referenced file was accepted",
        )?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await?;
    require(
        home.runtime().restore_validation_pending(),
        "invalid generated target was unblocked",
    )?;
    Ok(json!({"case":"shared-files", "passed":true, "scopes":2,
        "true_references":4, "valid_control_passed":true, "global_undercount_refused":true,
        "missing_referenced_file_refused":true, "target_stays_blocked":true}))
}
