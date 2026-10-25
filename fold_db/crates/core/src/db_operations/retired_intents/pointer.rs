//! One encrypted, exact-key pointer; no history census.

use super::*;

const POINTER_KEY: &[u8] = b"last_preserved:v1";
const MAX_POINTER_BYTES: usize = 128;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LastPreservedPointer {
    version: u8,
    source_id: String,
}

pub(super) async fn read_pointer(kv: &Arc<dyn KvStore>) -> Result<Option<String>, SchemaError> {
    let value = strict_values(kv, vec![POINTER_KEY.to_vec()])
        .await?
        .pop()
        .flatten();
    let Some(value) = value else {
        return Ok(None);
    };
    if value.len() > MAX_POINTER_BYTES {
        return Err(refused("retired pointer exceeds byte cap"));
    }
    let pointer: LastPreservedPointer =
        serde_json::from_slice(&value).map_err(|_| refused("retired pointer is unreadable"))?;
    if pointer.version != 1 || !exact_id(&pointer.source_id) {
        return Err(refused("retired pointer identity is invalid"));
    }
    Ok(Some(pointer.source_id))
}

/// Private ID for the native RAM-pipe reader, not public progress or logs.
pub async fn read_last_preserved_retired_source_id(
    store: &Arc<dyn NamespacedStore>,
) -> Result<Option<String>, SchemaError> {
    let kv = store
        .open_namespace(RETIRED_INTENT_NAMESPACE)
        .await
        .map_err(storage_error)?;
    let Some(id) = read_pointer(&kv).await? else {
        return Ok(None);
    };
    let raw = read_preserved_retired_intent(store, &id)
        .await?
        .ok_or_else(|| refused("retired pointer names an absent outcome"))?;
    let outcome = probe::decode_outcome(&raw, &id)?;
    if outcome.source_id != id {
        return Err(refused("retired pointer differs from its outcome"));
    }
    Ok(Some(id))
}

pub(super) fn exact_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
