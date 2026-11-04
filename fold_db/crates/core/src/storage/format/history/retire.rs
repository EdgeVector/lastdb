//! The queue directory delete is minted only after its token still matches.

use super::{invalid, queue, EncodedHistory};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::{KvMutation, KvStore};

pub(crate) async fn retire_bucket(
    main_store: &dyn KvStore,
    runtime: &PackRuntime,
    key: String,
    expected: Vec<u8>,
) -> Result<bool, SchemaError> {
    if !transaction::publication::active(runtime) {
        return Err(invalid(
            "a history retirement requires a complete native write scope",
        ));
    }
    let bucket = queue()
        .directory_bucket(&key)
        .filter(|bucket| queue().bucket_of(*bucket) == *bucket)
        .ok_or_else(|| invalid("the retired key is not a history bucket"))?;
    if queue().directory_key(bucket) != key {
        return Err(invalid("the retired history bucket key is malformed"));
    }
    let mut stored = main_store
        .get_many(vec![key.as_bytes().to_vec()])
        .await
        .map_err(|error| SchemaError::InvalidData(format!("read guarded keys: {error}")))?;
    if stored.len() != 1 {
        return Err(invalid("the history bucket read is incomplete"));
    }
    if stored.pop().flatten().as_deref() != Some(expected.as_slice()) {
        return Ok(false);
    }
    EncodedHistory {
        rows: vec![KvMutation::delete(key)],
    }
    .stage(main_store, runtime)
    .await
    .map_err(|error| SchemaError::InvalidData(format!("commit rows: {error}")))?;
    Ok(true)
}
