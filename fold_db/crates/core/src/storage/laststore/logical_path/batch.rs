//! All-or-restore batch writes through the logical path.

use super::*;

/// One op in a batch that restores prior bodies when a later op fails.
pub(in crate::storage::laststore) enum BatchOp {
    Put {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        collection: String,
        key: Vec<u8>,
    },
}

pub(super) enum UndoOp {
    Put {
        collection: String,
        key: Vec<u8>,
        previous: Option<Vec<u8>>,
    },
    Delete {
        collection: String,
        key: Vec<u8>,
        previous: Option<Vec<u8>>,
    },
}

/// Apply each op, restore prior bodies when a later op fails, and sync only
/// the groups this batch wrote.
pub(in crate::storage::laststore) fn apply_batch(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    ops: Vec<BatchOp>,
    deferred: bool,
) -> StorageResult<()> {
    let mut applied = Vec::with_capacity(ops.len());
    let mut scope: Vec<ShardKey> = Vec::with_capacity(ops.len());
    for op in ops {
        let result = apply_one(store, set, op, &mut applied, &mut scope);
        if let Err(error) = result {
            return Err(undo_or(store, set, applied, error));
        }
    }
    scope.sort();
    scope.dedup();
    if !deferred {
        if let Err(error) = store
            .flush_scope(&scope)
            .map_err(LastStoreKvStore::map_error)
        {
            return Err(undo_or(store, set, applied, error));
        }
        apply_durable_through(store, set);
    }
    Ok(())
}

pub(super) fn apply_one(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    op: BatchOp,
    applied: &mut Vec<UndoOp>,
    scope: &mut Vec<ShardKey>,
) -> StorageResult<()> {
    match op {
        BatchOp::Put {
            collection,
            key,
            value,
        } => {
            let previous = write_put(store, set, &collection, &key, &value)?;
            scope.push(store.shard_key_of(&collection, &LastStoreKvStore::encode_key(&key)));
            applied.push(UndoOp::Put {
                collection,
                key,
                previous,
            });
            Ok(())
        }
        BatchOp::Delete { collection, key } => {
            let (_, previous) = write_delete(store, set, &collection, &key)?;
            scope.push(store.shard_key_of(&collection, &LastStoreKvStore::encode_key(&key)));
            applied.push(UndoOp::Delete {
                collection,
                key,
                previous,
            });
            Ok(())
        }
    }
}

/// Restore the applied ops. When an undo write fails, the batch is partly
/// applied, so return that error in place of the op error and log the op error.
pub(super) fn undo_or(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    applied: Vec<UndoOp>,
    error: StorageError,
) -> StorageError {
    match restore_applied(store, set, applied) {
        Ok(()) => error,
        Err(undo) => {
            tracing::error!(
                batch_error = %error,
                undo_error = %undo,
                "batch undo failed; the batch is partly applied"
            );
            undo
        }
    }
}

/// Undo every applied op, newest first. Every undo runs; the first undo error
/// is returned.
pub(super) fn restore_applied(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    applied: Vec<UndoOp>,
) -> StorageResult<()> {
    let mut first_error = None;
    for op in applied.into_iter().rev() {
        let result = match op {
            UndoOp::Put {
                collection,
                key,
                previous: Some(body),
            }
            | UndoOp::Delete {
                collection,
                key,
                previous: Some(body),
            } => put(store, set, &collection, &key, &body).map(|()| {
                // No admit follows an undo write, so drop its pending entry.
                set.lock().expect("poison").clear_write_tokens(&key);
            }),
            UndoOp::Put {
                collection,
                key,
                previous: None,
            } => delete(store, set, &collection, &key).map(|_| ()),
            UndoOp::Delete { previous: None, .. } => Ok(()),
        };
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}
