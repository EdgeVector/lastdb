//! Dedicated-thread Tokio adapter for schema service shells.
//!
//! Each process entry constructs one [`DedicatedThreadRuntime`] and passes it
//! into `SchemaServiceState`. The adapter builds a fresh current-thread
//! runtime with every driver enabled, on a named thread, then drops that
//! runtime when the task finishes. That is the same schedule state init and
//! snapshot import used before the port.

use std::future::Future;

use schema_service_core::{BlockingOutput, BlockingRuntime, BlockingRuntimeError, BlockingTask};
use schema_types::FoldDbResult;

/// Process runtime. Construct this at the shell entrypoint.
#[derive(Clone, Copy, Debug)]
pub struct DedicatedThreadRuntime;

impl BlockingRuntime for DedicatedThreadRuntime {
    fn block_on_dedicated(
        &self,
        thread_name: &'static str,
        future: BlockingTask,
    ) -> Result<FoldDbResult<BlockingOutput>, BlockingRuntimeError> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                let _ = tx.send(run_current_thread(future));
            })
            .map_err(BlockingRuntimeError::Spawn)?;
        match rx.recv() {
            Ok(result) => result,
            Err(_) => Err(BlockingRuntimeError::Dropped),
        }
    }
}

fn run_current_thread(
    future: BlockingTask,
) -> Result<FoldDbResult<BlockingOutput>, BlockingRuntimeError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(BlockingRuntimeError::Build)?;
    Ok(runtime.block_on(future))
}

/// Drive `future` on a fresh current-thread runtime on this thread.
///
/// Canonical property tests use this so the core crate does not construct a
/// runtime. Callers that may already be inside a runtime use
/// [`DedicatedThreadRuntime`] instead.
pub fn block_on_current_thread<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}
