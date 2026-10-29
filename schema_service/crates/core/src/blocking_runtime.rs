//! Dedicated-thread runtime port.
//!
//! Core does not construct a runtime. The shell passes an adapter into the
//! state constructors. The process adapter runs each task on a fresh
//! current-thread runtime on a named thread.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;

use schema_types::{FoldDbError, FoldDbResult};

/// Output of a blocking task. Callers downcast it back to the concrete type.
pub type BlockingOutput = Box<dyn Any + Send>;

/// Work the shell runs to completion off the caller's thread.
pub type BlockingTask = Pin<Box<dyn Future<Output = FoldDbResult<BlockingOutput>> + Send>>;

/// Why a dedicated-thread run did not finish.
#[derive(Debug)]
pub enum BlockingRuntimeError {
    /// The thread could not be started.
    Spawn(std::io::Error),
    /// The runtime could not be built.
    Build(std::io::Error),
    /// The thread ended without sending a result.
    Dropped,
}

/// Runs async registry work on a dedicated thread.
///
/// Implementations must not drive the future on the caller's thread. State
/// init and snapshot import rely on that so they can be called from inside
/// an existing runtime.
pub trait BlockingRuntime: Send + Sync {
    fn block_on_dedicated(
        &self,
        thread_name: &'static str,
        future: BlockingTask,
    ) -> Result<FoldDbResult<BlockingOutput>, BlockingRuntimeError>;
}

pub(crate) fn drive(
    runtime: &dyn BlockingRuntime,
    thread_name: &'static str,
    build: &'static str,
    spawn: &'static str,
    dropped: &'static str,
    future: BlockingTask,
) -> FoldDbResult<BlockingOutput> {
    match runtime.block_on_dedicated(thread_name, future) {
        Ok(result) => result,
        Err(err) => Err(config_error(err, build, spawn, dropped)),
    }
}

pub(crate) fn downcast<T: Send + 'static>(value: BlockingOutput) -> T {
    *value.downcast::<T>().expect("blocking runtime output type")
}

fn config_error(
    err: BlockingRuntimeError,
    build: &'static str,
    spawn: &'static str,
    dropped: &'static str,
) -> FoldDbError {
    match err {
        BlockingRuntimeError::Build(error) => FoldDbError::Config(format!("{build}: {error}")),
        BlockingRuntimeError::Spawn(error) => FoldDbError::Config(format!("{spawn}: {error}")),
        BlockingRuntimeError::Dropped => FoldDbError::Config(dropped.to_string()),
    }
}
