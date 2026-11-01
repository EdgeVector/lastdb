//! Byte backup, mutation log, restore proof, and the native cloud work guard.
//! Backup V2 is a separate module. The disk home opens format v2 run files.

pub mod byte_backup;
mod cloud_work;
pub mod file_dependencies;
pub mod mutation_log;
pub mod run_restore;

pub use cloud_work::NativeCloudWorkGuard;
