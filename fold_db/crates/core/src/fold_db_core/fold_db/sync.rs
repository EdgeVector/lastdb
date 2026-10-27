//! Passive legacy status and local maintenance. No cloud engine can attach.
use super::FoldDB;
impl FoldDB {
    pub const CLOUD_SYNC_DISABLED: &'static str =
        "legacy cloud sync is retired; use byte backup V2";
}
