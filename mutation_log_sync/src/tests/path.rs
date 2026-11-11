use super::*;
use crate::SharedSyncPath;
use mutation_log_mem::MutationLogConfig;

#[test]
fn disabled_config_keeps_the_existing_worker_and_builds_no_log() {
    let config = MutationLogConfig::from_value(Some("off"));
    let mut built = false;
    let path = crate::select_path(config.is_enabled(), || {
        built = true;
    });
    assert!(!built);
    assert_eq!(path, SharedSyncPath::Existing);
}

#[test]
fn enabled_config_builds_one_log() {
    let config = MutationLogConfig::from_value(Some("1"));
    let mut built = false;
    let path = crate::select_path(config.is_enabled(), || {
        built = true;
        personal_log()
    });
    assert!(built);
    assert!(matches!(path, SharedSyncPath::Log(_)));
}
