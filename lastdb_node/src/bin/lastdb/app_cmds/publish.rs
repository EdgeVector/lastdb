//! Developer signing key setup for registry publication.

use super::*;

use lastdb_node::app_publish;

/// The default developer key path unless `--key-file` names one.
pub(crate) fn resolve_key_file(home: &Path, key_file: Option<PathBuf>) -> PathBuf {
    key_file.unwrap_or_else(|| app_publish::default_dev_key_path(home))
}

pub(crate) fn app_dev_init(home: &Path, key_file: Option<PathBuf>) -> Result<(), String> {
    let path = resolve_key_file(home, key_file);
    let pubkey = app_publish::generate_dev_key(&path)?;
    println!("dev signing key written to {}", path.display());
    println!("dev_pubkey: {pubkey}");
    Ok(())
}
