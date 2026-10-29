//! Explicit feature activation on an exclusively owned, cleanly stopped home.

use std::path::Path;

pub(super) fn enable(home: &Path, json: bool, allow_primary: bool) -> Result<(), String> {
    if !allow_primary {
        lastdb_node::offline_home::refuse_primary(home)?;
    }
    let user = std::env::var_os("HOME").ok_or("HOME is unset")?;
    let archive = std::path::PathBuf::from(user).join(".primary-lastdb-archive-20261014");
    if home.starts_with(&archive) {
        return Err("format activation refuses the archived primary".into());
    }
    let home = home.canonicalize().map_err(|_| "home is absent")?;
    if home.starts_with(archive) {
        return Err("format activation refuses the archived primary".into());
    }
    let data = home.join("data");
    let (_preflight, _maintenance) =
        lastdb_node::offline_home::preflight_stopped_home(&home, &data)?;
    let (keys, _signer) = lastdb_node::offline_home::load_e2e_keys(&home)?;
    let key = zeroize::Zeroizing::new(keys.encryption_key());
    let result = fold_db::storage::disk_pack::format_upgrade::enable_alternate_keys(&data, &key)
        .map_err(|error| error.to_string())?;
    let text = serde_json::to_string_pretty(&result).map_err(|_| "cannot encode format result")?;
    if json {
        println!("{text}");
    } else {
        println!("Alternate keys enabled. Existing data files stay unchanged.\n{text}");
    }
    Ok(())
}
