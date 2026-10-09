use std::path::PathBuf;

use lastdb_docstore::{collections, DocumentStore, LastStoreEngine, PlainCodec, TxnOp};

fn main() -> Result<(), String> {
    let home = smoke_home()?;
    std::fs::create_dir_all(&home)
        .map_err(|e| format!("create smoke home {}: {e}", home.display()))?;

    let store = LastStoreEngine::open(&home, Box::new(PlainCodec))
        .map_err(|e| format!("open docstore {}: {e}", home.display()))?;

    store
        .put(collections::ATOMS, "atom:001", br#"{"body":"one"}"#)
        .map_err(|e| format!("put atom: {e}"))?;
    let got = store
        .get(collections::ATOMS, "atom:001")
        .map_err(|e| format!("get atom: {e}"))?
        .ok_or_else(|| "atom:001 missing after put".to_string())?;
    if got != br#"{"body":"one"}"# {
        return Err(format!(
            "atom:001 body mismatch: {}",
            String::from_utf8_lossy(&got)
        ));
    }

    store
        .transaction(vec![
            TxnOp::put(collections::TIPS, "tip:001", b"atom:001".to_vec()),
            TxnOp::put(collections::TIPS, "tip:002", b"atom:002".to_vec()),
        ])
        .map_err(|e| format!("transaction: {e}"))?;

    let tip_keys = store
        .list_prefix_keys(collections::TIPS, "tip:")
        .map_err(|e| format!("list tips: {e}"))?;
    if tip_keys != ["tip:001".to_string(), "tip:002".to_string()] {
        return Err(format!("tip list mismatch: {tip_keys:?}"));
    }

    store
        .delete(collections::TIPS, "tip:002")
        .map_err(|e| format!("delete tip: {e}"))?;
    if store
        .exists(collections::TIPS, "tip:002")
        .map_err(|e| format!("exists deleted tip: {e}"))?
    {
        return Err("tip:002 still exists after delete".to_string());
    }

    let blob_ref = store
        .put_blob(b"storage-v2 smoke blob")
        .map_err(|e| format!("put blob: {e}"))?;
    let blob = store
        .get_blob(&blob_ref)
        .map_err(|e| format!("get blob: {e}"))?
        .ok_or_else(|| format!("missing blob {blob_ref}"))?;
    if blob != b"storage-v2 smoke blob" {
        return Err("blob body mismatch".to_string());
    }

    println!("storage-v2 docstore smoke OK home={}", home.display());
    Ok(())
}

fn smoke_home() -> Result<PathBuf, String> {
    let mut args = std::env::args_os();
    let _bin = args.next();
    if let Some(path) = args.next() {
        if args.next().is_some() {
            return Err("usage: lastdb_docstore_smoke [home]".to_string());
        }
        return Ok(PathBuf::from(path));
    }

    let ts = fold_db::clock::unix_nanos_wide();
    Ok(std::env::temp_dir().join(format!("lastdb-docstore-smoke-{}-{ts}", std::process::id())))
}
