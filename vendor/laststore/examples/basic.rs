//! Minimal Last Store usage — multi-collection put / get / flush / compact.

use laststore::{collections, LastStore, TxnOp};
use std::env;
use std::path::PathBuf;

fn main() -> laststore::Result<()> {
    let home: PathBuf = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("laststore-basic-example"));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home)?;

    println!("Last Store {} → {}", laststore::VERSION, home.display());

    let store = LastStore::open(&home)?;

    store.put(collections::SCHEMAS, "note", br#"{"fields":["body"]}"#)?;
    store.put(collections::ATOMS, "a:1", br#"{"body":"hello"}"#)?;
    store.put(collections::TIPS, "mk:note:1", b"a:1")?;
    store.flush()?;

    let tip = store.get(collections::TIPS, "mk:note:1")?.expect("tip");
    let atom = store
        .get(collections::ATOMS, std::str::from_utf8(&tip).unwrap())?
        .expect("atom");
    println!("tip → atom: {}", String::from_utf8_lossy(&atom));

    // Multi-doc transaction (flushes at end)
    store.transaction(vec![
        TxnOp::put(collections::ATOMS, "a:2", br#"{"body":"world"}"#),
        TxnOp::put(collections::TIPS, "mk:note:2", b"a:2"),
    ])?;

    // Churn + compact reclaims space
    store.delete(collections::ATOMS, "a:1")?;
    store.delete(collections::TIPS, "mk:note:1")?;
    store.flush()?;
    store.compact()?;

    let keys = store.list_prefix(collections::ATOMS, "a:")?;
    println!("atoms after compact: {} live", keys.len());
    for (id, _) in keys {
        println!("  {id}");
    }

    Ok(())
}
