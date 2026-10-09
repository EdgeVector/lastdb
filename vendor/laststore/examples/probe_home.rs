use laststore::{LastStore, LastStoreOptions};
use std::env;
fn main() {
    let path = env::args().nth(1).expect("path");
    let s = LastStore::open_existing_or_with(&path, LastStoreOptions::default()).unwrap();
    println!(
        "layout={:?} warm={}",
        s.options().layout_mode,
        s.options().hash_group_warm_bytes
    );
    for coll in ["schemas", "metadata", "node_config", "change_feed", "atoms"] {
        let keys = s.list_prefix_keys(coll, "").unwrap();
        println!("{coll}: n_keys={}", keys.len());
        if let Some(k) = keys.first() {
            let v = s.get(coll, k).unwrap();
            let len = v.as_ref().map(|b| b.len()).unwrap_or(0);
            let prefix = v
                .as_ref()
                .map(|b| String::from_utf8_lossy(&b[..b.len().min(80)]).to_string())
                .unwrap_or_default();
            println!("  first_key={k} body_len={len} prefix={prefix:?}");
        }
    }
    // empty bodies?
    let mut empty = 0u64;
    for id in s
        .list_prefix_keys("schemas", "")
        .unwrap()
        .into_iter()
        .take(50)
    {
        if s.get("schemas", &id)
            .unwrap()
            .map(|b| b.is_empty())
            .unwrap_or(true)
        {
            empty += 1;
            println!("EMPTY schema id={id}");
        }
    }
    println!("empty_schemas_sample={empty}");
}
