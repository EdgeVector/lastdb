//! Local snapshot frontier prerequisite; operator verifies cloud publication.

use super::*;
use fold_db::sync::engine::{
    decode_offline_pin_log_row, offline_pin_log_restore_frontier_key, OfflinePinLogRow,
    PIN_LOG_NAMESPACE,
};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    version: u32,
    #[serde(deserialize_with = "unique_writers")]
    by_writer: BTreeMap<String, u64>,
    #[serde(default)]
    mode: Option<fold_db::sync::engine::BackupRestoreMode>,
}

pub(super) async fn require(
    opened: &HomeStore,
    published: &BTreeMap<String, u64>,
) -> Result<BTreeMap<String, u64>, String> {
    let raw = opened
        .base
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(err)?;
    let key = offline_pin_log_restore_frontier_key();
    let bytes = raw
        .get(key)
        .await
        .map_err(err)?
        .ok_or("file blob reclaim requires a prior authoritative snapshot frontier")?;
    if !matches!(
        decode_offline_pin_log_row(key, &bytes)?,
        OfflinePinLogRow::RestoreFrontier
    ) {
        return Err("file blob snapshot frontier has a wrong key kind".into());
    }
    let marker: Marker = serde_json::from_slice(&bytes).map_err(err)?;
    if marker.version != 1
        || marker.mode.is_some()
        || marker.by_writer.is_empty()
        || &marker.by_writer != published
    {
        return Err("the normal snapshot writer map differs from the stopped published map".into());
    }
    Ok(marker.by_writer)
}

fn unique_writers<'de, D>(decoder: D) -> Result<BTreeMap<String, u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Unique;
    impl<'de> serde::de::Visitor<'de> for Unique {
        type Value = BTreeMap<String, u64>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("unique nonempty snapshot writers")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut writers = BTreeMap::new();
            while let Some((writer, frontier)) = map.next_entry::<String, u64>()? {
                if writer.trim().is_empty() || writers.insert(writer, frontier).is_some() {
                    return Err(serde::de::Error::custom(
                        "invalid or duplicate snapshot writer",
                    ));
                }
            }
            Ok(writers)
        }
    }
    decoder.deserialize_map(Unique)
}
