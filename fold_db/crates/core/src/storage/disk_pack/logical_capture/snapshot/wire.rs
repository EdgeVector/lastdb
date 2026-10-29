//! Parse typed fields directly, without internally tagged enum Content buffers.

use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Disabled,
    Enabled,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    state: State,
    map: Option<SnapshotInclusionMap>,
}

pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<SnapshotInclusion, D::Error> {
    let wire = Wire::deserialize(deserializer)?;
    match (wire.state, wire.map) {
        (State::Disabled, None) => Ok(SnapshotInclusion::Disabled),
        (State::Enabled, Some(map)) => Ok(SnapshotInclusion::Enabled { map }),
        _ => Err(serde::de::Error::custom("snapshot inclusion state differs")),
    }
}
