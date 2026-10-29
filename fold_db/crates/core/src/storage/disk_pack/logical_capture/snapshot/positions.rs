//! Do not retain more device entries than the snapshot contract permits.

use super::*;

pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<SnapshotDevicePosition>, D::Error> {
    struct Positions;

    impl<'de> serde::de::Visitor<'de> for Positions {
        type Value = Vec<SnapshotDevicePosition>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 33 snapshot device positions")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut positions = Vec::with_capacity(MAX_DEVICES);
            for _ in 0..MAX_DEVICES {
                match sequence.next_element()? {
                    Some(position) => positions.push(position),
                    None => return Ok(positions),
                }
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("snapshot device count exceeds 33"));
            }
            Ok(positions)
        }
    }

    deserializer.deserialize_seq(Positions)
}
