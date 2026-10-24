use crate::ProtocolError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// This version accepts only the JSON integer 2; the V1 scalar stays unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version;
impl Version {
    pub const V2: Self = Self;
    #[must_use]
    pub const fn get(self) -> u8 {
        2
    }
}
impl Serialize for Version {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(2)
    }
}
impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match u8::deserialize(deserializer)? {
            2 => Ok(Self::V2),
            _ => Err(serde::de::Error::custom(ProtocolError::invalid(
                "v2.version",
            ))),
        }
    }
}
