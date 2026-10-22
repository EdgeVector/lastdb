//! Charge JSON allocation shapes before collection growth. Raw input is separately capped.
use super::{invalid, StorageResult};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;

const HEAP_MAX: usize = 32 * 1024 * 1024;
struct Seed<'a> {
    left: &'a mut usize,
}
impl Seed<'_> {
    fn charge<E: de::Error>(&mut self, n: usize) -> std::result::Result<(), E> {
        *self.left = self
            .left
            .checked_sub(n)
            .ok_or_else(|| E::custom("restore JSON allocation cap"))?;
        Ok(())
    }
}
pub(super) fn decode(bytes: &[u8]) -> StorageResult<Value> {
    let mut left = HEAP_MAX;
    let mut reader = serde_json::Deserializer::from_slice(bytes);
    let value = Seed { left: &mut left }
        .deserialize(&mut reader)
        .map_err(|_| invalid("decoded atom JSON allocation cap"))?;
    reader.end().map_err(|_| invalid("atom JSON suffix"))?;
    Ok(value)
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(
        mut self,
        d: D,
    ) -> std::result::Result<Value, D::Error> {
        self.charge::<D::Error>(std::mem::size_of::<Value>())?;
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("bounded JSON")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Value, E> {
        Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(|| E::custom("invalid number"))
    }
    fn visit_str<E: de::Error>(mut self, v: &str) -> std::result::Result<Value, E> {
        self.charge::<E>(v.len())?;
        Ok(Value::String(v.to_owned()))
    }
    fn visit_string<E: de::Error>(mut self, v: String) -> std::result::Result<Value, E> {
        self.charge::<E>(v.capacity())?;
        Ok(Value::String(v))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<Value, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = a.next_element_seed(Seed { left: self.left })? {
            if out.len() == out.capacity() {
                let next = out
                    .capacity()
                    .max(4)
                    .checked_mul(2)
                    .ok_or_else(|| de::Error::custom("JSON capacity"))?;
                let extra = next - out.capacity();
                Seed { left: self.left }.charge::<A::Error>(
                    extra
                        .checked_mul(std::mem::size_of::<Value>())
                        .ok_or_else(|| de::Error::custom("JSON capacity"))?,
                )?;
                out.try_reserve_exact(extra)
                    .map_err(|_| de::Error::custom("JSON capacity"))?;
            }
            out.push(v);
        }
        Ok(Value::Array(out))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> std::result::Result<Value, A::Error> {
        let mut out = Map::new();
        while let Some(k) = a.next_key::<String>()? {
            // Includes map node space and temporary metadata-map conversion at Atom decode.
            Seed { left: self.left }.charge::<A::Error>(
                k.capacity()
                    .checked_add(256)
                    .ok_or_else(|| de::Error::custom("JSON capacity"))?,
            )?;
            if out.contains_key(&k) {
                return Err(de::Error::custom("duplicate atom field"));
            }
            let value = a.next_value_seed(Seed { left: self.left })?;
            out.insert(k, value);
        }
        Ok(Value::Object(out))
    }
}
