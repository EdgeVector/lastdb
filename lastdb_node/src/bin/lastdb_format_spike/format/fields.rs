//! Nested format functions: supplied values in, one typed block out, no I/O.
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The enclosing format injects a matching encoder and decoder together.
#[derive(Serialize)]
#[serde(bound(serialize = ""))]
pub(super) struct FieldsCodec<T> {
    pub name: &'static str,
    pub version: u32,
    #[serde(skip)]
    pub encode: fn(u32, &Map<String, Value>) -> Result<T>,
    #[serde(skip)]
    pub decode: fn(u32, T) -> Result<Map<String, Value>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldsBlock {
    version: u32,
    values: Map<String, Value>,
}

pub(super) fn plain_encode(_: u32, values: &Map<String, Value>) -> Result<Map<String, Value>> {
    Ok(values.clone())
}

pub(super) fn plain_decode(_: u32, values: Map<String, Value>) -> Result<Map<String, Value>> {
    Ok(values)
}

pub(super) fn nested_encode(version: u32, values: &Map<String, Value>) -> Result<FieldsBlock> {
    Ok(FieldsBlock {
        version,
        values: values.clone(),
    })
}

pub(super) fn nested_decode(version: u32, block: FieldsBlock) -> Result<Map<String, Value>> {
    if block.version != version {
        return Err("field block version differs from the selected format".into());
    }
    Ok(block.values)
}
