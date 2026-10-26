//! Key-list request rules. No host, schema lookup, or storage access.

use lastdb_host::wire::query_value;

const LIST_RECORD_KEYS_DEFAULT_LIMIT: usize = 100;
const LIST_RECORD_KEYS_MAX_LIMIT: usize = 1000;

pub(super) struct ListRecordKeysRequest {
    pub requested: String,
    pub limit: usize,
    pub cursor: Option<String>,
    pub hash_filter: Option<String>,
}

impl ListRecordKeysRequest {
    pub(super) fn parse(target: &str) -> Result<Self, &'static str> {
        let requested = query_value(target, "schema")
            .filter(|schema| !schema.is_empty())
            .ok_or("schema query parameter is required")?;
        let limit = match query_value(target, "limit") {
            None => LIST_RECORD_KEYS_DEFAULT_LIMIT,
            Some(raw) => match raw.parse::<usize>() {
                Ok(0) => return Err("limit must be at least 1"),
                Ok(limit) => limit.min(LIST_RECORD_KEYS_MAX_LIMIT),
                Err(_) => return Err("limit must be a positive integer"),
            },
        };
        Ok(Self {
            requested,
            limit,
            cursor: query_value(target, "cursor").filter(|cursor| !cursor.is_empty()),
            hash_filter: query_value(target, "hash"),
        })
    }
}
