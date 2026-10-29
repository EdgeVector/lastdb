//! Optional query reply shapes. The read, access rules and integrity checks stay shared.

use serde::Deserialize;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Which query reply shape the current task should build.
pub enum QueryResponseFormat {
    #[default]
    /// Rows include the full field metadata.
    Full,
    /// Rows keep a molecule id per field, then fold that into one conflict flag.
    Values,
    /// Return a count for the keyed scope and do not build row metadata.
    Count,
}

tokio::task_local! {
    static FORMAT: QueryResponseFormat;
}

impl QueryResponseFormat {
    /// Scope one query item without changing the format of concurrent batch items.
    pub async fn run<T>(self, future: impl std::future::Future<Output = T>) -> T {
        FORMAT.scope(self, future).await
    }

    pub(super) fn current() -> Self {
        FORMAT.try_with(|format| *format).unwrap_or_default()
    }
}

/// Keep one conservative row conflict flag after the field metadata leaves the reply.
pub(super) fn compact_rows(rows: &mut [serde_json::Value], coverage: &str) {
    use serde_json::Value;
    for row in rows {
        let Some(row) = row.as_object_mut() else {
            continue;
        };
        let metadata = row.remove("metadata");
        let flags = metadata
            .as_ref()
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|fields| fields.values())
            .filter_map(|field| field.get("has_conflicts"));
        let mut flag = if coverage == "known" {
            Value::Bool(false)
        } else {
            Value::String("unknown".into())
        };
        for value in flags {
            if value == &Value::Bool(true) {
                flag = Value::Bool(true);
                break;
            }
            if value.as_str() == Some("unknown") {
                flag = Value::String("unknown".into());
            }
        }
        row.insert("has_conflicts".into(), flag);
    }
}
