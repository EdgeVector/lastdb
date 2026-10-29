//! Optional query reply shapes. The read, access rules and integrity checks stay shared.

use serde::Deserialize;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryResponseFormat {
    #[default]
    Full,
    Values,
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
pub(super) fn compact_rows(rows: &mut [serde_json::Value]) {
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
        let mut flag = Value::Bool(false);
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
