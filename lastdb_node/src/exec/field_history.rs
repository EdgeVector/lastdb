//! Owner-only field policy, history lookup, purge, and conditional restore.
use super::*;
use fold_db::db_operations::record_store::field_history::{FieldHistoryPolicy, HistoryLocation};
use fold_db::schema::types::operations::Operation;
use fold_db::schema::types::MutationType;

mod records;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    location: HistoryLocation,
    action: Action,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Policy {
        policy: Option<FieldHistoryPolicy>,
    },
    List {
        cursor: Option<String>,
        limit: Option<usize>,
    },
    Get {
        id: String,
    },
    Purge {
        cursor: Option<String>,
    },
    Restore {
        id: String,
        mutation: Operation,
    },
}

pub(super) async fn execute_field_history_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match serde_json::from_slice::<Request>(&req.body) {
        Ok(request) => request,
        Err(error) => return Reject::from_serde_error(&error, None).response(),
    };
    render(execute(host, request).await, ctx)
}

async fn execute(host: &Host, mut request: Request) -> Result<Value, HostError> {
    request.location.schema = handlers::resolve_schema_name(host, &request.location.schema)?
        .ok_or_else(|| HostError::new(404, "history schema does not exist"))?;
    // A home keeps copies of old values at record history keys.
    records::execute(host, request).await
}

/// Restore the archived value only as part of the caller's complete record.
async fn restore(
    host: &Host,
    location: HistoryLocation,
    value: &Value,
    mutation: Operation,
) -> Result<Value, HostError> {
    let mut components = handlers::MutationComponents::from_operation(mutation, None);
    let canonical = handlers::resolve_schema_name(host, &components.schema)?
        .ok_or_else(|| HostError::new(404, "restore schema does not exist"))?;
    if canonical != location.schema
        || components.key_value != location.key
        || !matches!(
            components.mutation_type,
            MutationType::Create | MutationType::Replace
        )
        || components.record_fields.get(&location.field) != value.get("value")
    {
        return Err(HostError::new(
            400,
            "restore requires the complete target record containing the archived field value",
        ));
    }
    components.schema = canonical;
    handlers::execute_mutation(host, components).await
}
