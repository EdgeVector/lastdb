//! Owner-only field policy, history lookup, purge, and conditional restore.
use super::*;
use fold_db::db_operations::record_store::field_history::{FieldHistoryPolicy, HistoryLocation};
use fold_db::schema::types::cas::CasExpectation;
use fold_db::schema::types::operations::{MutationConvergence, MutationDurability};
use fold_db::schema::types::MutationType;

mod records;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    location: HistoryLocation,
    #[serde(default)]
    database: Option<String>,
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
        expected_mutation: Option<String>,
        #[serde(default)]
        absent: bool,
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
    render(execute(host, ctx, request).await, ctx)
}

async fn execute(
    host: &Host,
    ctx: &AccessContext,
    mut request: Request,
) -> Result<Value, HostError> {
    let scoped = match &request.database {
        Some(db) => ctx
            .clone()
            .with_db_handle(Some(db))
            .map_err(|e| HostError::new(400, e))?,
        None => ctx.clone(),
    };
    let ctx = &scoped;
    request.location.schema = handlers::resolve_schema_name(host, &request.location.schema)?
        .ok_or_else(|| HostError::new(404, "history schema does not exist"))?;
    // A home keeps copies of old values at record history keys.
    records::execute(host, ctx, request).await
}

fn require_restore_guard(expected_mutation: Option<&str>, absent: bool) -> Result<(), HostError> {
    if absent == expected_mutation.is_some() {
        return Err(HostError::new(
            400,
            "restore requires expected_mutation or absent",
        ));
    }
    Ok(())
}

/// Write an archived value back through the native request, guarded by the
/// current mutation of the field or by its absence.
async fn restore(
    host: &Host,
    ctx: &AccessContext,
    location: HistoryLocation,
    value: &Value,
    expected_mutation: Option<String>,
) -> Result<Value, HostError> {
    let field = location.field.clone();
    let expected = match expected_mutation {
        Some(uuid) if !uuid.is_empty() => CasExpectation::Mutation {
            field: field.clone(),
            uuid,
        },
        Some(_) => return Err(HostError::new(400, "expected mutation must be nonempty")),
        None => CasExpectation::Absent {
            field: field.clone(),
        },
    };
    let components = handlers::MutationComponents {
        metadata: value
            .pointer("/metadata/metadata")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| HostError::new(500, e.to_string()))?,
        schema: location.schema,
        fields_and_values: [(field, value["value"].clone())].into_iter().collect(),
        key_value: location.key,
        mutation_type: MutationType::Update,
        source_file_name: value
            .pointer("/metadata/source_file_name")
            .and_then(Value::as_str)
            .map(str::to_string),
        expected: Some(expected),
        convergence: MutationConvergence::Sync,
        durability: MutationDurability::Durable,
        must_exist: None,
        key_range_prefix: None,
    };
    handlers::execute_mutation(host, components, ctx).await
}
