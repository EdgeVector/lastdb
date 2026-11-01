//! Owner-only field policy, history lookup, purge, and conditional restore.
use super::*;
use fold_db::db_operations::atom_store::atom_history::{FieldHistoryPolicy, HistoryLocation};
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

pub(super) async fn execute_atom_history_route(
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
    let scope = ctx.storage_prefix.as_deref();
    request.location.schema = handlers::resolve_schema_name(host, &request.location.schema)?
        .ok_or_else(|| HostError::new(404, "history schema does not exist"))?;
    let db = host.db.db_ops();
    // A format 7 home keeps copies of old values at record history keys.
    if db.atoms().record_home() {
        return records::execute(host, ctx, request).await;
    }
    let target = db.atom_history_target(&request.location).await?;
    let atoms = db.atoms();
    // Policies belong to a field. Only a data action needs key coordinates.
    let slot = atoms.atom_history_slot(&target.molecule, &request.location.key, scope);
    match request.action {
        Action::Policy { policy } => {
            if let Some(policy) = policy {
                db.configure_atom_history(&target, policy, scope).await?;
            }
            let policy = atoms
                .field_history_policy(&target.field_identity, scope)
                .await?;
            Ok(serde_json::json!({"field_identity": target.field_identity, "policy": policy}))
        }
        Action::List { cursor, limit } => {
            let page = atoms
                .atom_history_page(&slot.await?, cursor.as_deref(), limit.unwrap_or(32))
                .await?;
            serde_json::to_value(page).map_err(|e| HostError::new(500, e.to_string()))
        }
        Action::Get { id } => atoms
            .atom_history_value(&slot.await?, &id)
            .await?
            .ok_or_else(|| HostError::new(404, "history entry is absent or expired")),
        Action::Purge { cursor } => Ok(atoms
            .purge_atom_history(&slot.await?, cursor.as_deref())
            .await?),
        Action::Restore {
            id,
            expected_mutation,
            absent,
        } => {
            require_restore_guard(expected_mutation.as_deref(), absent)?;
            let value = atoms
                .atom_history_value(&slot.await?, &id)
                .await?
                .ok_or_else(|| HostError::new(404, "history entry is absent or expired"))?;
            restore(host, ctx, request.location, &value, expected_mutation).await
        }
    }
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
