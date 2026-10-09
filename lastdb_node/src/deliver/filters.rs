use super::*;

use super::types::*;

pub(super) fn resolve_filter(
    explicit: Option<HashRangeFilter>,
    max_records: Option<usize>,
) -> Option<HashRangeFilter> {
    if explicit.is_some() {
        return explicit;
    }
    max_records.map(HashRangeFilter::SampleN)
}

pub(super) fn parse_since_duration_secs(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("since must not be empty".into());
    }
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (amount, unit) = trimmed.split_at(split_at);
    let amount: u64 = amount
        .parse()
        .map_err(|_| format!("invalid since duration '{raw}'"))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 60 * 60,
        "d" | "day" | "days" => 24 * 60 * 60,
        _ => return Err(format!("invalid since duration unit in '{raw}'")),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("since duration '{raw}' is too large"))
}

pub(super) fn resolve_order_by(
    order_by: Option<&StageOrderBy>,
    order: Option<SortOrder>,
) -> Result<Option<QueryOrderBy>, String> {
    match order_by {
        None => Ok(None),
        Some(StageOrderBy::Field(field)) if field.trim().is_empty() => {
            Err("order_by field must not be empty".into())
        }
        Some(StageOrderBy::Field(field)) => Ok(Some(QueryOrderBy {
            field: field.clone(),
            order,
        })),
        Some(StageOrderBy::Query(query_order)) => {
            if order.is_some() && query_order.order.is_some() && order != query_order.order {
                return Err("order conflicts with order_by.order".into());
            }
            let mut resolved = query_order.clone();
            if resolved.order.is_none() {
                resolved.order = order;
            }
            Ok(Some(resolved))
        }
    }
}

pub(super) fn stage_predicates(
    base: Option<&[FieldPredicate]>,
    since: Option<&str>,
    since_field: Option<&str>,
    columns_include: Option<&[String]>,
    columns_exclude: Option<&[String]>,
) -> Result<Option<Vec<FieldPredicate>>, String> {
    let mut predicates = base.map_or_else(Vec::new, <[FieldPredicate]>::to_vec);
    if let Some(raw_since) = since {
        let duration_secs = parse_since_duration_secs(raw_since)?;
        let threshold = unix_secs().saturating_sub(duration_secs);
        let field = since_field.unwrap_or("updated_at").trim();
        if field.is_empty() {
            return Err("since_field must not be empty".into());
        }
        predicates.push(FieldPredicate::After {
            field: field.to_string(),
            instant: Value::Number(threshold.into()),
        });
    }
    if let Some(columns) = columns_include.filter(|c| !c.is_empty()) {
        predicates.push(FieldPredicate::In {
            field: "column".to_string(),
            values: columns.iter().cloned().map(Value::String).collect(),
        });
    }
    if let Some(columns) = columns_exclude.filter(|c| !c.is_empty()) {
        let values: HashSet<&str> = columns.iter().map(String::as_str).collect();
        let allowed = ["backlog", "todo", "doing", "review", "done"]
            .into_iter()
            .filter(|column| !values.contains(column))
            .map(|column| Value::String(column.to_string()))
            .collect();
        predicates.push(FieldPredicate::In {
            field: "column".to_string(),
            values: allowed,
        });
    }
    Ok((!predicates.is_empty()).then_some(predicates))
}

pub(super) fn two_pass_limit(
    predicates: Option<&[FieldPredicate]>,
    order_by: Option<&QueryOrderBy>,
    predicate_limit: Option<usize>,
    max_records: Option<usize>,
) -> Option<usize> {
    predicate_limit.or_else(|| {
        (predicates.is_some_and(|p| !p.is_empty()) || order_by.is_some()).then_some(max_records)?
    })
}

pub(super) fn leg_filter(
    explicit: Option<HashRangeFilter>,
    max_records: Option<usize>,
    predicates: Option<&[FieldPredicate]>,
    order_by: Option<&QueryOrderBy>,
    predicate_limit: Option<usize>,
) -> Option<HashRangeFilter> {
    if predicates.is_some_and(|p| !p.is_empty()) || order_by.is_some() || predicate_limit.is_some()
    {
        return explicit;
    }
    resolve_filter(explicit, max_records)
}
