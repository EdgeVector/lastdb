//! `lastdb ops`: print completed requests by caller and by schema, and the time
//! committed writes spent in each phase.

use std::fmt::Write;
use std::io::{self, Write as IoWrite};
use std::time::Duration;

use serde_json::Value;

use super::uds_client::{get_path, resolve_client_home_and_socket, split_http_200};

const TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn ops(
    data_dir: Option<std::path::PathBuf>,
    by_count: bool,
    json: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let raw = get_path(&socket, "/api/ops", TIMEOUT)?;
    let body = split_http_200(raw.as_bytes(), "ops")?;
    if json {
        IoWrite::write_all(&mut io::stdout(), body).map_err(|error| format!("ops: {error}"))?;
        return Ok(());
    }
    let value: Value =
        serde_json::from_slice(body).map_err(|error| format!("invalid ops JSON: {error}"))?;
    print_tables(&value, by_count)
}

fn print_tables(value: &Value, by_count: bool) -> Result<(), String> {
    let mut calls = call_lines(array(value, "calls")?)?;
    let mut schemas = schema_lines(array(value, "schemas")?)?;
    sort_calls(&mut calls, by_count);
    sort_schemas(&mut schemas, by_count);
    let call_rows: Vec<Vec<String>> = calls.iter().map(CallLine::cells).collect();
    write_table(
        &[
            "caller", "call", "count", "items", "total", "slowest", "refused",
        ],
        &call_rows,
        &[false, false, true, true, true, true, false],
    );
    println!();
    let schema_rows: Vec<Vec<String>> = schemas.iter().map(SchemaLine::cells).collect();
    write_table(
        &["caller", "schema", "op", "items", "total"],
        &schema_rows,
        &[false, false, false, true, true],
    );
    // A daemon older than this command sends no phases.
    if let Some(phases) = value.get("write_phases") {
        println!();
        print_write_phases(phases)?;
    }
    Ok(())
}

/// One line for each phase of `/v1/write`: committed writes, mean, total, and the slowest
/// single value of that phase.
fn print_write_phases(value: &Value) -> Result<(), String> {
    let writes = number(value, "writes")?;
    let mut rows = Vec::new();
    for phase in array(value, "phases")? {
        let total_ns = number(phase, "total_ns")?;
        let mean = total_ns
            .checked_div(writes)
            .map_or_else(|| "-".to_owned(), format_ns);
        rows.push(vec![
            text(phase, "phase")?.to_owned(),
            writes.to_string(),
            mean,
            format_ns(total_ns),
            format_ns(number(phase, "slowest_ns")?),
        ]);
    }
    write_table(
        &["write phase", "writes", "mean", "total", "slowest"],
        &rows,
        &[false, true, true, true, true],
    );
    Ok(())
}

struct CallLine {
    caller: String,
    client: String,
    call: String,
    count: u64,
    items: u64,
    total_ns: u64,
    slowest_ns: u64,
    refused: String,
}

impl CallLine {
    fn cells(&self) -> Vec<String> {
        vec![
            shown_caller(&self.caller, &self.client),
            self.call.clone(),
            self.count.to_string(),
            self.items.to_string(),
            format_ns(self.total_ns),
            format_ns(self.slowest_ns),
            self.refused.clone(),
        ]
    }
}

struct SchemaLine {
    caller: String,
    client: String,
    schema: String,
    op: String,
    items: u64,
    total_ns: u64,
}

impl SchemaLine {
    fn cells(&self) -> Vec<String> {
        vec![
            shown_caller(&self.caller, &self.client),
            self.schema.clone(),
            self.op.clone(),
            self.items.to_string(),
            format_ns(self.total_ns),
        ]
    }
}

fn call_lines(rows: &[Value]) -> Result<Vec<CallLine>, String> {
    rows.iter()
        .map(|row| {
            Ok(CallLine {
                caller: text(row, "caller")?.to_owned(),
                client: client_text(row)?,
                call: text(row, "call")?.to_owned(),
                count: number(row, "count")?,
                items: number(row, "items")?,
                total_ns: number(row, "total_ns")?,
                slowest_ns: number(row, "slowest_ns")?,
                refused: refused_text(row)?,
            })
        })
        .collect()
}

fn schema_lines(rows: &[Value]) -> Result<Vec<SchemaLine>, String> {
    rows.iter()
        .map(|row| {
            Ok(SchemaLine {
                caller: text(row, "caller")?.to_owned(),
                client: client_text(row)?,
                schema: text(row, "schema")?.to_owned(),
                op: text(row, "op")?.to_owned(),
                items: number(row, "items")?,
                total_ns: number(row, "total_ns")?,
            })
        })
        .collect()
}

fn sort_calls(rows: &mut [CallLine], by_count: bool) {
    rows.sort_by(|left, right| {
        let order = if by_count {
            right.count.cmp(&left.count)
        } else {
            right.total_ns.cmp(&left.total_ns)
        };
        order
            .then(if by_count {
                right.total_ns.cmp(&left.total_ns)
            } else {
                right.count.cmp(&left.count)
            })
            .then(left.caller.cmp(&right.caller))
            .then(left.client.cmp(&right.client))
            .then(left.call.cmp(&right.call))
    });
}

fn sort_schemas(rows: &mut [SchemaLine], by_count: bool) {
    rows.sort_by(|left, right| {
        let order = if by_count {
            right.items.cmp(&left.items)
        } else {
            right.total_ns.cmp(&left.total_ns)
        };
        order
            .then(if by_count {
                right.total_ns.cmp(&left.total_ns)
            } else {
                right.items.cmp(&left.items)
            })
            .then(left.caller.cmp(&right.caller))
            .then(left.client.cmp(&right.client))
            .then(left.schema.cmp(&right.schema))
            .then(left.op.cmp(&right.op))
    });
}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value], String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("ops response has no {key} array"))
}

fn text<'a>(row: &'a Value, key: &str) -> Result<&'a str, String> {
    row.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("ops row has no {key}"))
}

fn number(row: &Value, key: &str) -> Result<u64, String> {
    row.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("ops row has no {key}"))
}

fn client_text(row: &Value) -> Result<String, String> {
    match row.get("client") {
        Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.to_owned()),
        _ => Err("ops row has no client".to_owned()),
    }
}

fn refused_text(row: &Value) -> Result<String, String> {
    let object = row
        .get("refused")
        .and_then(Value::as_object)
        .ok_or_else(|| "ops row has no refused object".to_owned())?;
    let mut parts = Vec::new();
    for (code, count) in object {
        let count = count
            .as_u64()
            .ok_or_else(|| format!("ops refused {code} is not a count"))?;
        if count > 0 {
            parts.push(format!("{code} {count}"));
        }
    }
    parts.sort();
    Ok(if parts.is_empty() {
        "-".to_owned()
    } else {
        parts.join(", ")
    })
}

fn shown_caller(caller: &str, client: &str) -> String {
    if client.is_empty() {
        caller.to_owned()
    } else {
        format!("{caller} {client}")
    }
}

fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{}us", ns / 1_000)
    } else if ns < 1_000_000_000 {
        format!("{:.3}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.3}s", ns as f64 / 1_000_000_000.0)
    }
}

fn write_table(header: &[&str], rows: &[Vec<String>], right: &[bool]) {
    let widths: Vec<usize> = (0..header.len())
        .map(|index| {
            rows.iter().fold(header[index].len(), |width, row| {
                width.max(row[index].len())
            })
        })
        .collect();
    emit(header, &widths, right);
    for row in rows {
        emit(row, &widths, right);
    }
}

fn emit(cells: &[impl AsRef<str>], widths: &[usize], right: &[bool]) {
    let mut line = String::new();
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            line.push_str("  ");
        }
        let cell = cell.as_ref();
        let width = widths[index];
        let written = if right[index] {
            write!(line, "{cell:>width$}")
        } else {
            write!(line, "{cell:<width$}")
        };
        written.expect("a string accepts the cell");
    }
    println!("{line}");
}
