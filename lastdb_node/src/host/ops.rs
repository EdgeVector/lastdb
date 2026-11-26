//! In-memory counts of completed requests. The tables start empty when the
//! node starts. Once a minute the ops history job writes the change since the
//! previous minute (see `sample`) to a local file that no backup includes.
//!
//! Caps, and what happens at each cap:
//! - 256 call rows of (caller, client, route). The next new key counts on
//!   caller `{overflow}` and route `other`.
//! - 256 schema rows of (caller, client, schema, read or write). The next new
//!   key counts on caller `{overflow}`, an empty client, schema `{overflow}`,
//!   and op `read`.
//! - 32 distinct unknown paths. A further unknown path keeps its caller and
//!   uses route `other`. `other` does not consume one of the 32.
//! - 8 distinct schema names in one request. Further names in that request
//!   count on schema `{overflow}` for that caller. Each named schema, and the
//!   overflow row when there is one, receives the whole request time.
//! - Caller, client, and an unknown path are cut at 64 bytes on a character
//!   boundary. A schema name longer than 128 bytes is left out of the schema
//!   table; the item still counts on the call.
//!
//! `{overflow}` and `other` are ordinary keys. A real caller, schema, or path
//! with one of those names shares the row. Once a key exists, recording it
//! only adds to the counters.
//!
//! `write_phases` splits the time of committed `/v1/write` requests by phase
//! (see `phases`). It has no rows and no caps.

mod body;
mod phases;
mod sample;
mod scan;
mod view;

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use serde_json::{json, Value};

pub(crate) use phases::WriteTimes;
pub(crate) use scan::{call_of, CallLabel};

const MAX_CALLS: usize = 256;
const MAX_SCHEMAS: usize = 256;
const MAX_UNKNOWN: u16 = 32;
const SCHEMAS_PER_REQUEST: usize = 8;
const CALL_CLIP: usize = 64;
const SCHEMA_LIMIT: usize = 128;
const OWNER: &str = "owner";
const UNKNOWN: &str = "unknown";
const OVERFLOW: &str = "{overflow}";
const OTHER: &str = "other";
const CODES: [&str; 8] = [
    "invalid_request",
    "permission_denied",
    "not_found",
    "conflict",
    "too_large",
    "busy",
    "method_not_allowed",
    "internal",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Read,
    Write,
}

impl Op {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

enum Label {
    Static(&'static str),
    Owned(String),
}

impl Label {
    fn as_str(&self) -> &str {
        match self {
            Self::Static(text) => text,
            Self::Owned(text) => text,
        }
    }

    fn shared(value: &str) -> Self {
        match value {
            "" => Self::Static(""),
            OWNER => Self::Static(OWNER),
            UNKNOWN => Self::Static(UNKNOWN),
            OVERFLOW => Self::Static(OVERFLOW),
            OTHER => Self::Static(OTHER),
            _ => Self::Owned(value.to_owned()),
        }
    }

    fn call(value: &str, known: Option<&'static str>) -> Self {
        known.map_or_else(|| Self::shared(value), Self::Static)
    }
}

struct CallRow {
    caller: Label,
    client: Label,
    call: Label,
    count: u64,
    total_ns: u64,
    slowest_ns: u64,
    items: u64,
    refused: [u64; 8],
    unknown: bool,
    mark: sample::CallMark,
}

impl CallRow {
    fn matches(&self, caller: &str, client: &str, call: &str) -> bool {
        self.caller.as_str() == caller
            && self.client.as_str() == client
            && self.call.as_str() == call
    }

    fn bump(&mut self, status: u16, ns: u64, items: u64) {
        self.count = self.count.saturating_add(1);
        self.total_ns = self.total_ns.saturating_add(ns);
        self.slowest_ns = self.slowest_ns.max(ns);
        self.items = self.items.saturating_add(items);
        self.mark.recent_slowest_ns = self.mark.recent_slowest_ns.max(ns);
        if status >= 400 {
            let slot = &mut self.refused[scan::code_at(status)];
            *slot = slot.saturating_add(1);
        }
    }
}

struct SchemaRow {
    caller: Label,
    client: Label,
    schema: Label,
    op: Op,
    items: u64,
    total_ns: u64,
    /// Items and total time at the last history sample.
    mark: (u64, u64),
}

impl SchemaRow {
    fn matches(&self, caller: &str, client: &str, schema: &str, op: Op) -> bool {
        self.op == op
            && self.caller.as_str() == caller
            && self.client.as_str() == client
            && self.schema.as_str() == schema
    }
}

struct Tables {
    calls: Vec<CallRow>,
    unknown_paths: u16,
    schemas: Vec<SchemaRow>,
}

impl Tables {
    fn new() -> Self {
        Self {
            calls: Vec::with_capacity(MAX_CALLS),
            unknown_paths: 0,
            schemas: Vec::with_capacity(MAX_SCHEMAS),
        }
    }
}

pub(crate) struct Completed<'a> {
    pub(crate) apps: bool,
    pub(crate) headers: &'a [(String, String)],
    pub(crate) body: &'a [u8],
    pub(crate) call: CallLabel,
    pub(crate) status: u16,
    pub(crate) elapsed: Duration,
}

pub(crate) struct Ops {
    inner: Mutex<Tables>,
    writes: phases::PhaseTotals,
}

impl Default for Ops {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Tables::new()),
            writes: phases::PhaseTotals::default(),
        }
    }
}

impl Ops {
    pub(crate) fn record(&self, done: &Completed<'_>) {
        let caller = scan::clip(scan::caller_of(done), CALL_CLIP);
        let client = scan::clip(scan::client_of(done), CALL_CLIP);
        let ns = u64::try_from(done.elapsed.as_nanos()).unwrap_or(u64::MAX);
        let scan = match done.call.as_str() {
            "/v1/read" => body::items(done.body, b"reads"),
            "/v1/write" => body::items(done.body, b"mutations"),
            _ => None,
        };
        let items = scan.as_ref().map_or(1, |found| found.items);
        let mut tables = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        tables.add_call(caller, client, &done.call, done.status, ns, items);
        if let Some(found) = scan.as_ref() {
            let op = if done.call.as_str() == "/v1/read" {
                Op::Read
            } else {
                Op::Write
            };
            tables.add_schemas(caller, client, found, op, ns);
        }
    }

    /// Add the phases of one committed `/v1/write` request.
    pub(crate) fn record_write(&self, times: &WriteTimes) {
        self.writes.add(times);
    }

    pub(crate) fn snapshot(&self, uptime_secs: u64) -> Value {
        let tables = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let mut calls: Vec<Value> = tables.calls.iter().map(view::call_value).collect();
        let mut schemas: Vec<Value> = tables.schemas.iter().map(view::schema_value).collect();
        drop(tables);
        view::sort_calls(&mut calls);
        view::sort_schemas(&mut schemas);
        json!({
            "uptime_secs": uptime_secs,
            "calls": calls,
            "schemas": schemas,
            "write_phases": self.writes.value(),
        })
    }
}

impl Tables {
    fn add_call(
        &mut self,
        caller: &str,
        client: &str,
        label: &CallLabel,
        status: u16,
        ns: u64,
        items: u64,
    ) {
        let mut call = label.as_str();
        let mut known = label.known();
        let mut track = label.is_unknown();
        let fresh = track
            && !self
                .calls
                .iter()
                .any(|row| row.unknown && row.call.as_str() == call);
        if fresh && self.unknown_paths >= MAX_UNKNOWN {
            call = OTHER;
            known = Some(OTHER);
            track = false;
        }
        if let Some(row) = self
            .calls
            .iter_mut()
            .find(|row| row.matches(caller, client, call))
        {
            row.bump(status, ns, items);
            return;
        }
        if self.calls.len() >= MAX_CALLS - 1 {
            self.add_call_overflow(status, ns, items);
            return;
        }
        if track && fresh {
            self.unknown_paths = self.unknown_paths.saturating_add(1);
        }
        let mut row = blank_call(caller, client, call, known, track);
        row.bump(status, ns, items);
        self.calls.push(row);
    }

    fn add_call_overflow(&mut self, status: u16, ns: u64, items: u64) {
        if let Some(row) = self
            .calls
            .iter_mut()
            .find(|row| row.matches(OVERFLOW, "", OTHER))
        {
            row.bump(status, ns, items);
            return;
        }
        if self.calls.len() < MAX_CALLS {
            let mut row = blank_call(OVERFLOW, "", OTHER, Some(OTHER), false);
            row.bump(status, ns, items);
            self.calls.push(row);
        }
    }

    fn add_schemas(&mut self, caller: &str, client: &str, scan: &body::Scan, op: Op, ns: u64) {
        scan.visit(|name, items| self.add_schema(caller, client, name, op, items, ns));
        if scan.extra > 0 {
            self.add_schema(caller, client, OVERFLOW, op, scan.extra, ns);
        }
    }

    fn add_schema(
        &mut self,
        caller: &str,
        client: &str,
        schema: &str,
        op: Op,
        items: u64,
        ns: u64,
    ) {
        if items == 0 {
            return;
        }
        let capped = self.schemas.len() >= MAX_SCHEMAS - 1
            && !self
                .schemas
                .iter()
                .any(|row| row.matches(caller, client, schema, op));
        let (caller, client, schema, op) = if capped {
            (OVERFLOW, "", OVERFLOW, Op::Read)
        } else {
            (caller, client, schema, op)
        };
        if let Some(row) = self
            .schemas
            .iter_mut()
            .find(|row| row.matches(caller, client, schema, op))
        {
            row.items = row.items.saturating_add(items);
            row.total_ns = row.total_ns.saturating_add(ns);
            return;
        }
        if self.schemas.len() < MAX_SCHEMAS {
            self.schemas.push(SchemaRow {
                caller: Label::shared(caller),
                client: Label::shared(client),
                schema: Label::shared(schema),
                op,
                items,
                total_ns: ns,
                mark: (0, 0),
            });
        }
    }
}

fn blank_call(
    caller: &str,
    client: &str,
    call: &str,
    known: Option<&'static str>,
    unknown: bool,
) -> CallRow {
    CallRow {
        caller: Label::shared(caller),
        client: Label::shared(client),
        call: Label::call(call, known),
        count: 0,
        total_ns: 0,
        slowest_ns: 0,
        items: 0,
        refused: [0; 8],
        unknown,
        mark: sample::CallMark::default(),
    }
}
