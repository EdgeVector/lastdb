//! The **shared owner-socket handler bodies** — the single implementation of
//! query / mutation / native-index-search / molecule-history / atom-content /
//! schema-resolution that BOTH binaries' socket route executors
//! (`lastdb_node::exec`, `fold_db_node::server::uds_exec`) drive.
//!
//! Historically each executor re-implemented these handler bodies against
//! `fold_db` core, kept identical only by discipline (the extraction card's
//! predecessor moved the *wire* layer here; this module moves the *handler*
//! layer). Now the schema-resolution, pagination push-down, row formatting,
//! conflict annotation, native-search enrichment/filtering, history scoping, and
//! atom hydration live in exactly one place, generic over [`HostNode`], so the
//! two socket surfaces cannot drift below the app-identity axis.
//!
//! Each handler returns a serialized JSON payload (the inner object that the
//! caller wraps in the [`crate::envelope::envelope`] `{ ok, ...data, user_hash }`
//! success shape via [`crate::envelope::json_ok`]) or a [`HostError`] carrying an
//! HTTP status + owner-visible message (the caller maps it through
//! [`crate::envelope::owner_or_content_free`] so a non-owner stays content-free —
//! **I4**). The GET routes parse their own params from the request target; the
//! POST routes take the already-parsed body value. The caller's only job is to
//! bind the user-context task-local, parse the wire, and apply the envelope.

use std::collections::{HashMap, HashSet};

use fold_db::access::AccessContext;
use fold_db::constants::MUTATION_BACKGROUND_TASK_TIMEOUT;
use fold_db::db_operations::HomeConflictAnnotation;
use fold_db::error::FoldDbError;
use fold_db::fold_db_core::mutation_manager::{
    CloudCapturePolicy, CloudCaptureState, CloudMutationReceipt, CloudPublicationState,
    ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages, ResidentDurability,
};
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::field::{HashRangeFilter, KeyWindow};
use fold_db::schema::types::operations::{
    MutationCloudPublication, MutationConvergence, MutationDurability, Query, QueryOrderBy,
    SortOrder,
};
use fold_db::schema::types::{
    AggregateFinalize, AggregateRepair, AggregateSet, KeyValue, Mutation, MutationType, Schema,
    SchemaError, ValueFilter,
};
use fold_db::schema::SchemaState;
use futures::StreamExt;
use lastdb_uds::uds_http::UdsResponse;
use serde_json::{Map, Value};

use crate::envelope::{envelope, json_ok, owner_json_or_content_free, owner_or_content_free};
use crate::host_node::{HostNode, ReadBusy};
use crate::pagination::{clamp_request_limit, compute_has_more, INTERNAL_FETCH_CAP};
use crate::qos::Lane;

mod errors;
mod history;
mod mutation;
mod query;
mod schema_resolve;
mod search;

pub use self::errors::*;
pub use self::history::*;
pub use self::mutation::*;
pub use self::query::*;
pub use self::schema_resolve::*;
pub use self::search::*;
