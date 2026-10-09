//! Direct schema declare / catalog sync: proposal normalization, catalog-alias
//! persistence, reuse/bind resolution and the sync/check executors.

use super::*;

mod app_binding;
mod bind;
mod catalog_alias;
mod execute;
mod expansion;
mod plan;
mod proposal;
mod reuse;

pub(super) use app_binding::*;
pub(super) use bind::*;
pub(super) use catalog_alias::*;
pub(super) use execute::*;
pub(super) use expansion::*;
pub(super) use plan::*;
pub(super) use proposal::*;
pub(super) use reuse::*;
