//! Writes placed inside one synchronous storage call.
//!
//! Embedders carry the placements across their async boundary. The buffer
//! keeps ids and group addresses only. It does not keep value bytes.

use std::cell::RefCell;

use crate::store::ShardKey;

/// One id a store write placed, and the groups that partition can occupy.
///
/// `written` is the single group `group_of` assigned. `touched` is every
/// fanout slot of the id's partition. Callers flush `written` only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedWrite {
    /// Document id that was written.
    pub id: String,
    /// The one group this id was placed in.
    pub written: ShardKey,
    /// Every fanout slot of this id's partition. Not a flush argument.
    pub touched: Vec<ShardKey>,
}

thread_local! {
    static PLACED: RefCell<Option<Vec<PlacedWrite>>> = const { RefCell::new(None) };
}

/// Observe synchronous writes. Nested calls contribute once to their parent.
pub fn observe_placed_writes<T>(work: impl FnOnce() -> T) -> (T, Vec<PlacedWrite>) {
    struct Scope(Option<Vec<PlacedWrite>>);
    impl Drop for Scope {
        fn drop(&mut self) {
            PLACED.with(|cell| {
                let observed = cell.borrow_mut().take().unwrap_or_default();
                match self.0.take() {
                    Some(mut parent) => {
                        parent.extend(observed);
                        *cell.borrow_mut() = Some(parent);
                    }
                    None => *cell.borrow_mut() = None,
                }
            });
        }
    }
    let scope = Scope(PLACED.with(|cell| cell.replace(Some(Vec::new()))));
    let result = work();
    let observed = PLACED.with(|cell| cell.borrow().clone().unwrap_or_default());
    drop(scope);
    (result, observed)
}

pub(crate) fn is_observing() -> bool {
    PLACED.with(|cell| cell.borrow().is_some())
}

pub(crate) fn record(write: PlacedWrite) {
    PLACED.with(|cell| {
        if let Some(buf) = cell.borrow_mut().as_mut() {
            buf.push(write);
        }
    });
}
