//! A bounded merge of sealed key cursors and the open tail's key map.
#![allow(dead_code)] // LastStore write-path wiring is a follow-up card.
//!
//! No body is read to choose a key or suppress an older version. The merge
//! holds one key and one transient 4 KiB block per sealed segment. The tail
//! remains borrowed, so a page cannot clone the complete tail map either.

use crate::sorted::{Cursor, KeyRecord, Segment};
use crate::{Error, Result};
use std::collections::{btree_map, BTreeMap};
use std::iter::Peekable;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodyLocation<T> {
    Tail(T),
    Sealed {
        segment: usize,
        offset: u64,
        length: usize,
    },
}

#[derive(Debug)]
pub(crate) struct MergedKey<T> {
    pub key: String,
    /// A tombstone remains visible to a merge until all older segments retire.
    pub body: Option<BodyLocation<T>>,
}

pub(crate) struct Merge<'a, T> {
    cursors: Vec<Cursor<'a>>,
    heads: Vec<Option<KeyRecord>>,
    advance: Vec<bool>,
    tail: Peekable<btree_map::Range<'a, String, Option<T>>>,
    advance_tail: bool,
}

impl<'a, T: Copy> Merge<'a, T> {
    /// `segments` are in commit order, oldest first. The open tail is newer
    /// than every sealed segment. Each source contains unique ascending keys.
    pub(crate) fn new(
        segments: &'a [Segment],
        key: Option<&'a [u8; 32]>,
        tail: &'a BTreeMap<String, Option<T>>,
        start: &str,
    ) -> Result<Self> {
        let mut cursors = Vec::with_capacity(segments.len());
        let mut heads = Vec::with_capacity(segments.len());
        for segment in segments {
            if segment.header.encrypted != key.is_some()
                || segments
                    .first()
                    .is_some_and(|first| first.header.shard != segment.header.shard)
            {
                return Err(Error::Corrupt(
                    "sorted merge: incompatible segment identity".into(),
                ));
            }
            let mut cursor = segment.cursor(key, start.as_bytes())?;
            let head = loop {
                let next = cursor.next_key()?;
                if next
                    .as_ref()
                    .map_or(true, |record| record.key.as_str() >= start)
                {
                    break next;
                }
            };
            cursors.push(cursor);
            heads.push(head);
        }
        Ok(Self {
            cursors,
            heads,
            advance: vec![false; segments.len()],
            tail: tail.range(start.to_string()..).peekable(),
            advance_tail: false,
        })
    }

    /// Return the newest version of the next key, including tombstones.
    /// Advance consumed sources only on the following call, so a selected
    /// body can reuse its header's decoded block without speculative reads.
    pub(crate) fn next_key(&mut self) -> Result<Option<MergedKey<T>>> {
        for slot in 0..self.cursors.len() {
            if self.advance[slot] {
                let next = self.cursors[slot].next_key()?;
                if let (Some(previous), Some(next)) = (&self.heads[slot], &next) {
                    if previous.key >= next.key {
                        return Err(Error::Corrupt(
                            "sorted merge: source keys are not ascending".into(),
                        ));
                    }
                }
                self.heads[slot] = next;
                self.advance[slot] = false;
            }
        }
        if self.advance_tail {
            self.tail.next();
            self.advance_tail = false;
        }
        let sealed_key = self
            .heads
            .iter()
            .flatten()
            .map(|head| head.key.as_str())
            .min();
        let next_key = sealed_key
            .into_iter()
            .chain(self.tail.peek().map(|(key, _)| key.as_str()))
            .min();
        let Some(next_key) = next_key else {
            return Ok(None);
        };
        let key = next_key.to_string();
        let mut body = None;
        for (slot, head) in self.heads.iter().enumerate() {
            if let Some(head) = head.as_ref().filter(|head| head.key == key) {
                // Later segments win even when the later record is a delete.
                body = head.body.map(|(offset, length)| BodyLocation::Sealed {
                    segment: slot,
                    offset,
                    length,
                });
                self.advance[slot] = true;
            }
        }
        if let Some((_, tail_body)) = self.tail.peek().filter(|(tail_key, _)| **tail_key == key) {
            body = tail_body.map(BodyLocation::Tail);
            self.advance_tail = true;
        }
        Ok(Some(MergedKey { key, body }))
    }

    pub(crate) fn read_sealed_into(
        &mut self,
        segment: usize,
        offset: u64,
        destination: &mut [u8],
    ) -> Result<()> {
        self.cursors
            .get_mut(segment)
            .ok_or_else(|| Error::Corrupt("sorted merge: invalid segment location".into()))?
            .read_into(offset, destination)
    }
}
