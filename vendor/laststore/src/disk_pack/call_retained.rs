//! Bounded reads through a verified durable reference and the writer lease.
use super::home::HomeDescriptor;
use super::lifetime::{DurableReference, GenerationHold, WriterReadLease};
use super::{corrupt, resource, ReadBudget, ReadCall, ReadCosts, PAGE_BYTES};
use crate::read_receipt_io::ReadClass;
use crate::Result;
use std::collections::HashMap;
use std::path::Path;

// Embedded CURRENT prefix plus the complete fixed reference page.
const REFERENCE_PROOF_BYTES: u64 = 4352 + PAGE_BYTES as u64;

impl ReadCall {
    pub(super) fn from_reference(
        root: &Path,
        descriptor: HomeDescriptor,
        reference: &DurableReference,
        budget: ReadBudget,
        lease: &WriterReadLease,
    ) -> Result<Self> {
        budget.validate()?;
        if root != reference.home_path() {
            return Err(corrupt("durable reference belongs to another root"));
        }
        let current = reference.current().clone();
        current.encode()?;
        descriptor.check_current(&current)?;
        if budget.bytes < REFERENCE_PROOF_BYTES {
            return Err(resource());
        }
        let hold = GenerationHold::from_reference(lease, reference)?;
        Ok(Self {
            root: root.to_path_buf(),
            current,
            budget,
            costs: ReadCosts {
                file_bytes: REFERENCE_PROOF_BYTES,
                budget_bytes: REFERENCE_PROOF_BYTES,
                ..ReadCosts::default()
            },
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            files: super::call_io::CallFiles::default(),
            payload_class: ReadClass::Payload,
            native_descriptor: Some(descriptor),
            _generation_hold: Some(hold),
        })
    }
}
