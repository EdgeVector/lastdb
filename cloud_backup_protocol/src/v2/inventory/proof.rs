use super::{InventoryIndex, InventoryPage, InventoryRoot};
use crate::v2::transfer::ObjectReceipt;
use crate::{CloudScope, Digest, ProtocolError};

/// A bounded, sequential proof over concrete pages. This is not service authority.
/// The service must independently resolve every receipt to its verified store.
pub struct InventoryVerifier<'a> {
    index: &'a InventoryIndex,
    root: InventoryRoot,
    root_receipt_id: Digest,
    root_receipt: Option<ObjectReceipt>,
    next: usize,
}

impl<'a> InventoryVerifier<'a> {
    pub fn new(index: &'a InventoryIndex, root_receipt_id: Digest) -> Result<Self, ProtocolError> {
        Ok(Self {
            index,
            root: index.root()?,
            root_receipt_id,
            root_receipt: None,
            next: 0,
        })
    }

    pub fn push(&mut self, page: &InventoryPage) -> Result<(), ProtocolError> {
        if page.page_index as usize != self.next
            || page.scope != self.index.scope
            || self.index.pages.get(self.next) != Some(&page.descriptor()?)
        {
            return Err(ProtocolError::invalid("v2.inventory_proof.page"));
        }
        if let Some(receipt) = page
            .objects
            .iter()
            .find(|receipt| receipt.receipt_id == self.root_receipt_id)
        {
            if self.root_receipt.is_some() {
                return Err(ProtocolError::invalid("v2.inventory_proof.duplicate_root"));
            }
            self.root_receipt = Some(receipt.clone());
        }
        self.next += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<InventoryProof, ProtocolError> {
        if self.next != self.index.pages.len() {
            return Err(ProtocolError::invalid("v2.inventory_proof.incomplete"));
        }
        Ok(InventoryProof {
            scope: self.index.scope.clone(),
            root: self.root,
            root_receipt: self
                .root_receipt
                .ok_or_else(|| ProtocolError::invalid("v2.inventory_proof.missing_root"))?,
        })
    }
}

/// Constructed only after every declared page, including the root receipt, validates.
/// It carries no provider proof and cannot be deserialized from a caller declaration.
pub struct InventoryProof {
    scope: CloudScope,
    root: InventoryRoot,
    root_receipt: ObjectReceipt,
}

impl InventoryProof {
    pub fn validate_for(
        &self,
        scope: &CloudScope,
        root: &InventoryRoot,
        root_receipt_id: &Digest,
    ) -> Result<(), ProtocolError> {
        if &self.scope != scope
            || &self.root != root
            || &self.root_receipt.receipt_id != root_receipt_id
        {
            return Err(ProtocolError::invalid("v2.inventory_proof.identity"));
        }
        Ok(())
    }

    pub fn root_receipt(&self) -> &ObjectReceipt {
        &self.root_receipt
    }
}
