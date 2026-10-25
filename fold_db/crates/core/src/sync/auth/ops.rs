//! Passive saved metadata decoders only. No legacy action endpoint exists.
pub mod org_authority;
mod recipient_enrollment;
pub use recipient_enrollment::CloudRecipientEnrollment;
