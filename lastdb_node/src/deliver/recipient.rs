//! A receiver publishes a new signed address; legacy queued targets remain unchanged.
use super::*;

pub(super) async fn address(host: &Host) -> Result<crate::cloud::RecipientAddress, String> {
    let auth = super::cloud_generation::auth(host).await?;
    let api_url = crate::cloud::recipient_api_url(&host.home)?;
    crate::cloud::ensure_recipient_address(&host.home, &api_url, host.keypair.as_ref(), &auth).await
}

/// `POST /api/sharing/recipient` creates or returns one durable account-owned address.
pub async fn execute_recipient_address(ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_json(403, "recipient address is owner-only", ctx);
    }
    let address = match address(host).await {
        Ok(address) => address,
        Err(error) => {
            return error_json(502, &format!("recipient enrollment failed: {error}"), ctx)
        }
    };
    let announcement = match crate::seal::seal_signed_envelope(
        &serde_json::json!({"message_type":"recipient_address","address":&address}),
        host.keypair.as_ref(),
    ) {
        Ok(envelope) => envelope,
        Err(error) => return error_json(500, &error, ctx),
    };
    ok_json(
        &serde_json::json!({"recipient":{"recipient_pubkey":address.identity_public_key,
        "messaging_public_key":address.messaging_public_key,"messaging_pseudonym":address.recipient_pseudonym},
        "address":address,"announcement":announcement,
        "note":"publish this signed recipient address; legacy queued deliveries keep their original target"}),
        ctx,
    )
}
