use super::{Result, ShareInvitationError};
use crate::share_spec::PublicationSpec;

const INVALID: ShareInvitationError = ShareInvitationError("invalid share invitation spec");

pub(super) fn encode(body: &mut Vec<u8>, spec: &PublicationSpec, permission: &[u8]) -> Result<()> {
    database_policy::SpecBinding::new(&spec.name, spec.version).map_err(|_| INVALID)?;
    let length = u8::try_from(spec.name.len()).map_err(|_| INVALID)?;
    body.push(length);
    body.extend_from_slice(spec.name.as_bytes());
    body.extend_from_slice(&spec.version.to_be_bytes());
    body.extend_from_slice(permission);
    Ok(())
}

pub(super) fn decode(payload: &[u8]) -> Result<(PublicationSpec, &[u8])> {
    let (&length, payload) = payload.split_first().ok_or(INVALID)?;
    let (name, payload) = payload
        .split_at_checked(usize::from(length))
        .ok_or(INVALID)?;
    let (version, permission) = payload.split_first_chunk::<4>().ok_or(INVALID)?;
    let name = std::str::from_utf8(name).map_err(|_| INVALID)?;
    let version = u32::from_be_bytes(*version);
    database_policy::SpecBinding::new(name, version).map_err(|_| INVALID)?;
    Ok((
        PublicationSpec {
            name: name.to_owned(),
            version,
        },
        permission,
    ))
}
