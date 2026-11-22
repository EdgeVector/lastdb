//! `log.streams` proof. Opt-in (`LASTDB_RELAY_E2E_STREAMS`), self-contained: it
//! mints its own publisher, reader, member and stranger and its own database,
//! so it shares nothing with the bundle proof and can be switched on for any
//! relay that implements the route.
//!
//! Steps, in order (their names are reported in the final JSON):
//! `streams_empty_writer`, `streams_two_devices`, `streams_granted_callers_list`,
//! `streams_head_advances`, `streams_contiguous_head`,
//! `streams_nongrantee_refused`, `streams_revoked_refused`.

use mutation_log::{entry_hash, DatabaseId, SignedEntry};

use crate::chain::{Chain, Publisher};
use crate::grants::http_status;
use crate::identity::{new_device_id, Actor, DbBirth};
use crate::relay::{RelayClient, StreamHead};

/// What the relay should list for one stream: device id, head seq, head hash.
type Expected = (String, u64, String);

struct World<'a> {
    http: &'a reqwest::Client,
    relay_url: &'a str,
    database_id: DatabaseId,
}

impl World<'_> {
    fn client(&self, actor: &Actor) -> RelayClient {
        RelayClient::new(
            self.relay_url,
            self.http,
            actor,
            self.database_id,
            new_device_id(),
        )
    }
}

/// Run every step against the relay and return the names of the steps that passed.
pub async fn prove(http: &reqwest::Client, relay_url: &str) -> Result<Vec<&'static str>, String> {
    let owner = Actor::generate();
    let (reader, member, stranger) = (Actor::generate(), Actor::generate(), Actor::generate());
    let birth = DbBirth::generate(owner.node_key);
    let publisher = Publisher {
        actor: owner,
        database_id: birth.database_id,
    };
    let world = World {
        http,
        relay_url,
        database_id: birth.database_id,
    };
    let owner_hex = publisher.actor.node_key.to_hex();
    let as_publisher = world.client(&publisher.actor);
    let mut steps = Vec::new();
    let (mut a, mut b, mut c) = (
        Chain::new(new_device_id()),
        Chain::new(new_device_id()),
        Chain::new(new_device_id()),
    );

    let opening = [
        publisher.birth(&mut a, &birth)?,
        publisher.grant_read(&mut a, reader.node_key)?,
        publisher.grant_read(&mut a, member.node_key)?,
    ];
    as_publisher.append(&opening).await?;
    let empty = as_publisher.streams(&reader.node_key.to_hex()).await?;
    expect("an empty writer", &empty, &[])?;
    steps.push("streams_empty_writer");

    let mut on_b = vec![publisher.heads(&mut b, a_tips(&a))?];
    on_b.push(publisher.heads(&mut b, a_tips(&a))?);
    as_publisher.append(&on_b).await?;
    let mut expected = vec![head(&a, &opening[2])?, head(&b, &on_b[1])?];
    expected.sort();
    expect(
        "two device streams",
        &as_publisher.streams(&owner_hex).await?,
        &expected,
    )?;
    steps.push("streams_two_devices");

    for who in [&reader, &member] {
        let listed = world.client(who).streams(&owner_hex).await?;
        expect("a caller with a read grant", &listed, &expected)?;
    }
    steps.push("streams_granted_callers_list");

    let more = publisher.heads(&mut b, a_tips(&a))?;
    as_publisher.append(std::slice::from_ref(&more)).await?;
    expected = vec![head(&a, &opening[2])?, head(&b, &more)?];
    expected.sort();
    expect(
        "a stream that grew",
        &as_publisher.streams(&owner_hex).await?,
        &expected,
    )?;
    steps.push("streams_head_advances");

    contiguous_head(&publisher, &as_publisher, &owner_hex, &mut c, &a).await?;
    steps.push("streams_contiguous_head");

    let outsider = world.client(&stranger);
    same_refusal("a caller with no grant", &outsider, &owner_hex).await?;
    steps.push("streams_nongrantee_refused");

    let revoke = publisher.revoke(&mut a, member.node_key)?;
    as_publisher.append(std::slice::from_ref(&revoke)).await?;
    same_refusal("a revoked member", &world.client(&member), &owner_hex).await?;
    steps.push("streams_revoked_refused");
    Ok(steps)
}

/// Seq 1 and seq 3 are stored, seq 2 is not: the head stays at 1. Once seq 2
/// arrives the head jumps to 3.
async fn contiguous_head(
    publisher: &Publisher,
    client: &RelayClient,
    owner_hex: &str,
    c: &mut Chain,
    a: &Chain,
) -> Result<(), String> {
    let first = publisher.heads(c, a_tips(a))?;
    let second = publisher.heads(c, a_tips(a))?;
    let third = publisher.heads(c, a_tips(a))?;
    client.append(std::slice::from_ref(&first)).await?;
    client.append(std::slice::from_ref(&third)).await?;
    let listed = client.streams(owner_hex).await?;
    expect_head("a stream with a gap", find(&listed, c)?, &head(c, &first)?)?;
    client.append(std::slice::from_ref(&second)).await?;
    let listed = client.streams(owner_hex).await?;
    expect_head(
        "a stream whose gap was filled",
        find(&listed, c)?,
        &head(c, &third)?,
    )
}

fn a_tips(chain: &Chain) -> Vec<mutation_log::DeviceHead> {
    chain.tip().into_iter().collect()
}

fn head(chain: &Chain, last: &SignedEntry) -> Result<Expected, String> {
    let hash = entry_hash(last).map_err(|e| e.to_string())?;
    Ok((
        chain.device_id.to_hex(),
        last.header().writer_seq,
        hash.to_hex(),
    ))
}

fn find<'a>(listed: &'a [StreamHead], chain: &Chain) -> Result<&'a StreamHead, String> {
    let device = chain.device_id.to_hex();
    listed
        .iter()
        .find(|s| s.device_id == device)
        .ok_or_else(|| format!("log.streams does not list device {device}"))
}

fn expect_head(what: &str, got: &StreamHead, want: &Expected) -> Result<(), String> {
    if (got.device_id.as_str(), got.head_seq, got.head_hash.as_str())
        == (want.0.as_str(), want.1, want.2.as_str())
    {
        Ok(())
    } else {
        Err(format!(
            "log.streams for {what}: got {got:?}, want {want:?}"
        ))
    }
}

fn expect(what: &str, got: &[StreamHead], want: &[Expected]) -> Result<(), String> {
    let same = got.len() == want.len()
        && got.iter().zip(want).all(|(g, w)| {
            (g.device_id.as_str(), g.head_seq, g.head_hash.as_str())
                == (w.0.as_str(), w.1, w.2.as_str())
        });
    if same {
        Ok(())
    } else {
        Err(format!(
            "log.streams for {what}: got {got:?}, want {want:?}"
        ))
    }
}

/// `log.streams` and an all-devices `log.read` of the same writer must both be
/// refused with 403, with the same code and field.
async fn same_refusal(who: &str, client: &RelayClient, writer_key: &str) -> Result<(), String> {
    let listing = client.streams(writer_key).await.map(|_| ());
    let reading = client.read_writer(writer_key).await.map(|_| ());
    let listed = refusal(who, "log.streams", listing)?;
    let read = refusal(who, "log.read of all devices", reading)?;
    if listed == read {
        Ok(())
    } else {
        Err(format!(
            "{who}: log.streams was refused with {listed:?} but log.read with {read:?}"
        ))
    }
}

fn refusal(who: &str, call: &str, outcome: Result<(), String>) -> Result<String, String> {
    match outcome {
        Ok(()) => Err(format!("relay accepted {call} from {who}")),
        Err(error) if http_status(&error) == Some(403) => Ok(error
            .split_once("HTTP ")
            .map_or_else(String::new, |(_, rest)| rest.to_owned())),
        Err(error) => Err(format!(
            "{who} {call} failed with something other than 403: {error}"
        )),
    }
}
