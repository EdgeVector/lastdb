use super::{client_from_home, enabled};
use cloud_backup_protocol::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::discovery::{
    StreamsListRequest, StreamsListResponse, STREAMS_LIST_LIMIT_MAX,
};
use cloud_backup_protocol::publication::{HeadReadRequest, HeadReadResult};
use cloud_backup_protocol::{AccountId, DecimalU64, Text, Version};
use serde::Serialize;
use std::path::Path;

#[derive(Serialize)]
pub struct NativeBackupDiscovery {
    pub context: ContextReadResponse,
    pub directory: StreamsListResponse,
    pub heads: Vec<HeadReadResult>,
}

/// Read one bounded directory page. Each head remains tied to its exact stream.
pub async fn discover(
    home: &Path,
    expected_account: &AccountId,
    generation: Option<DecimalU64>,
    cursor: Option<Text<1024>>,
) -> Result<NativeBackupDiscovery, String> {
    enabled()?;
    let client = client_from_home(home)?;
    let context = client
        .context(
            &ContextReadRequest {
                version: Version::V1,
            },
            expected_account,
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut scope = context.scope.clone();
    if let Some(generation) = generation {
        scope.account_generation = generation;
    }
    let request = StreamsListRequest {
        version: Version::V1,
        scope,
        limit: STREAMS_LIST_LIMIT_MAX,
        cursor,
    };
    let directory = client
        .list(&request)
        .await
        .map_err(|error| error.to_string())?;
    let mut heads = Vec::with_capacity(directory.streams.len());
    for streams in directory
        .streams
        .chunks(cloud_backup_protocol::limits::CONCURRENT_STREAMS_MAX)
    {
        let requests: Vec<_> = streams
            .iter()
            .cloned()
            .map(|stream| HeadReadRequest {
                version: Version::V1,
                stream,
            })
            .collect();
        let results =
            futures::future::join_all(requests.iter().map(|request| client.head(request))).await;
        for result in results {
            heads.push(result.map_err(|error| error.to_string())?);
        }
    }
    Ok(NativeBackupDiscovery {
        context,
        directory,
        heads,
    })
}
