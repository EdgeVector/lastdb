//! Optional actual AWS proof. Only the accepted main workflow and its fresh
//! run-owned bucket can execute this case; loopback CI does not execute it.

use super::support::*;
use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream};
use schema_service_core::ExternalSchemaPersistence;

#[test]
fn aws_isolated_bucket_cas_instances_and_cold_cache() {
    run(async {
        let harness = Harness::aws().await;
        actual_aws_conditional_race(&harness).await;
        let first = harness.state().await;
        let second = harness.state().await;
        let (a, b) = tokio::join!(
            first.add_canonical_field(
                "parcel_delivery_status",
                field("Parcel delivery status from first carrier")
            ),
            second.add_canonical_field(
                "parcel_delivery_status",
                field("Parcel delivery status from second carrier")
            ),
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let error = a.err().or_else(|| b.err()).expect("conflict");
        assert!(
            error.to_string().contains("canonical_field_conflict"),
            "{error}"
        );
        let stale = harness.state().await;
        let other = harness.state().await;
        let (a, b) = tokio::join!(
            stale.add_canonical_field(
                "parcel_current_location",
                field("Parcel current location from the carrier")
            ),
            other.add_canonical_field(
                "parcel_customs_status",
                field("Parcel customs status from the carrier")
            ),
        );
        a.expect("location");
        b.expect("customs status");
        let durable = harness
            .backend()
            .load_all_canonical_fields()
            .await
            .expect("durable AWS vocabulary");
        assert_eq!(durable.len(), 3);
        assert_eq!(
            *harness
                .state()
                .await
                .canonical_fields
                .read()
                .expect("cold AWS cache"),
            durable
        );
        harness.cleanup().await;
    });
}

async fn actual_aws_conditional_race(harness: &Harness) {
    let initial = harness
        .direct
        .put_object()
        .bucket(&harness.bucket)
        .key("direct-cas")
        .body(ByteStream::from_static(b"initial"))
        .if_none_match("*")
        .send()
        .await
        .expect("create CAS object");
    let etag = initial.e_tag().expect("real AWS ETag").to_owned();
    let put = |value: &'static [u8]| {
        harness
            .direct
            .put_object()
            .bucket(&harness.bucket)
            .key("direct-cas")
            .body(ByteStream::from_static(value))
            .if_match(&etag)
            .send()
    };
    let (a, b) = tokio::join!(put(b"first"), put(b"second"));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let failure = a.err().or_else(|| b.err()).expect("stale ETag loses");
    assert!(
        matches!(
            failure.as_service_error().and_then(|error| error.code()),
            Some("PreconditionFailed" | "ConditionalRequestConflict")
        ),
        "AWS documents both conditional conflict responses: {failure}"
    );
}
