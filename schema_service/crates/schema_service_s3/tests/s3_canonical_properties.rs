//! Real S3-compatible HTTP checks. CI supplies a disposable loopback MinIO.
//! These checks do not establish AWS, Lambda, DynamoDB, or production behavior.

#[path = "s3/aws.rs"]
mod aws;
#[path = "s3/backend.rs"]
mod backend;
#[path = "s3/cases.rs"]
mod cases;
#[path = "s3/proxy.rs"]
mod proxy;
#[path = "s3/support.rs"]
mod support;
