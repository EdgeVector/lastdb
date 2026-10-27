# Nightly LastDB releases

This process publishes LastDB candidates on GitHub. A release publication does not authorize an upgrade of the primary.

## Phase 1: GitHub prereleases

The [nightly workflow](../.github/workflows/nightly-release.yml) runs each day at **07:17 UTC**. A manual dispatch can select the same process.
It builds one exact main commit for Apple Silicon and Linux x64. It uses standard GitHub-hosted runners in this public repository.
Use no paid larger runner or self-hosted runner. All builds and product checks run in CI.

The targets are aarch64-apple-darwin and x86_64-unknown-linux-gnu. Each archive contains a matched lastdb CLI and lastdbd daemon.
The tag is **nightly-YYYYMMDD-SHA9**. The binary version is **0.1.0-nightly.YYYYMMDD.gSHA9**.
The 0.1.0 base comes from the workspace package version at this process's initial source review.
The publication preflight skips an unchanged source commit after it verifies the existing release.
An accepted source change receives a new candidate, unless a newer source already has a verified nightly.

Each published tag and its asset bytes are immutable. A rerun must verify an existing published release or fail.
It must not replace published assets. It can repair only a source-matched unpublished draft that this workflow owns.
The workflow discovers a draft through a bounded authenticated release list, then checks its numeric release ID before each mutation.
GitHub's tag lookup returns published releases only. A missing release fails with a clear error before a mutation.
The release is a prerelease and sets GitHub's latest flag to false. It does not move next, stable, a Homebrew formula, or a tap.
The old release.yml and nightly-canary.yml workflows stay disabled. The new workflow does not dispatch them or push their v-prefixed tags.

The release must include:

- One archive, one bundle manifest, and one compact product-check record for each target.
- The combined nightly.json descriptor and SHA256SUMS.txt, which covers the other seven files.
- The exact source commit, target, paired binary versions, and per-binary hashes.
- The CI run link and GitHub provenance for the published artifacts.

GitHub's workflow token authorizes the GitHub publication. GitHub provenance identifies the builder and source.
It does not grant an app-registry publisher identity or replace a DevCert.
Do not copy a developer key from the primary or place a credential in an archive, manifest, log, or Git commit.

## Build, check, and publish

The workflow must complete these steps before it publishes a release:

1. Resolve one source commit and use it for both target builds.
2. Build lastdb and lastdbd with the same candidate version.
3. Package both executable files and record their versions and hashes.
4. Run the packaged candidate's required product checks on a fresh CI home.
5. Upload and download the artifacts. Verify the target, source, checksums, versions, and product-check records.
6. Verify provenance, stage the draft, verify its uploaded assets, and publish the prerelease.

The product check uses generated data and a fresh home. It must not use the primary, its archive, or a copy of either.
The check must confirm a keyed write, a keyed read, and the required restart result.
It must check the archive that the release publishes. A successful build alone does not establish that result.
Do not add tests or a fixture suite. Keep build, format, syntax, lint, secret, and real product checks.

The implementation files have separate roles:

| File | Role |
| --- | --- |
| [.github/workflows/nightly-release.yml](../.github/workflows/nightly-release.yml) | Schedule, target jobs, gates, and publication order. |
| [scripts/release/nightly_plan.py](../scripts/release/nightly_plan.py) | Exact source, date, version, tag, and source gates. |
| [scripts/release/nightly_package.py](../scripts/release/nightly_package.py) | Paired archives, hashes, and source manifests. |
| [scripts/release/nightly_probe.py](../scripts/release/nightly_probe.py) | Fresh-home product checks on the candidate. |
| [scripts/release/nightly_publish.py](../scripts/release/nightly_publish.py) | Immutable GitHub publication and uploaded-asset checks. |

Generated manifests, logs, and raw output stay outside Git. CI summaries and artifacts provide the validation record.
Record the source revision, artifact URL, checksum, and artifact expiration where CI output can expire.
Do not describe expiring CI artifact URLs as permanent release assets.

## Operator rules, rollback, and retention

Publication leaves the primary and its service unchanged. An operator selects and authorizes a primary upgrade separately through the required safe-upgrade procedure.
The nightly workflow must not install binaries on a user machine, change its current pointer, or restart its installed daemon.
It must not enroll an app, change registry channels, or publish with a personal credential.

On a failed candidate, stop publication and retain the failed CI evidence for review. Leave earlier successful prereleases intact.
After a bad publication, mark the release clearly and select an earlier known candidate for any separately authorized upgrade.
Do not repair a release by replacing its bytes. A corrected candidate needs a new immutable identity.

Phase 1 has no automatic release deletion. An operator can review retention after the release cadence is established.
Keep any release that an operator or registry references, plus the previous known candidate needed for rollback.
Remove an unsafe release only through an explicit operator action after the reference check.
Disable the new schedule to stop future publications. This action does not alter an installed node or a previous release.

## Registry state at the initial review

The source review used LastDB commit **e6e4f667f9ebbfc51586b3742f11f7d7a6633e77** on 2026-10-26.
At that observation, the public LastDB repository had no GitHub releases. Both legacy release workflows were disabled.
The local apps/registry.json shelf contained ingestion only. It did not establish the state of the cloud registry.

Exact anonymous V1 reads for /v1/apps/lastdb returned HTTP 404 in dev and prod. Both service responses contained app_id lastdb and reason unknown_app.
These responses establish that those V1 registries had no lastdb slug record at that observation.

Exact reads for /v2/apps/lastdb and /v2/apps/lastdb/channels/nightly returned API Gateway HTTP 404 in dev and prod.
The response body was {"message":"Not Found"}. This is a missing deployed route response, not the service's unknown_app response.
Do not use it as evidence that an app or channel record is absent.

The infrastructure review used schema-infra commit **e4183407561e8a0dea0bcb6b93f75d6ef9d699dc**.
Its cdk/lib/schema-stack.ts declares V1 app routes at lines 534-556. It declares no V2 app-release routes.
Persistent V2 release and channel code in the service source does not establish a deployed endpoint.

## Existing clients and their limits

The default app install command reads a signed static index. It checks compatibility and installs a source commit.
The current resolver accepts supported API and outbox contract ranges. It prefers an exact LastDB build where one exists.
Rows without contract information require an exact build match. The older exact-only description in the registry document does not describe all current rows.

The legacy live-service installer also installs source. It refuses an artifact-only app record.
The separate release-install command downloads one signed archive and changes a Host Track current pointer.
It does not select an operating system or architecture. It does not enforce the available compatibility records.
It does not start or restart a daemon. It has no LastDB-specific primary-upgrade guard.

A registry publication does not invoke any installer. An explicitly active release-check watch can restore an older pointer after drift or revocation.
That watch does not download a newly desired release. An unchanged registry channel therefore does not prove a host's current state.

The generic release probe checks its directory and receipt when probe.sh is absent. This does not establish that LastDB can read or write data.
The generic installer moves current before its status proof. That proof does not block activation.
Do not use this installer as the primary upgrade procedure.

## Phase 2: explicit SYSTEM_NODE adapter

Reuse the registry's publisher checks, immutable storage, channel generations, and revocation model. Add a distinct, versioned SYSTEM_NODE descriptor and route contract.
Do not represent the node as an ordinary schema-owning app. Do not add dummy schemas or remove the ordinary app's schema requirement.

The new contract must define:

- The real lastdb app slug, stable UUID, owner, and authorized publisher.
- The descriptor version and the explicit SYSTEM_NODE package kind.
- An immutable release version, source commit, and descriptor identity.
- A platform map with each target's artifact URL, byte size, digest, and publisher signature.
- The paired CLI and daemon versions and hashes for each target.
- The HTTP API contract, outbox contract where relevant, and disk storage format **2**.
- Required fresh-home product probes and the CI evidence that supports each release.
- The permitted source and target versions for a separately authorized upgrade or rollback.

The service must reject unsupported descriptor versions. Old app installers must fail closed before they can download or activate a node package.
An extra kind field on the existing manifest is insufficient. Existing clients can ignore unknown fields.
Use a separate route or an explicit read contract that refuses SYSTEM_NODE packages to legacy clients.

The current release-publish command requires a nonempty schema set and a schema lockfile. A binary-only LastDB package cannot pass that command unchanged.
The current release manifest has one artifact URL. It has no package kind, platform map, or node version contract.
The adapter must address these gaps before a nightly workflow writes registry releases or channels.

The current S3 channel writer already uses conditional writes through an ETag or a create-only condition.
The core changes its local channel cache before the durable write completes. A failed write can leave a value that the service cannot confirm.
The adapter must publish cache changes after durable success and refresh that cache after a conflict or storage failure.
Channel reads must report the authoritative durable result before automatic node distribution starts.
A channel create must require generation zero. A lost creation race must return HTTP 409 consistently across independent service instances.

## Phase 2 identity and deployment boundary

GitHub's token and provenance do not authorize a registry write. The current registry publisher needs an enrolled developer API credential and an Ed25519 developer key.
Each signed write uses a short-lived DevCert and a purpose-specific signature. A new key must not silently replace the registered owner.

Choose either a delegated CI publisher or a new, explicitly trusted GitHub OIDC identity path. Define its repository, workflow, branch, environment, and action limits.
Keep a compatibility verifier identity separate from the app publisher where the service requires that separation.
Use the approved secret service at the point of use. Print no credential and keep no credential in generated release files.

The adapter needs a separately authorized infrastructure deployment to expose its routes. A service-source merge alone does not make the routes available.
Verify the deployed dev route before a prod route change. Keep phase 1 usable while the adapter is unavailable.
Registry publication must remain separate from primary installation and restart.

## Future files and verification

| Area | Exact source or deployment file | Required change |
| --- | --- | --- |
| App manifest | [lastdb_node/src/app_publish/manifest.rs](../lastdb_node/src/app_publish/manifest.rs) | Keep ordinary app requirements; add explicit node-package input separately. |
| Signed writes | [lastdb_node/src/app_publish/release.rs](../lastdb_node/src/app_publish/release.rs) | Build and sign the versioned node descriptor with the authorized CI identity. |
| Service records | [schema_service/crates/core/src/app_release/records.rs](../schema_service/crates/core/src/app_release/records.rs) | Define immutable SYSTEM_NODE records and target metadata. |
| Service validation | [schema_service/crates/core/src/app_release/validate.rs](../schema_service/crates/core/src/app_release/validate.rs) | Enforce kind, format, platform, publisher, and upgrade constraints. |
| Service persistence | [schema_service/crates/core/src/app_release/persistence.rs](../schema_service/crates/core/src/app_release/persistence.rs) | Preserve conditional S3 writes and make cache reads agree with durable channel state after failure. |
| Registry client | [lastdb_node/src/app_release_host/registry.rs](../lastdb_node/src/app_release_host/registry.rs) | Read the explicit node route and refuse unsupported formats. |
| Artifact check | [lastdb_node/src/app_release_host/artifact.rs](../lastdb_node/src/app_release_host/artifact.rs) | Verify the selected target, matched pair, signatures, and required probe. |
| Activation | [lastdb_node/src/app_release_host/activation.rs](../lastdb_node/src/app_release_host/activation.rs) | Keep download checks separate from authorized LastDB upgrade steps. |
| CLI | [lastdb_node/src/bin/lastdb/cli_defs/app.rs](../lastdb_node/src/bin/lastdb/cli_defs/app.rs) | Expose an explicit node-package action with no automatic primary change. |
| API Gateway | schema-infra/cdk/lib/schema-stack.ts | Deploy the new route contract through a separately authorized infrastructure change. |

Verify a real release with an exact anonymous read after publication. Check its descriptor, target bytes, digest, signature, source, and paired versions.
Verify stale channel generations fail instead of overwriting a concurrent update. Verify unsupported clients and targets refuse activation.
Verify revocation and rollback retain the previous known artifact. Use generated data on fresh CI homes for all product checks.
Keep the primary, its archive, and copies of either outside this work.

## Pinned source references

- [LastDB app manifest and schema gate](https://github.com/EdgeVector/lastdb/blob/e6e4f667f9ebbfc51586b3742f11f7d7a6633e77/lastdb_node/src/app_publish/manifest.rs#L105-L133).
- [Release descriptor and publisher signatures](https://github.com/EdgeVector/lastdb/blob/e6e4f667f9ebbfc51586b3742f11f7d7a6633e77/lastdb_node/src/app_publish/release.rs#L14-L88).
- [DevCert request and signed write](https://github.com/EdgeVector/lastdb/blob/e6e4f667f9ebbfc51586b3742f11f7d7a6633e77/lastdb_node/src/app_publish/release.rs#L188-L263).
- [Current compatibility selection](https://github.com/EdgeVector/lastdb/blob/e6e4f667f9ebbfc51586b3742f11f7d7a6633e77/lastdb_node/src/app_registry_index/resolve.rs#L44-L150).
- [Generic activation order](https://github.com/EdgeVector/lastdb/blob/e6e4f667f9ebbfc51586b3742f11f7d7a6633e77/lastdb_node/src/app_release_host/activation.rs#L39-L100).
- [Existing immutable release fields](https://github.com/EdgeVector/lastdb/blob/e6e4f667f9ebbfc51586b3742f11f7d7a6633e77/schema_service/crates/core/src/app_release/records.rs#L9-L29).
- [Deployed route declarations at the infrastructure review](https://github.com/EdgeVector/schema-infra/blob/e4183407561e8a0dea0bcb6b93f75d6ef9d699dc/cdk/lib/schema-stack.ts#L534-L556).
