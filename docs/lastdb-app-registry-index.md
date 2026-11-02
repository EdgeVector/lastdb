# LastDB app registry: signed interface lists

The separate registry checks an Exemem account, a public GitHub commit and the
publisher's Ed25519 signature. DynamoDB holds mutable listings. A projector
signs one list per channel and exact API version with AWS KMS and writes it to
S3. Install reads S3 anonymously, without calling the registry Lambda,
DynamoDB, Exemem or Schema Service.

## Publish

Run the app, then publish from its clean git checkout. The command derives
source from the origin remote and sha from HEAD. It reads the running node's
build, api_version and current_outbox_format_version in one socket call.
The build is a label; API equality decides compatibility. A manifest with
outbox_format_version also records the node's current outbox integer.
HTTP-only apps omit it. These values cannot be typed as publish overrides.

The manifest supplies app_id and version. Optional description remains
owner-only metadata. The command signs RFC 8785 JCS of app_id, channel,
app_version, api_version, lastdb_build, source, sha, and the optional
outbox_format_version. Absent fields are omitted rather than null.

Create a person key with `lastdb app dev-init --key-file lastsecrets://SLUG`.
Use the same locator with publish. The private key travels to LastSecrets on
stdin and is read only at use; it never enters the registry or GitHub. Existing
0600 developer key files remain supported.

Pass the registry HTTP base with --registry-url or LASTDB_APP_REGISTRY_URL.
Provide the account key in EXEMEM_DEV_API_KEY, resolved from LastSecrets at
use. The channel defaults to stable. A listed response means the S3 object
written by that publish contains the app. A failed projection returns an error.

## Download and trust

The list base is --index, then LASTDB_REGISTRY_INDEX, then production S3.
Public bases live in folddb_profile/environments.json under registry_index.
Paths are:

```
<base>/<channel>/api/<api_version>.json
<base>/<channel>/api/<api_version>.json.sig
```

The node's integer selects the path. The client uses the owner socket first,
then the paired daemon build, then its own release. A typed --lastdb-version
is refused on this path. Explicit directories and file:// bases are supported.
Network bases require HTTPS, carry no authorization header, and have a 30-second
timeout. Lists are capped at 2 MiB and 500 apps.

The detached signature names alg ed25519, the SHA256 of the raw public key,
the lowercase SHA256 hex of the exact list bytes, and the base64 signature.
KMS signs the ASCII hex digest with ED25519_SHA_512 and message type RAW.
The pinned KMS public key sits beside the historical GitHub index public key.
`lastdb app index trust-key` prints both IDs and their purposes. Explicit trust
overrides remain visible. The index verify command understands both grammars;
interface-list verification includes every publisher signature.

Lists with list_version 1 contain channel, api_version, generated_at, seq
and apps. Every entry must match the list's channel and API version, name a
unique app ID and a full 40-hex public GitHub commit, and verify its publisher
signature. One bad entry rejects the download. An outbox mismatch skips that
entry after authentication; it does not invalidate other apps.

## Stored lists and sequence rules

The default cache is ~/.local/state/lastdb-app-registry/CHANNEL/api/API.json
(or under XDG_STATE_HOME). It retains exact signed bytes, signature and digest.
Explicit index bases get separate namespaces under sources/, so dev and
alternate hosts cannot change production rollback state.

Updates hold a per-file process lock and atomically replace a synced file.
A lower sequence keeps the stored list. Equal sequence and digest is a no-op.
Equal sequence with another digest rejects the download. A higher sequence is
saved before resolve returns. Failed downloads and invalid signatures or lists
leave the verified stored list in place. Without a stored list the operation
fails. Lists at least seven days old produce a generation-time notice.

S3 publishes the signature before conditionally replacing the list. A reader
can observe a mismatched pair during publication or after a failed write.
That download is rejected. A mismatched pair is never used as a valid list.

## Install and follow

`lastdb app resolve APP --json` returns the selected list entry. Missing apps
produce the phrase `was proved with lastdb`; host-track treats it as a hold.
There is no minimum API range or exact build-string match on this path.
The API integer stays unchanged for optional additions and owner-only changes;
an app-socket break raises it in the breaking change.

Install clones source into HOME/apps/APP/source, checks out sha, requires
HEAD to equal it, and writes lastdb-app-install.json. The receipt includes
the listing, index URL and verifying key ID. Failed new installs are cleaned up.
Upgrade stages a checkout before swapping and restores the prior directory
on a failed swap. An unchanged commit stays in place. The list is the whole
compatibility decision; this path does not apply manifest version ranges.
Local --dev --from installs remain separate.

Apps run with the user's permissions. Signatures do not sandbox code. Source
apps may need their normal dependency installation before lastdb app run.
Withdraw stops future selection and leaves installed copies alone.

Install defaults to stable. Host-track follows next and stays enabled. The
shared reader switch waits for a real S3 next resolve of the expected commit.
Retiring the old Mac signer remains a separate decision.
