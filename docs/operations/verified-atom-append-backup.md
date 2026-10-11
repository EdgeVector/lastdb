# Verified atom append for a normal backup

A normal DEV snapshot can recover its predecessor from the cloud.
That manifest mirror does not supply local retirement authority.
The previous cut refused every changed atom digest without a local pending receipt.
It refused before it checked whether the file kept the exact predecessor bytes.

The normal publisher now permits an exact strict append at the same physical address.
The predecessor must have a positive length. The selected file must be longer.
One file handle verifies the exact predecessor prefix and the complete selected length and digest.
A missing file, read error, shorter file, changed prefix, or stale selected digest still stops the cut.

The normal serving fence and publication packing lock cover this proof and the selected cut.
The existing snapshot, copy, source, chain, counter, and file-pack capability checks stay in place.
The existing public cut, backup-only cut, and primary resume paths keep their previous retirement requirement.
The change writes no pending receipt and grants no commit, purge, retention, or GC authority.

The successor contains every predecessor atom byte at the same physical address.
The latest restore reads those complete successor files.
The existing GC policy keeps the published manifest's object digests.
An older prefix object's digest can leave that keep set after a successor.
This change does not promise object availability for an arbitrary older manifest.

Use product build, format, lint, secret, and source-size checks. Do not add or run tests.
A fresh synthetic product proof must show a valid strict append with no local pending receipt for the remote predecessor.
That proof must show the exact successor parent and a fresh source-free restore of the known keys.
The whole safe upgrade keeps the normal disk floor, copy checks, deadlines, and Cloud On soak.
