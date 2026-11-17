# Personal replay conflict custody — retired

This proposal was retired on 17 November 2026. Private owner-device sync
uses whole-record last writer wins, including external and inline attachments.
It does not keep a permanent collection of losing versions or require an
owner to resolve ordinary attachment conflicts.

A pending local edit can temporarily hide the confirmed winning record.
LastDB retains that one confirmed record and its attachment bytes until the
local edit settles. This temporary state exists to install the correct
winner; it is not a conflict archive. Superseded and settled temporary files
are released through the normal file lifecycle.

See [attachment replay](personal-attachment-replay.md) for the implemented
ordering, retention, and compatibility rules. Incompatible schemas and
unavailable required bytes can still prevent replay from advancing.
