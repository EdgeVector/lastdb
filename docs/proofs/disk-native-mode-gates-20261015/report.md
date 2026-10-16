# Native mode gates, October 15, 2026

Source: `331d8320eb7351841d56cd67694fcc5a649e4a12`.
Daemon SHA-256: `88c70558575bc42441cff5132adf98f8a9025574b3d74d474c08d2ef97f0aad4`.

The generated node refuses both `LASTDB_RESIDENT_MODE=read` and `write` with the explicit native root/admission error.
Both commands exit 1 before the native store marker appears.
The same daemon opens a separate generated native home in `off` mode and returns healthy status.
Cloud sync remains off. The reported record budget remains 10,000.
The supported node closes normally.

The root-verification control now uses a mutex instead of a cell.
This preserves the journal writer's shared-thread API and leaves the durable bytes unchanged.
A release home command with that exact control change has SHA-256 `f39bfe530da8888c351deed5822cd33aa7ceb25df3ff5b86aa70887139af225b`.
It accepts a three-operation batch on the generated home from the generation-carry receipt.
The result reaches frontier 3, generation 10.
The prior PR #197 reader returns all 384 expected slots: 192 absences and 64 values of each expected body.

The daemon build, workspace library/binary Clippy, format, source-size, function-size, new-allow, diff, and non-test lints pass.
No test, fixture harness, failure injection, primary, primary copy, archive, factory, or shared service participates.
The mode refusal is a release gate. It does not implement memory-first admission.
