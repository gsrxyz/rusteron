# Changelog

Main changes, newest first, grouped by published crate version. Release-only
commits, build logs, and changes later reverted or superseded are omitted.
Early history predating the available release tags is grouped separately.

## Unreleased

- Updated Aeron to master at `8352de142d` (1.54.0-SNAPSHOT), including new errors, APIs, and regenerated bindings.
- Included the upstream fix preventing tagged network subscriptions from reusing closed endpoints (`250f657364`).
- Added archive-scoped recording-counter lookups and corrected source-identity buffer handling.
- Updated driver affinity configuration and enabled destination removal by registration ID after the upstream fix.
- Made persistent-subscription context ownership explicit and expanded regression coverage.
- Prevented SNAPSHOT builds from using incompatible released binaries; corrected macOS AddressSanitizer runtime selection.
- Scoped test cleanup to archive Java processes launched by the test harness.

## 0.2.14

- Propagated supported callback panics safely after Aeron cleanup; logged background callback panics without stopping processing.
- Kept archive clients alive through their contexts and improved callback lifetimes.
- Fixed binary counter-key reads to return the complete key region.

## 0.2.13

- Corrected target-specific compiler flags for cross-language LTO.

## 0.2.11

- Upgraded Aeron to 1.53.3.
- Added cross-language C/Rust LTO support and optimized header reads and generated getters.
- Corrected handle thread-safety requirements under the `multi-threaded` feature.
- Improved archive connection, persistent-subscription lifetimes, and recording-position helpers.
- Expanded benchmarks, examples, and Linux, cloud, and Kubernetes tuning guidance.

## 0.2.6

- Added cancellation for incomplete asynchronous operations and destination-management helpers.
- Adopted Rust edition 2024 and expanded ARM64 coverage.

## 0.2.5

- Fixed binding generation for unnamed helper structs on ARM64 Linux.

## 0.2.2

- Upgraded Aeron to 1.52.2 and corrected static linking on Linux.

## 0.2.1

- Upgraded Aeron to 1.52.0 and fixed dependency compatibility.

## 0.2.0

- Introduced the 0.2 API redesign with reference-counted handlers and deferred resource closure.
- Added typed publication/archive errors, vectored publishing, and zero-copy buffer claims.
- Expanded memory-safety coverage with AddressSanitizer and lifecycle tests.

## 0.1.168

- Applied the handler cleanup fix to precompiled documentation bindings.

## 0.1.167

- Fixed handler cleanup.

## 0.1.164

- Added persistent-subscription builders, listeners, and integration tests.
- Added error, archive, and idle-strategy helpers.
- Improved Valgrind coverage.

## 0.1.163

- Upgraded Aeron to 1.51.0 and refreshed Docker examples.

## 0.1.162

- Upgraded Aeron to 1.50.2.

## 0.1.161

- Added Valgrind checks and simplified dependencies.
- Added packet-loss simulation tests.

## 0.1.158-0.1.160

- Updated Aeron integration and corrected examples.

## 0.1.157

- Avoided panics when decoding invalid UTF-8.

## 0.1.156

- Added richer debug output.
- Fixed double-free errors after failed asynchronous publication/subscription creation.

## 0.1.155

- Added C-binding call logging.

## 0.1.154

- Fixed temporary-value lifetimes in generated field accessors.

## 0.1.153

- Added controlled fragment closure assembly.

## 0.1.151

- Improved precompiled-library compatibility with older Linux systems.

## 0.1.147

- Improved docs.rs support.

## 0.1.145

- Fixed URI-builder cleanup.

## 0.1.143

- Improved error-message reporting.

## 0.1.140-0.1.142

- Fixed precompiled-library paths and docs.rs output.

## 0.1.x (early history)

- Added precompiled-library support, deterministic code generation, and resource-management tests.
- Added system-counter types and publication readiness helpers.
- Fixed missing resource closure and drop-related crashes.
- Introduced generated Rust wrappers for the Aeron client, archive, and media driver.
- Added closure-based handlers, C-string conversion, and media-driver examples.
- Added CnC inspection and driver-heartbeat access.
