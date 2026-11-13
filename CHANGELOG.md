# Changelog

Main changes, newest first, grouped by published crate version. Release-only
commits, build logs, and changes later reverted or superseded are omitted.
Early history predating the available release tags is grouped separately.

## 0.2.21

- Breaking: getters that return a C object owned by `self`, such as `archive.get_archive_context()`, `get_aeron()`, `context()`, `counters_reader()` and the archive pollers, now return `Ref<'_, T>`. It borrows the owner, so using the result after dropping or closing the owner no longer compiles (#81). `AeronArchive::aeron()` returns `Ref<'_, Aeron>`.
- Added callback slots such as `AeronErrorHandlerSlot`. Use `set` or `clear` to change a callback without registering again. Old callbacks are freed after their last call. See the [example](rusteron-archive/examples/replaceable_error_handler.rs).

## 0.2.20

- Documented why registered handlers survive replacement and asynchronous close. Added an archive regression test for copied handler pointers and final callback cleanup.
- Renamed the generated Rust directory in release bundles to `rust-bindings`. The committed `docs-rs` directories remain documentation snapshots.
- Fixed static precompiled builds to load Rust bindings for the same Cargo target as the C libraries. Linux builds no longer use the Darwin documentation snapshots.
- Added `just test-precompiled-bindings` and CI checks for Linux x86_64, Linux ARM64, and macOS.
- Upgraded Aeron to 1.54.2.

## 0.2.19

- Added `AeronUriStringBuilder::socket_tos` for UDP traffic class and `untethered_linger_timeout` for untethered subscription linger timeouts.
- Update docs and general clean-up
- More unit tests

## 0.2.18

- Updated bindgen to 0.73, syn to 3 and proptest to 1.12. bindgen now mangles `C` and `C-unwind` functions alike, so the `trust_clang_mangling(false)` workaround is gone.
- Breaking: forward-declared C structs (`aeron_stct`, `aeron_context_stct`, `aeron_publication_stct`, ...) no longer derive `Copy` or `Clone` (bindgen 0.73). Their wrappers stay `Clone`, but lose `From<Wrapper> for aeron_x_t`, which copied a zero-sized placeholder rather than the C struct. Use `get_inner()` for the pointer.

## 0.2.17

- Breaking: callbacks receive wrapper arguments by reference (`&AeronHeader`, `&AeronImage`, `&AeronSubscription`, `&AeronArchiveRecordingDescriptor`, ...). C frees or reuses the struct when the callback returns, and the reference stops safe code keeping it past that point. Handler trait impls, `_fn` closures, fragment assemblers and `poll` handlers change from `header: AeronHeader` to `header: &AeronHeader` (#73).
- Breaking: structs Aeron only passes to callbacks (`AeronHeader`, `AeronArchiveRecordingDescriptor`, `AeronArchiveRecordingSubscriptionDescriptor`, `AeronArchiveRecordingSignal`, `AeronPublicationErrorValues`) no longer implement `Clone`, `From<*mut T>`, `From<*const T>` or `From<T>`. Copy the fields you need out, or use `unsafe fn from_ptr` / `unsafe fn clone_reference` within the callback (#73).
- Breaking: `clone_struct` is `unsafe` on structs with pointer fields, such as the recording descriptor's channel strings, because the copy still points at the source's memory. It stays safe on structs without pointers (#73).
- Added missing constants such as `AERON_DATA_HEADER_BEGIN_FLAG`, `AERON_DATA_HEADER_END_FLAG`, `AERON_DATA_HEADER_EOS_FLAG`, `AERON_DATA_HEADER_REVOKED_FLAG`, `AERON_DATA_HEADER_UNFRAGMENTED`, `AERON_HDR_TYPE_*` and `AERON_FRAME_ALIGNMENT`. They are generated as `u32`, so compare them with a header's `u8` `flags` using `as u8`.
- Added the driver error codes (`AERON_ERROR_CODE_*`) to rusteron-client and rusteron-archive.
- Added the archive client's context defaults and environment variable names (`AERON_ARCHIVE_*_DEFAULT`, `AERON_ARCHIVE_*_ENV_VAR`). `AeronArchiveContext` now exposes its fields through getters, and `AeronArchiveCredentialsSupplier` is a full wrapper.
- Added `AeronDriver::version_text`, `version_git_sha`, `version_major`, `version_minor` and `version_patch`, plus the default termination validators `termination_validator_default_allow`, `termination_validator_default_deny` and `termination_validator_load`.
- Fixed generated wrappers that pass a slice to a C function with an `int32_t` length; they now cast `len()` to the C length type.

## 0.2.16

- Added `AeronArchiveRecordingDescriptorPoller::new` and `AeronArchiveRecordingSubscriptionDescriptorPoller::new`, so recordings can be listed without blocking. The archive's own pollers are borrowed and refuse `reset`.
- Breaking: `AeronCountersReader::get_counter_value` returns `Result<i64, AeronCError>`. An id that is not a counter (`-1`, or above `max_counter_id`) is an error, instead of a crash or a read past the counters buffer. The value is read atomically (#70).
- Breaking: `AeronBufferClaim::data_mut` and `frame_header_mut` take `&mut self`. A claim with no buffer gives an empty slice and `None`, instead of a null pointer.

## 0.2.15

- Updated Aeron to 1.54.0, including new errors, APIs, and regenerated bindings.
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
