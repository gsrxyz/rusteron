# Parked upstream PR: expose the archive error code in the C client API

**Target repo:** [aeron-io/aeron](https://github.com/aeron-io/aeron) · **Status:** not yet raised · **Origin:** rusteron 0.2 error-handling work (July 2026)

## Symptom

C archive clients cannot obtain the numeric archive error code (`ARCHIVE_ERROR_CODE_*`,
e.g. `UNKNOWN_RECORDING = 5`) programmatically. Every public error surface flattens it into
text:

- `aeron_archive_poll_for_error_response()` fills a caller buffer with the *message* only.
- Failed synchronous calls (e.g. `aeron_archive_start_replay` on an unknown recording) report
  via `AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "... errorCode=%" PRIi64 ", error: %s", ...)`,
  so `aeron_errcode()` returns the generic `-11` and the archive code only appears inside
  the `aeron_errmsg()` string.

The Java and C++ clients both expose it (`io.aeron.archive.client.ArchiveException#errorCode()`,
C++ `ArchiveException::errorCode()`), so C is the odd one out. Downstream consumers (rusteron,
and presumably any C user wanting typed error handling) must regex `errorCode=N` out of the
message text.

## Root cause / evidence

The C client already captures the code — it just never surfaces it:

- `aeron_archive_control_response_poller.h:35` — `int64_t relevant_id;` holds the wire
  `errorCode` alongside `error_message` (line 43).
- `aeron_archive_client.c:444` — `aeron_archive_poll_for_error_response()` copies
  `poller->error_message` into the caller's buffer and discards `poller->relevant_id`.
- `aeron_archive_client.c` (`aeron_archive_poll_for_response`, ~line 165) — the sync-call
  error path prints `poller->relevant_id` into the errmsg text, then throws it away.

The poller struct is internal (`aeron_archive_t` is opaque, no public accessor), so
downstream code cannot reach `relevant_id` without patching aeron.

## Proposed change (minimal, additive)

Add one function next to the existing pair in `client/aeron_archive.h` (after line 607):

```c
/**
 * Poll the response stream once for an error, additionally returning the archive error
 * code (ARCHIVE_ERROR_CODE_*) when an error is found.
 *
 * @param aeron_archive the archive client
 * @param error_code_p out param set to the archive error code, or AERON_NULL_VALUE if no error
 * @param buffer to fill with the error message (empty string if no error)
 * @param buffer_length length of the buffer
 * @return 0 for success, -1 for failure while reading the subscription
 */
int aeron_archive_poll_for_error_response_with_code(
    aeron_archive_t *aeron_archive, int32_t *error_code_p, char *buffer, size_t buffer_length);
```

Implementation in `aeron_archive_client.c` is the existing `aeron_archive_poll_for_error_response`
body (line 444) with two extra writes:

```c
    *error_code_p = AERON_NULL_VALUE;                      // no-error default, before cleanup
    ...
    if (poller->is_control_response && poller->is_code_error)
    {
        *error_code_p = (int32_t)poller->relevant_id;      // the archive error code
        snprintf(buffer, buffer_length, "%s", poller->error_message);
        goto cleanup;
    }
```

Optionally refactor the old function to delegate:
`return aeron_archive_poll_for_error_response_with_code(aeron_archive, &ignored, buffer, buffer_length);`

## Test plan

- Extend the existing archive C system test that exercises `poll_for_error_response`
  (aeron-archive C tests) with an unknown-recording replay: assert the message *and*
  `*error_code_p == ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING`.
- CHANGELOG.adoc entry under `[C]`: "Add aeron_archive_poll_for_error_response_with_code to
  expose the archive error code, matching the Java/C++ clients."

## Out of scope (possible follow-up PR)

Making failed synchronous calls set a distinguishable `aeron_errcode()` instead of
`-AERON_ERROR_CODE_GENERIC_ERROR` (e.g. a dedicated archive errcode range). More useful but
changes observable behaviour of existing APIs — keep it out of the first PR.

## Process notes

- aeron requires a signed CLA before PRs are accepted.
- Branch from `master` of aeron-io/aeron; C client code style follows the surrounding files
  (goto-cleanup pattern, `AERON_SET_ERR`/`AERON_APPEND_ERR`).

## rusteron adoption once merged

- `AeronArchive::poll_for_error()` (rusteron-archive/src/lib.rs) switches from
  `AeronArchiveError::parse()` on the message text to reading the out-param directly —
  no rusteron API change; `AeronArchiveErrorCode` and the
  `archive_error_codes_match_the_c_header` sync test stay as they are.
- `AeronArchiveError::parse()` remains for codes embedded in `aeron_errmsg()` by failed
  synchronous calls (until the follow-up PR lands, if ever).
- Bump the aeron submodules to the release containing the fix (`just update-aeron-version`),
  regenerate (`just build`) — the new function is picked up automatically by bindgen and the
  wrapper generator.
