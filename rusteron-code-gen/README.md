# rusteron-code-gen

This internal build dependency reads Aeron's C headers and generates the bindings,
resource wrappers and callback traits used by the client, Archive and Media Driver.
Applications should depend on those crates instead.

[GSR](https://www.gsr.io) sponsors and maintains Rusteron. It has run in GSR's
low-latency trading system since early 2025.

## Changing generated code

1. Edit the generator, argument classification or shared helpers.
2. Review native ownership, callback lifetimes and the thread-safety allowlist.
3. Regenerate the checked-in `docs-rs` snapshots for all affected crates.
4. Run generator tests, native integration tests and ownership compile checks.

Storage is selected through a sealed type parameter. New native resource types
stay local until their thread-safety contracts have been reviewed.
See [owning and borrowed handles](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md#owning-and-borrowed-handles)
and [thread safety](https://github.com/gsrxyz/rusteron/blob/main/docs/thread-safety.md).

Use the generator for repeated wrappers. `convenience.rs` generates Duration
accessors from native getter/setter pairs and destination helpers from the native
operations available for each handle. URI method names come from native string
constants with the `AERON_URI_`, `AERON_URI_STRING_BUILDER_` or
`AERON_UDP_CHANNEL_` prefix and the `_KEY` suffix. The generator strips these
parts and converts the remaining name to lowercase. Explicit rules preserve the
boolean, numeric, enum and nanosecond value types. Other keys accept `&str`.
New keys get methods when the bindings are regenerated. This adds no runtime
lookup. Constants that define values, such as `reserved`, do not get setters.
One Archive descriptor-poller template retains callbacks and rejects borrowed
pollers.
`constant_enums.rs` generates enums and integer conversions from these native
families. Each enum keeps its own namespace.

| Native constants | Rust enum |
| --- | --- |
| `AERON_COUNTER_*_TYPE_ID` | `AeronCounterTypeId` |
| `AERON_COUNTER_RECORD_*` | `AeronCounterState` |
| `AERON_ERROR_CODE_*` | `AeronDriverErrorCode` |
| `AERON_ARCHIVE_ERROR_CODE_*`, `AERON_ARCHIVE_CLIENT_ERROR_*` | `AeronArchiveErrorCode` |
| `AERON_SYSTEM_COUNTER_ID_*` | `AeronSystemCounterType`, generated in `convenience.rs` |

The first three enums have `from_code()` and `code()` methods, plus `From`
conversions for `i32`. They preserve unrecognised values as `Unknown(i32)`.
The driver enum takes the protocol value without changing its sign. Archive
errors keep their existing wire-code and signed native-code conversions.

```ignore
let kind = AeronCounterTypeId::from(reader.counter_type_id(counter_id)?);
let state = AeronCounterState::from(reader.counter_state(counter_id)?);
```

The raw integer methods still work. These enums add no work to polling or
publishing. `AeronErrorType` and `AeronOfferError` also contain Rust-specific
errors and retry rules, so they remain separate. Sizes, offsets and combinable
flags stay as constants.

`native_enums.rs` also generates `TryFrom` and `From` conversions for native
enum declarations. It reads each enum's integer representation and variants.
`TryFrom` returns `Err(value)` for an unknown integer. It uses a checked match
and does not use `transmute` or allocate. Type aliases share these conversions.

```ignore
use rusteron_client::bindings::aeron_controlled_fragment_handler_action_t;

let action = aeron_controlled_fragment_handler_action_t::try_from(4_u32).unwrap();
let code = u32::from(action);
```

The Archive recording-signal helper uses this conversion. It still returns
`None` for the native null signal. Direct conversion returns the null variant.
These conversions run only when called. They add no work to polling or publishing.

The same generator adds Rust names, such as
`AeronControlledFragmentHandlerAction::Continue` and
`AeronThreadingMode::Shared`. It strips C type suffixes and the shared constant
prefix. Each Rust type name re-exports the native enum. Each value name is an
associated constant with the native enum type. Both forms pass directly to the
existing methods and callbacks.

The Archive includes `aeron_archive_replay_params.h` for `copy_from` and
`is_bounded`, and `aeron_archive_configuration.h` for protocol-version queries.
These methods use the regular generator. Copy methods keep their source as a
separate borrowed argument, including when it has the same type as the receiver.

Keep custom code for rules that C signatures cannot express, such as claim
lifetimes, retained parent handles and borrowed buffers. Text getters borrow by
default. Mutable native text requires an explicit safety contract; `_owned`
helpers make a copy only when the caller requests one. Each custom
behavior needs unit tests for its results and error cases. Add compile-fail tests
for lifetime restrictions and native integration tests for cleanup and callbacks.
Test both generated variants when a template supports more than one handle.

## Contributing and license

See [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md).
Dual-licensed under MIT or Apache-2.0. Thanks to Rusteron's creator
[@mimran1980](https://github.com/mimran1980),
[@bspeice](https://github.com/bspeice) for the original
[libaeron-sys](https://github.com/bspeice/libaeron-sys), and the Aeron community.
