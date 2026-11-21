# Callback panics

Aeron calls Rust callbacks from C. The generated bindings choose panic handling
for each C function:

| Callback | Runs on | What happens to the panic | What happens to the message |
|---|---|---|---|
| Fragment, controlled or block handler passed to a subscription, image or replay-merge `poll`, including `_fn` variants | Your thread, inside `poll` | Unwinds through C to the caller of `poll` with its original payload | The next poll redelivers fragments from the start of the failed poll or its last `COMMIT`. Handlers must tolerate redelivery. Aeron preserves order within each image. A subscription with several images can start the next poll at a different image. |
| `FragmentAssembler` or `AeronFragmentClosureAssembler` delegate | Your thread, inside `poll` | The wrapper catches the panic and raises it after the assembler updates its state | Aeron does not redeliver the panicking message. It still delivers the rest of that poll. |
| `ControlledFragmentAssembler` or `AeronControlledFragmentClosureAssembler` delegate | Your thread, inside `poll` | The wrapper catches the panic and raises it from `poll` | The delegate returns `ABORT`, so the next poll delivers the panicking message again. |
| Persistent subscription handler and its `on_live_joined`, `on_live_left` and `on_error` listeners | Your thread, inside the persistent `poll` | The wrapper catches the panic and raises it from `poll` | Aeron does not redeliver the panicking fragment. |
| Recording descriptor consumer stored by `reset` on an owned poller | Your thread, inside the poller's `poll` | The wrapper catches the panic and raises it from the poller's `poll` | The poller advances past that descriptor and does not deliver it again. Borrowed pollers returned by `AeronArchive` reject `reset`. |
| `for_each_image` visitor | Your thread | The wrapper catches the panic and raises it from `for_each_image` after C releases the image list | C still visits the remaining images. |
| Reserved-value supplier passed to `offer_with_reserved_value` | Your thread, inside `offer` | The wrapper catches the panic and raises it from `offer` | Aeron still publishes the message with reserved value `0`. |
| Other synchronous callbacks, such as `aeron_cnc_error_log_read`, counter `foreach`, loss-report readers and URI parsers | Your thread | The wrapper catches the panic, returns a default value to C and raises it after C returns | The C function receives `0`, null or `()` according to the callback's return type. |
| Typed driver strategy, interceptor, and resolver callbacks, including setup and destruction | Native agent or closing caller | An escaping panic aborts at the `extern "C"` boundary | No fallback or recovery occurs. |
| Retained image, error, registration, client-close, Archive and generated driver hooks | Usually the conductor thread | The wrapper logs the panic with `log::error!` and `eprintln!` and lets the client continue | The wrapper drops that event. Later events still arrive. |

Notes:

- Aeron C has no exception cleanup, so most callbacks cannot unwind through it. Subscription, image and replay merge fragment polls (`poll`, `controlled_poll`, `bounded_poll`, `block_poll`) are the exception. They save the read position only after the handler returns, so a panic unwinds straight through them. A panic skips the read-position commit, so the next poll redelivers the fragments. These poll functions add no `catch_unwind` or check to the poll loop.
- For other callbacks the wrapper catches the panic, returns the value the callback type needs (`()`, `0`, null or `ABORT`) and raises the panic after C returns.
- A retained callback has no waiting Rust caller. The wrapper logs the panic and drops that event.

Edge cases:

- Only the first caught panic on a thread is kept until it is raised. A later one is reported by the panic hook and dropped.
- A caught panic is raised at the next resume point on the same thread. If you drive the image fragment assemblers through the raw bindings, or wrap an assembler in your own handler type, the panic waits until the next call that raises a caught panic. The same applies to a persistent subscription listener when the persistent `poll` is given no fragment handler.
- With `use_conductor_agent_invoker`, conductor callbacks run inside your `invoke` call, but are still logged rather than raised.
- For raw bindings, declare poll handlers as `unsafe extern "C-unwind" fn`. The compiler rejects `extern "C"` handlers for these poll functions. Other raw callbacks must not panic because Rust aborts at the `extern "C"` boundary.
- To make any panic stop the process, set `panic = "abort"` in your Cargo profile. Every panic then aborts immediately, before any of the above runs.


Driver loss suppliers still expose native endpoint pointers. Dereferencing those
pointers or attaching a generator requires the native lifetime and threading
contract. The supplier runs on the conductor. The installed generator runs on the
sender or receiver. A termination validator must check a token's embedded length
against the actual byte slice before interpreting the token.
