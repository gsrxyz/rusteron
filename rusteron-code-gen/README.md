# rusteron-code-gen

`rusteron-code-gen` reads Aeron's C headers and generates the bindings and wrappers
used by `rusteron-client`, `rusteron-archive` and `rusteron-media-driver`.

---

## Purpose

The parser identifies C structs, functions and callbacks. Templates turn them into
Rust wrappers. Shared code adds resource cleanup and callback lifetime management.

This crate is an internal build dependency of Rusteron.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

## Features

The generator produces raw FFI bindings, resource wrappers and callback traits.
The build scripts append shared and crate-specific helpers to that output.

---

## Usage

The Rusteron build scripts call this crate. Applications should depend on the client,
Archive or Media Driver crate instead.

---

## Safety considerations

Generated wrappers use `unsafe` to call Aeron C. When an upstream API changes, review
the generator's argument classification, cleanup rules and callback lifetimes.
Edit the templates or shared helpers, then regenerate the checked-in output.

---

<a id="license--acknowledgments"></a>

## License and acknowledgments

Dual-licensed under MIT or Apache-2.0. See the root [README](https://github.com/gsrxyz/rusteron#readme).

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
