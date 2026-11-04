# rusteron-code-gen

`rusteron-code-gen` is an internal code generation tool used within the [Rusteron](https://github.com/gsrxyz/rusteron) project.  
It generates Rust bindings from Aeron's C APIs, which reduces manual effort and keeps wrapper interfaces consistent across the `rusteron-*` crates.

---

## Purpose

Aeron's C APIs follow predictable structural patterns. This tool parses those C headers and uses templates to emit Rust wrappers around the raw FFI layer. It is primarily used to generate:

- `rusteron-client`
- `rusteron-archive`
- `rusteron-media-driver`

Generating them keeps maintenance cost down and makes it more reliable to track upstream changes in Aeron.

> This crate is not intended for standalone use outside the Rusteron project.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

## Features

- Converts Aeron C headers into Rust APIs.
- Uses templates built on Aeron's consistent structure to reduce boilerplate.
- The output is used directly in the Rusteron crates.

---

## Usage

This crate is used via internal tooling (e.g. in `just` scripts or CI pipelines) and is not meant to be added as a dependency in consumer projects.

---

## Safety considerations

Generated code includes `unsafe` blocks where it must interface with Aeron's low-level constructs.
Most of the generation is automated, but you may need to review and patch it by hand, especially when Aeron changes its API.

---

## License & Acknowledgments

Dual-licensed under MIT or Apache-2.0. See the root [README](https://github.com/gsrxyz/rusteron#readme).

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
