# rusteron-code-gen

`rusteron-code-gen` is an internal code generation tool used within the [Rusteron](https://github.com/gsrxyz/rusteron) project.  
It automates the creation of Rust bindings from Aeron’s C APIs, reducing manual effort and ensuring consistent wrapper interfaces across the `rusteron-*` crates.

---

## Purpose

Aeron's C APIs follow predictable structural patterns. This tool parses those C headers and uses templates to emit Rust wrappers around the raw FFI layer. It is primarily used to generate:

- `rusteron-client`
- `rusteron-archive`
- `rusteron-media-driver`

By automating this step, we reduce maintenance cost and improve reliability when tracking upstream changes in Aeron.

> **Note**: This crate is not intended for standalone use outside the Rusteron project.

---

## Features

- **Automated Code Generation** – Converts Aeron C headers into Rust-safe APIs.
- **Pattern-Based Templating** – Leverages Aeron’s consistent structure to minimize boilerplate.
- **Integration-Friendly** – Output is directly used in production modules of the Rusteron stack.

---

## Usage

This crate is used via internal tooling (e.g. in `just` scripts or CI pipelines) and is not meant to be added as a dependency in consumer projects.

---

## Safety Considerations

Generated code includes `unsafe` blocks where necessary to interface with Aeron’s low-level constructs.  
While much of the generation is automated, occasional manual review and patching may be required to ensure correctness, especially when Aeron introduces API changes.
