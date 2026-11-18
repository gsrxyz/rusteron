# CPU targets and cross-language link-time optimisation

## CPU target

A release build from source compiles the Aeron C code for the build machine's CPU
with `-march=native`. Set `RUSTERON_C_MARCH` to a shared target, such as `x86-64-v3`,
when binaries run on other machines. The `precompile` libraries target the
architecture's baseline, `x86-64` or `armv8-a`. On the
x86-64 VMs in [Tuning on x86-64 Linux](https://github.com/gsrxyz/rusteron/blob/main/docs/tuning_x86_64_linux.md), `-march=native` for the
client's C code gave no consistent gain over `x86-64` or `x86-64-v3` on IPC.

<a id="cross-language-lto-linux"></a>

## Cross-language LTO on Linux

A static build lets LLVM inline Aeron's C into its Rust callers. This can remove
foreign-function and indirect callback calls for `try_claim` and fragment
handlers. Enable cross-language LTO in the final binary. Rusteron's build
settings do not apply to crates that depend on it.

1. Install clang, lld and llvm-ar with the same LLVM major version as rustc.
   `rustc -vV` prints the LLVM version. For LLVM 22, [apt.llvm.org](https://apt.llvm.org) provides these commands:
   `wget https://apt.llvm.org/llvm.sh && sudo bash llvm.sh 22 && sudo apt install lld-22`.
   Upgrade them together with Rust. rustc cannot read bitcode from another LLVM major,
   and the build reports a version mismatch.
2. Link Aeron statically and use fat LTO in the final binary's `Cargo.toml`:

   ```toml
   [dependencies]
   rusteron-client = { version = "0.3", features = ["static"] }

   [profile.release]
   lto = "fat"
   codegen-units = 1
   ```

3. Add the following to the final binary's `.cargo/config.toml`.
   On Arm, use `aarch64-unknown-linux-gnu`:

   ```toml
   [target.x86_64-unknown-linux-gnu]
   linker = "clang-22"
   rustflags = ["-Clinker-plugin-lto", "-Clink-arg=-fuse-ld=lld-22", "-Ctarget-cpu=native"]

   [env]
   CC = "clang-22"
   ```

   `-Clinker-plugin-lto` makes Rusteron's build script compile Aeron C to LLVM bitcode
   with `$CC` or `RUSTERON_CLANG`, for the CPU named by `-Ctarget-cpu`.
   C and Rust must target the same CPU, or LLVM refuses to inline across them.
   `CC` also applies to other C dependencies built by the `cc` crate.
   C uses `-flto` to match `lto = "fat"`. With Rust `lto = "thin"`, set
   `RUSTERON_C_LTO=thin`. Mixing fat Rust LTO with thin C LTO prevents cross-language inlining.

4. Check the build. It prints
   `cross-language LTO: Aeron C compiled to bitcode by clang-22 (LLVM 22) with -flto` for each
   Rusteron crate. Cargo shows build-script warnings only for path dependencies.
   To confirm inlining, inspect the final binary's disassembly at the call sites you care about.
   Larger functions such as `offer`, `offerv` and `try_claim` may stay as calls if
   LLVM judges inlining too costly.

   To see linker inlining decisions, add
   `-Clink-arg=-Wl,--opt-remarks-filename,remarks.yaml` and
   `-Clink-arg=-Wl,--opt-remarks-passes,inline`.

The `precompile` libraries contain native code rather than LLVM bitcode.
They cannot use cross-language LTO, and the build reports a warning.
Rusteron does not support cross-language LTO on macOS.
