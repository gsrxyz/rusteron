//! Release compiler flags for the vendored Aeron C build.
//!
//! The choice lives here rather than in `build_common.rs`, which each crate's
//! build script `include!`s and so cannot be unit-tested in place.

/// Which CPUs a release build of Aeron C must run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuTarget {
    /// Built from source for this CPU (`-march=native`).
    ThisCpu,
    /// A prebuilt library published for `precompile` users, which must run on any CPU of
    /// the architecture.
    Baseline,
}

/// The compiler CMake actually used, rather than a potentially different `clang` on PATH.
pub fn cmake_c_compiler(cache: &str) -> Option<&str> {
    cache.lines().find_map(|line| {
        let value = line
            .strip_prefix("CMAKE_C_COMPILER:FILEPATH=")
            .or_else(|| line.strip_prefix("CMAKE_C_COMPILER:STRING="))
            .or_else(|| line.strip_prefix("CMAKE_C_COMPILER:UNINITIALIZED="))?;
        (!value.is_empty()).then_some(value)
    })
}

/// `CMAKE_C_FLAGS_RELEASE` for the Aeron C build of `arch` (`CARGO_CFG_TARGET_ARCH`)
/// and `vendor` (`CARGO_CFG_TARGET_VENDOR`) for `target`. `march` (`RUSTERON_C_MARCH`)
/// overrides the target's `-march`.
pub fn release_c_flags(arch: &str, vendor: &str, target: CpuTarget, march: Option<&str>) -> String {
    let mut flags = String::from("-O3 -DNDEBUG -funroll-loops");
    match (march.filter(|m| !m.is_empty()), target) {
        (Some(march), _) => flags.push_str(&format!(" -march={march}")),
        (None, CpuTarget::ThisCpu) => flags.push_str(" -march=native"),
        (None, CpuTarget::Baseline) => match arch {
            "x86_64" => flags.push_str(" -march=x86-64 -mtune=generic"),
            "aarch64" if vendor != "apple" => flags.push_str(" -march=armv8-a"),
            // Apple's default CPU is already the platform baseline.
            _ => {}
        },
    }
    flags
}

/// Whether the final binary links with `-Clinker-plugin-lto`, from `CARGO_ENCODED_RUSTFLAGS`
/// (flags separated by `0x1f`). That flag is the switch for cross-language LTO: Aeron C is
/// then compiled to LLVM bitcode so the linker can inline it into Rust callers.
pub fn wants_linker_plugin_lto(encoded_rustflags: &str) -> bool {
    codegen_options(encoded_rustflags)
        .any(|o| o == "linker-plugin-lto" || o.starts_with("linker-plugin-lto=") && o != "linker-plugin-lto=no")
}

/// The `-C` options in `CARGO_ENCODED_RUSTFLAGS`, written `-Cname=value` or `-C name=value`.
fn codegen_options(encoded_rustflags: &str) -> impl Iterator<Item = &str> {
    let flags: Vec<&str> = encoded_rustflags.split('\x1f').collect();
    (0..flags.len()).filter_map(move |i| match flags[i].strip_prefix("-C") {
        Some("") => flags.get(i + 1).copied(),
        Some(option) => Some(option),
        None => None,
    })
}

/// `CMAKE_C_FLAGS_RELEASE` under cross-language LTO. LLVM inlines a C function into a Rust
/// caller only when the C target features are a subset of Rust's, so C targets exactly the
/// CPU rustc does (`rustc_cpu`, from `-Ctarget-cpu`; `None` keeps both on the target default).
pub fn lto_release_c_flags(arch: &str, rustc_cpu: Option<&str>) -> String {
    let mut flags = String::from("-O3 -DNDEBUG -funroll-loops");
    match rustc_cpu {
        // clang's -march=native on aarch64 picks a different CPU from rustc's native
        Some(cpu) if arch == "aarch64" => flags.push_str(&format!(" -mcpu={cpu}")),
        Some(cpu) => flags.push_str(&format!(" -march={cpu}")),
        None => {}
    }
    if arch == "x86_64" {
        // clang gives Ice Lake server and later Intel CPUs SGX, which LLVM's own CPU models
        // (and so rustc's) lack; Aeron uses no SGX
        flags.push_str(" -mno-sgx");
    }
    flags
}

/// The last `-Ctarget-cpu` in `CARGO_ENCODED_RUSTFLAGS`.
pub fn rustc_target_cpu(encoded_rustflags: &str) -> Option<&str> {
    codegen_options(encoded_rustflags)
        .filter_map(|o| o.strip_prefix("target-cpu="))
        .last()
}

/// The LLVM major version in `rustc -vV` (`LLVM version: 22.1.2`) or `clang --version`
/// (`clang version 22.1.8`) output.
pub fn llvm_major(version_output: &str) -> Option<u32> {
    version_output.lines().find_map(|line| {
        let rest = line
            .strip_prefix("LLVM version: ")
            .or_else(|| line.split_once("clang version ").map(|(_, v)| v))?;
        rest.split('.').next()?.trim().parse().ok()
    })
}

/// The `CMAKE_C_FLAGS` for cross-language LTO: `-flto` for `RUSTERON_C_LTO` (`full`, the
/// default, or `thin`), and `--target` set to rustc's `target` (cargo's `TARGET`). Without
/// it clang tags the bitcode with its own default triple (`x86_64-pc-linux-gnu` on Debian
/// and Ubuntu), and lld warns on every C module linked into Rust's `x86_64-unknown-linux-gnu`.
/// The cmake crate would pass `--target` itself, but skips its compiler flags once
/// `CMAKE_C_FLAGS` is defined.
pub fn c_lto_flags(kind: Option<&str>, target: &str) -> Result<String, String> {
    let lto = match kind.filter(|k| !k.is_empty()) {
        None | Some("full") => "-flto",
        Some("thin") => "-flto=thin",
        Some(other) => return Err(format!("RUSTERON_C_LTO={other} is not one of `full` or `thin`")),
    };
    Ok(format!("{lto} --target={target}"))
}

#[cfg(test)]
mod tests {
    use super::{
        CpuTarget, c_lto_flags, cmake_c_compiler, llvm_major, lto_release_c_flags, release_c_flags, rustc_target_cpu,
        wants_linker_plugin_lto,
    };

    #[test]
    fn cmake_c_compiler_reads_configured_path() {
        assert_eq!(
            cmake_c_compiler("// compiler\nCMAKE_C_COMPILER:FILEPATH=/usr/bin/cc\n"),
            Some("/usr/bin/cc")
        );
        assert_eq!(
            cmake_c_compiler("CMAKE_C_COMPILER:STRING=/compiler with spaces/clang\n"),
            Some("/compiler with spaces/clang")
        );
        assert_eq!(
            cmake_c_compiler("CMAKE_C_COMPILER:UNINITIALIZED=/usr/bin/cc\n"),
            Some("/usr/bin/cc")
        );
        assert_eq!(cmake_c_compiler("CMAKE_C_COMPILER:FILEPATH=\n"), None);
        assert_eq!(cmake_c_compiler("CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/c++\n"), None);
    }

    #[test]
    fn lto_c_follows_rustc_cpu() {
        assert_eq!(
            lto_release_c_flags("aarch64", Some("native")),
            "-O3 -DNDEBUG -funroll-loops -mcpu=native"
        );
        assert_eq!(
            lto_release_c_flags("x86_64", Some("emeraldrapids")),
            "-O3 -DNDEBUG -funroll-loops -march=emeraldrapids -mno-sgx"
        );
        assert_eq!(
            lto_release_c_flags("x86_64", None),
            "-O3 -DNDEBUG -funroll-loops -mno-sgx"
        );
        assert_eq!(
            rustc_target_cpu("-Ctarget-cpu=native\x1f-Clinker-plugin-lto"),
            Some("native")
        );
        assert_eq!(rustc_target_cpu("-C\x1ftarget-cpu=a\x1f-Ctarget-cpu=b"), Some("b"));
        assert_eq!(rustc_target_cpu("-Clinker-plugin-lto"), None);
    }

    #[test]
    fn detects_linker_plugin_lto() {
        assert!(wants_linker_plugin_lto("-Ctarget-cpu=native\x1f-Clinker-plugin-lto"));
        assert!(wants_linker_plugin_lto("-C\x1flinker-plugin-lto"));
        assert!(wants_linker_plugin_lto("-Clinker-plugin-lto=yes"));
        assert!(!wants_linker_plugin_lto("-Clinker-plugin-lto=no"));
        assert!(!wants_linker_plugin_lto("-Ctarget-cpu=native"));
        assert!(!wants_linker_plugin_lto(""));
    }

    #[test]
    fn parses_llvm_major() {
        assert_eq!(llvm_major("rustc 1.95.0\nhost: x\nLLVM version: 22.1.2\n"), Some(22));
        assert_eq!(llvm_major("Homebrew clang version 22.1.8\nTarget: arm64"), Some(22));
        assert_eq!(llvm_major("Ubuntu clang version 18.1.3 (1ubuntu1)\n"), Some(18));
        assert_eq!(llvm_major("Apple clang version 21.0.0 (clang-2100.3.34.2)"), Some(21));
        assert_eq!(llvm_major("gcc (GCC) 13.2.0"), None);
    }

    #[test]
    fn lto_flags_carry_kind_and_rustc_target() {
        let target = "x86_64-unknown-linux-gnu";
        assert_eq!(
            c_lto_flags(None, target),
            Ok("-flto --target=x86_64-unknown-linux-gnu".into())
        );
        assert_eq!(
            c_lto_flags(Some("thin"), target),
            Ok("-flto=thin --target=x86_64-unknown-linux-gnu".into())
        );
        assert!(c_lto_flags(Some("fat"), target).is_err());
    }

    #[test]
    fn from_source_targets_this_cpu() {
        assert_eq!(
            release_c_flags("x86_64", "unknown", CpuTarget::ThisCpu, None),
            "-O3 -DNDEBUG -funroll-loops -march=native"
        );
    }

    #[test]
    fn published_libraries_target_the_architecture_baseline() {
        assert_eq!(
            release_c_flags("x86_64", "unknown", CpuTarget::Baseline, None),
            "-O3 -DNDEBUG -funroll-loops -march=x86-64 -mtune=generic"
        );
        assert_eq!(
            release_c_flags("aarch64", "unknown", CpuTarget::Baseline, None),
            "-O3 -DNDEBUG -funroll-loops -march=armv8-a"
        );
        assert_eq!(
            release_c_flags("aarch64", "apple", CpuTarget::Baseline, None),
            "-O3 -DNDEBUG -funroll-loops"
        );
    }

    #[test]
    fn march_override_wins() {
        assert_eq!(
            release_c_flags("x86_64", "unknown", CpuTarget::ThisCpu, Some("x86-64-v3")),
            "-O3 -DNDEBUG -funroll-loops -march=x86-64-v3"
        );
        assert_eq!(
            release_c_flags("x86_64", "unknown", CpuTarget::Baseline, Some("x86-64-v2")),
            "-O3 -DNDEBUG -funroll-loops -march=x86-64-v2"
        );
        assert_eq!(
            release_c_flags("x86_64", "unknown", CpuTarget::ThisCpu, Some("")),
            "-O3 -DNDEBUG -funroll-loops -march=native"
        );
    }
}
