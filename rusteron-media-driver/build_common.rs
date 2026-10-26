// Shared build-script implementation for rusteron-client / rusteron-archive /
// rusteron-media-driver.
//
// This file is `include!`d by each crate's build.rs (NOT compiled as part of
// rusteron-code-gen), so `cfg!(feature = ...)` and `env!("CARGO_MANIFEST_DIR")` resolve
// against the crate being built — exactly as when the three build scripts were copies.
// Each build.rs supplies a `RusteronBuildConfig` describing what actually varies:
// cmake targets, header path, bindgen extras, and an optional pre-build hook.

use std::path::{Path, PathBuf};
use std::{env, fs};
use walkdir::WalkDir;

/// What differs between the three crates' builds. Everything else is shared below.
pub struct RusteronBuildConfig {
    /// C header directory under aeron/, e.g. "aeron-client/src/main/c".
    pub header_subdir: &'static str,
    /// cmake target (and lib to link) for dynamic / static linking.
    pub target_dynamic: &'static str,
    pub target_static: &'static str,
    /// Additional base library to link, per link type (e.g. the client lib under archive).
    pub base_target_dynamic: Option<&'static str>,
    pub base_target_static: Option<&'static str>,
    /// Extra clang include dir under aeron/ for bindgen (archive/driver need client headers).
    pub extra_clang_include: Option<&'static str>,
    /// Extra bindgen var allowlists beyond `AERON_.*`.
    pub extra_allowlist_vars: &'static [&'static str],
    /// cmake defines applied on top of the shared defaults (later wins).
    pub cmake_defines: &'static [(&'static str, &'static str)],
    /// Work around duplicate symbols when linking two aeron libs (archive).
    pub allow_multiple_definition: bool,
    /// Extra dynamic-link system lib on linux for the precompiled path (driver: "bsd").
    pub precompile_linux_extra_lib: Option<&'static str>,
    /// Skip generating wrappers for these C type names (by predicate on type_name).
    pub wrapper_type_filter: fn(&str) -> bool,
    /// Sanity check: a wrapper type that must exist after parsing (None to skip).
    pub expected_wrapper: Option<&'static str>,
    /// Per-crate hand-written wrapper code appended after the common `aeron_custom.rs`
    /// (e.g. `rusteron_code_gen::CUSTOM_ARCHIVE_CODE`). May reference crate-only types.
    pub extra_custom_code: &'static [&'static str],
    /// Snapshot name under rusteron-code-gen/bindings/ used by COPY_BINDINGS.
    pub bindings_snapshot: &'static str,
    /// Hook before cmake configuration (archive runs gradle here). Receives aeron/ path.
    pub pre_build: fn(&Path),
}

impl RusteronBuildConfig {
    pub fn no_filter(_type_name: &str) -> bool {
        true
    }

    /// Emit wrappers only for Aeron's own types: bindgen also pulls in platform
    /// typedefs (Darwin/glibc pthread internals) that would otherwise become
    /// public wrapper noise in the generated API.
    pub fn aeron_only(type_name: &str) -> bool {
        type_name.starts_with("aeron_")
    }

    pub fn no_pre_build(_aeron_path: &Path) {}
}

#[derive(PartialEq)]
pub enum LinkType {
    Dynamic,
    Static,
}

impl LinkType {
    fn detect() -> LinkType {
        if cfg!(feature = "static") {
            LinkType::Static
        } else {
            LinkType::Dynamic
        }
    }

    fn link_lib(&self) -> &'static str {
        match self {
            LinkType::Dynamic => "dylib=",
            LinkType::Static => "static=",
        }
    }

    fn target_name(&self, config: &RusteronBuildConfig) -> &'static str {
        match self {
            LinkType::Dynamic => config.target_dynamic,
            LinkType::Static => config.target_static,
        }
    }

    fn base_target_name(&self, config: &RusteronBuildConfig) -> Option<&'static str> {
        match self {
            LinkType::Dynamic => config.base_target_dynamic,
            LinkType::Static => config.base_target_static,
        }
    }
}

fn emit_link_libs(link_type: &LinkType, config: &RusteronBuildConfig) {
    println!(
        "cargo:rustc-link-lib={}{}",
        link_type.link_lib(),
        link_type.target_name(config)
    );
    if let Some(base) = link_type.base_target_name(config) {
        println!("cargo:rustc-link-lib={}{}", link_type.link_lib(), base);
    }
    if config.allow_multiple_definition {
        if cfg!(target_os = "linux") {
            println!("cargo:rustc-link-arg=-Wl,--allow-multiple-definition");
        } else if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-arg=/FORCE:MULTIPLE");
        }
    }
}

pub fn rusteron_build_main(config: &RusteronBuildConfig) {
    // C built outside this crate, such as a custom UDP transport, must compile against the
    // headers of the Aeron built here. rusteron-media-driver's `links = "aeron_driver"` key
    // hands these to its direct dependents' build scripts as DEP_AERON_DRIVER_INCLUDE,
    // DEP_AERON_DRIVER_CLIENT_INCLUDE and DEP_AERON_DRIVER_AERON_ROOT; crates without the key
    // ignore them.
    let aeron_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("aeron");
    println!("cargo:include={}", aeron_root.join(config.header_subdir).display());
    println!("cargo:client_include={}", aeron_root.join("aeron-client/src/main/c").display());
    println!("cargo:aeron_root={}", aeron_root.display());

    // Skip build script when building on docs.rs
    let docs_rs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs-rs");
    if std::env::var("DOCS_RS").is_ok() {
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        println!("cargo:warning=docs.rs build detected, skipping build script");
        copy_rs_files(&docs_rs, &out_path);
        return;
    }

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=bindings.h");
    // the submodule's headers aren't tracked, so an Aeron upgrade would otherwise reuse stale bindings
    println!("cargo:rerun-if-changed=aeron/version.txt");
    println!("cargo:rerun-if-env-changed=COPY_BINDINGS");
    println!("cargo:rerun-if-env-changed=PUBLISH_ARTIFACTS");
    println!("cargo:rerun-if-env-changed=RUSTERON_C_MARCH");

    // If precompiled artifacts exist (or can be downloaded), use them instead of cmake+java.
    #[cfg(all(any(feature = "precompile", feature = "precompile-rustls"), feature = "static"))]
    {
        let artifacts_dir = get_artifact_path();
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

        if fs::read_dir(&artifacts_dir)
            .as_mut()
            .map(|s| s.next().is_none())
            .unwrap_or_default()
            && std::env::var_os("RUSTERON_BUILD_FROM_SOURCE").is_none()
            && let Err(e) = download_precompiled_binaries(&artifacts_dir) {
                eprintln!("Error downloading precompiled binaries: {e:?}");
                println!("Error downloading precompiled binaries: {e:?}");
            }
        if artifacts_dir.exists()
            && fs::read_dir(&artifacts_dir)
                .as_mut()
                .map(|s| s.next().is_some())
                .unwrap_or_default()
            && std::env::var_os("RUSTERON_BUILD_FROM_SOURCE").is_none()
        {
            println!(
                "Artifacts found in {}. Using published artifacts.",
                artifacts_dir.display()
            );
            if rusteron_code_gen::c_build::wants_linker_plugin_lto(&env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default()) {
                println!(
                    "cargo:warning=-Clinker-plugin-lto is set but the precompiled Aeron C is native code, \
                     so it cannot be inlined into Rust; set RUSTERON_BUILD_FROM_SOURCE=1 for cross-language LTO"
                );
            }

            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", artifacts_dir.display());
            println!("cargo:rustc-link-search=native={}", artifacts_dir.display());
            let link_type = LinkType::detect();
            emit_link_libs(&link_type, config);

            if pkg_config::probe_library("uuid").is_err() {
                eprintln!("uuid lib not found in path");
            }
            if let LinkType::Static = link_type {
                // On Windows, there are some extra libraries needed for static link
                // that aren't included by Aeron.
                if cfg!(target_os = "windows") {
                    println!("cargo:rustc-link-lib=shell32");
                    println!("cargo:rustc-link-lib=iphlpapi");
                }
                if cfg!(target_os = "linux") {
                    println!("cargo:rustc-link-lib=uuid");
                    println!("cargo:rustc-link-lib=bsd");
                }
            }
            if cfg!(target_os = "linux")
                && let Some(lib) = config.precompile_linux_extra_lib {
                    println!("cargo:rustc-link-lib={lib}");
                }

            // Copy generated Rust files (*.rs) from the artifacts folder into OUT_DIR.
            copy_rs_files(&docs_rs, &out_path);

            // Exit early to skip rebuild since artifacts are already published.
            return;
        }
    }

    build_from_source(config, &docs_rs);
}

fn copy_rs_files(docs_rs: &Path, out_path: &Path) {
    for entry in WalkDir::new(docs_rs) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() && entry.path().extension().map(|s| s == "rs").unwrap_or(false) {
            let file_name = entry.path().file_name().unwrap();
            let dest = out_path.join(file_name);
            fs::copy(entry.path(), dest).expect("Failed to copy generated Rust file from artifacts");
        }
    }
}

#[cfg(feature = "build-from-source")]
fn build_from_source(config: &RusteronBuildConfig, docs_rs: &Path) {
    use bindgen::EnumVariation;
    use cmake::Config;
    use dunce::canonicalize;
    use proc_macro2::TokenStream;
    use rusteron_code_gen::{append_to_file, format_with_rustfmt};

    #[cfg(feature = "static")]
    let publish_binaries = std::env::var("PUBLISH_ARTIFACTS").is_ok();

    if pkg_config::probe_library("uuid").is_err() {
        eprintln!("uuid lib not found in path");
    }

    let aeron_path = canonicalize(Path::new("./aeron")).unwrap();
    let header_path = aeron_path.join(config.header_subdir);
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    (config.pre_build)(&aeron_path);

    let link_type = LinkType::detect();
    emit_link_libs(&link_type, config);

    if let LinkType::Static = link_type {
        // On Windows, there are some extra libraries needed for static link
        // that aren't included by Aeron.
        if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-lib=shell32");
            println!("cargo:rustc-link-lib=iphlpapi");
        }
        if cfg!(target_os = "linux") {
            println!("cargo:rustc-link-lib=uuid");
            // clang turns the ring buffers' packed 64-bit atomics into __atomic_* calls on aarch64
            println!("cargo:rustc-link-lib=atomic");
        }
    }

    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-arg=-Wl,--no-as-needed");
        println!("cargo:rustc-link-lib=bsd");
        println!("cargo:rustc-link-arg=-Wl,--as-needed");
    }

    let mut cmake_config = Config::new(&aeron_path);
    let lto_flag = cross_language_lto(&mut cmake_config, &link_type);
    if std::env::var("PROFILE").unwrap() == "release" {
        cmake_config.profile("Release");
        let var = |name: &str| env::var(name).ok();
        let release_flags = if lto_flag.is_some() {
            let rustflags = var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
            rusteron_code_gen::c_build::lto_release_c_flags(
                &var("CARGO_CFG_TARGET_ARCH").unwrap_or_default(),
                rusteron_code_gen::c_build::rustc_target_cpu(&rustflags),
            )
        } else {
            rusteron_code_gen::c_build::release_c_flags(
                &var("CARGO_CFG_TARGET_ARCH").unwrap_or_default(),
                &var("CARGO_CFG_TARGET_VENDOR").unwrap_or_default(),
                if var("PUBLISH_ARTIFACTS").is_some() {
                    rusteron_code_gen::c_build::CpuTarget::Baseline
                } else {
                    rusteron_code_gen::c_build::CpuTarget::ThisCpu
                },
                var("RUSTERON_C_MARCH").as_deref(),
            )
        };
        cmake_config.define("CMAKE_CXX_FLAGS_RELEASE", &release_flags);
        cmake_config.define("CMAKE_C_FLAGS_RELEASE", &release_flags);
    } else {
        cmake_config.profile("Debug");
    }
    // Support for custom GRADLE_WRAPPER path
    // This allows static builds to use a different gradlew script, useful for isolated network environments
    if let Ok(gradle_wrapper) = std::env::var("GRADLE_WRAPPER") {
        cmake_config.define("GRADLE_WRAPPER", gradle_wrapper);
    }

    // Sanitizer support — the `sanitize-address` feature is the discoverable way to turn
    // this on; RUSTERON_SANITIZE remains for other sanitizers (thread, undefined, ...).
    // This injects -fsanitize=* flags into the C/C++ compilation via cmake.
    // -fexceptions lets a Rust panic pass safely through the Aeron poll functions
    // (UNWIND_POLL_FNS). Without it that is undefined behaviour
    #[allow(unused_mut)]
    let mut c_flags = "-fcommon -fexceptions".to_string();
    let mut cxx_flags = String::new();
    println!("cargo:rerun-if-env-changed=RUSTERON_SANITIZE");
    let san_env = std::env::var("RUSTERON_SANITIZE").ok().filter(|s| !s.is_empty());
    let san = if std::env::var("CARGO_FEATURE_SANITIZE_ADDRESS").is_ok() {
        Some(san_env.unwrap_or_else(|| "address".to_string()))
    } else {
        san_env
    };
    if let Some(san) = san {
        let san_flags = format!("-fsanitize={} -fno-omit-frame-pointer -g -O1", san);
        c_flags = format!("{} {}", san_flags, c_flags);
        cxx_flags = san_flags;
        // The instrumented Aeron C objects reference __asan_* symbols, so the
        // crate's test/example link step needs an ASan runtime on the link line.
        // cargo:rustc-link-arg scopes this to THIS crate's own binaries — build
        // scripts and proc-macros are not instrumented and stay unaffected. We do
        // NOT use `-Z sanitizer=address` (rustc's own runtime): on Linux CI the
        // bundled LLVM (20+) is ABI-incompatible with Ubuntu's clang (14), so two
        // runtimes abort at startup with "incompatible ASan runtimes". Instead we
        // link the single clang/gcc libasan whose __asan_* API matches the
        // clang-instrumented C objects.
        if san == "address" {
            match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
                Ok("macos") => {
                    if let Ok(output) =
                        std::process::Command::new("clang").arg("-print-resource-dir").output()
                    {
                        let resource_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
                        if !resource_dir.is_empty() {
                            println!("cargo:rustc-link-search=native={resource_dir}/lib/darwin");
                            println!("cargo:rustc-link-lib=dylib=clang_rt.asan_osx_dynamic");
                        }
                    }
                }
                Ok("linux") => {
                    // Link clang's ASan runtime and output its path for LD_PRELOAD.
                    // We use clang's resource-dir to find the exact libclang_rt.asan
                    // path, then emit it so the CI workflow can LD_PRELOAD it to ensure
                    // the ASan runtime loads before any other library (required to avoid
                    // "ASan runtime does not come first in initial library list" errors).
                    if let Ok(output) =
                        std::process::Command::new("clang").arg("-print-resource-dir").output()
                    {
                        let resource_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
                        if !resource_dir.is_empty() {
                            // Try both possible paths for the ASan runtime library
                            let asan_path = format!("{}/lib/x86_64-unknown-linux-gnu/libclang_rt.asan.so", resource_dir);
                            let asan_path_legacy = format!("{}/lib/libclang_rt.asan-x86_64.so", resource_dir);

                            if std::path::Path::new(&asan_path).exists() {
                                println!("cargo:rustc-link-search=native={resource_dir}/lib/x86_64-unknown-linux-gnu");
                                println!("cargo:rustc-link-lib=dylib=clang_rt.asan");
                                println!("cargo:warning=ASAN_RUNTIME_PATH={}", asan_path);
                            } else if std::path::Path::new(&asan_path_legacy).exists() {
                                println!("cargo:rustc-link-search=native={resource_dir}/lib");
                                println!("cargo:rustc-link-lib=dylib=clang_rt.asan-x86_64");
                                println!("cargo:warning=ASAN_RUNTIME_PATH={}", asan_path_legacy);
                            } else {
                                // Fallback to system libasan if clang's runtime isn't found
                                println!("cargo:rustc-link-lib=asan");
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    if let Some(lto_flag) = lto_flag {
        c_flags = format!("{c_flags} {lto_flag}");
    }
    cmake_config.define("CMAKE_C_FLAGS", &c_flags);
    if !cxx_flags.is_empty() {
        cmake_config.define("CMAKE_CXX_FLAGS", &cxx_flags);
    }

    cmake_config
        .define("BUILD_AERON_DRIVER", "OFF")
        .define("BUILD_AERON_ARCHIVE_API", "OFF")
        .define("AERON_TESTS", "OFF")
        .define("AERON_BUILD_SAMPLES", "OFF")
        .define("AERON_BUILD_DOCUMENTATION", "OFF");
    for (key, value) in config.cmake_defines {
        cmake_config.define(key, value);
    }
    let cmake_output = cmake_config.build_target(link_type.target_name(config)).build();

    // Trying to figure out the final path is a bit weird;
    // For Linux/OSX, it's just build/lib
    // For Windows, the .lib file is in build/lib/{profile}, but the DLL
    // is shipped in build/binaries/{profile}
    let base_lib_dir = cmake_output.join("build");
    println!("cargo:rustc-link-search=native={}", base_lib_dir.join("lib").display());
    // Because the `cmake_output` path is different for debug/release, we're not worried
    // about accidentally linking the Debug library when this is a release build or vice-versa
    for sub in ["lib/Debug", "binaries/Debug", "lib/Release", "binaries/Release"] {
        println!("cargo:rustc-link-search=native={}", base_lib_dir.join(sub).display());
    }

    let mut builder = bindgen::Builder::default()
        .clang_arg(format!("-I{}", header_path.display()))
        // Match the CMAKE_C_STANDARD 11 used to actually compile the Aeron C sources.
        .clang_arg("-std=gnu11");
    // On Linux, libclang can end up resolving `<stdatomic.h>` to GCC's own
    // copy (found via the default system include path) instead of the one
    // bundled with the libclang/clang version actually doing the parsing.
    if cfg!(target_os = "linux")
        && let Ok(output) = std::process::Command::new("clang").arg("-print-resource-dir").output()
    {
        let resource_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !resource_dir.is_empty() {
            builder = builder.clang_arg(format!("-resource-dir={resource_dir}"));
        }
    }
    let mut builder = builder
        .header("bindings.h")
        .allowlist_function("aeron_.*")
        .allowlist_type("aeron_.*")
        .allowlist_var("AERON_.*")
        .rustified_enum("aeron_.*_enum")
        .default_enum_style(EnumVariation::Rust { non_exhaustive: false })
        .derive_debug(true)
        .derive_copy(true)
        .derive_eq(true)
        .derive_default(true)
        .derive_hash(true)
        .derive_partialeq(true);
    if let Some(include) = config.extra_clang_include {
        // We need to include some of the headers from `aeron c client`, so update the include path here
        builder = builder.clang_arg(format!("-I{}", aeron_path.join(include).display()));
    }
    for var in config.extra_allowlist_vars {
        builder = builder.allowlist_var(*var);
    }
    // Only UNWIND_POLL_FNS let a panic through, so only they and their handler types are
    // `C-unwind`. Every other handler catches its panic before returning to C. Names are
    // anchored (^...$) for exact matches
    for name in rusteron_code_gen::UNWIND_HANDLER_TYPES
        .iter()
        .chain(rusteron_code_gen::UNWIND_POLL_FNS)
    {
        builder = builder.override_abi(bindgen::Abi::CUnwind, format!("^{name}$"));
    }
    let bindings = builder.generate().expect("Unable to generate aeron bindings");

    let out = out_path.join("bindings.rs");
    bindings.write_to_file(out.clone()).expect("Couldn't write bindings!");

    let mut bindings = rusteron_code_gen::parse_bindings_with_custom(&out, config.extra_custom_code);
    if let Some(expected) = config.expected_wrapper {
        assert_eq!(
            expected,
            bindings.wrappers.get(expected).unwrap().type_name,
            "expected wrapper {expected} missing from parsed bindings"
        );
    }
    let aeron = out_path.join("aeron.rs");

    // include custom aeron code
    let aeron_custom = out_path.join("aeron_custom.rs");

    let _ = fs::remove_file(aeron_custom.clone());
    append_to_file(aeron_custom.to_str().unwrap(), rusteron_code_gen::CUSTOM_AERON_CODE).unwrap();
    for code in config.extra_custom_code {
        append_to_file(aeron_custom.to_str().unwrap(), code).unwrap();
    }

    let _ = fs::remove_file(aeron.clone());
    let mut stream = TokenStream::new();
    let bindings_copy = bindings.clone();
    for handler in bindings.handlers.iter_mut() {
        // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
        let _ = rusteron_code_gen::generate_handlers(handler, &bindings_copy);
    }
    for (p, w) in bindings
        .wrappers
        .values()
        .filter(|w| (config.wrapper_type_filter)(&w.type_name))
        .enumerate()
    {
        let code =
            rusteron_code_gen::generate_rust_code(w, &bindings.wrappers, p == 0, false, true, &bindings.handlers);
        stream.extend(code);
    }
    let bindings_copy = bindings.clone();
    for handler in bindings.handlers.iter_mut() {
        let code = rusteron_code_gen::generate_handlers(handler, &bindings_copy);
        stream.extend(code);
    }

    let generated_code = stream.to_string();
    if generated_code.trim().is_empty() {
        panic!("ERROR: Generated code is empty! This indicates a problem with code generation.");
    }

    // fail fast if it fails to format (usually due to invalid rust code)
    let debug_file = out_path.join("aeron_unformatted.rs");
    std::fs::write(&debug_file, &generated_code).expect("Failed to write debug file");
    eprintln!("Saved unformatted code to: {}", debug_file.display());

    let formatted_code = match format_with_rustfmt(&generated_code) {
        Ok(code) if !code.trim().is_empty() => code,
        Ok(_) => {
            eprintln!("WARNING: rustfmt returned empty output, using unformatted code");
            eprintln!(
                "First 1000 chars of generated code: {}",
                &generated_code[..generated_code.len().min(1000)]
            );
            panic!("rustfmt returned empty output - likely syntax error in generated code");
        }
        Err(e) => {
            eprintln!("WARNING: rustfmt failed with error: {e}");
            eprintln!(
                "First 1000 chars of generated code: {}",
                &generated_code[..generated_code.len().min(1000)]
            );
            panic!("rustfmt failed - likely syntax error in generated code: {e}");
        }
    };

    let _ = std::fs::remove_file(debug_file);

    append_to_file(aeron.to_str().unwrap(), &formatted_code).expect("Failed to write generated code to file");

    if std::env::var("COPY_BINDINGS").is_ok() {
        // Deliberate snapshot regeneration: refresh BOTH committed snapshots from
        // this build's output so they stay mutually consistent. Must be gated —
        // bindings/ and docs-rs/ are platform-specific (e.g. Darwin vs Linux
        // pthread types), so writing them on every build would corrupt whichever
        // platform differs from the committed snapshots and trip the snapshot
        // tests in rusteron-code-gen on CI.
        let cargo_base_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let custom_bindings_path = cargo_base_dir.join(format!("../rusteron-code-gen/bindings/{}", config.bindings_snapshot));
        if custom_bindings_path.exists() {
            fs::copy(out.clone(), custom_bindings_path.clone()).unwrap_or_else(|_| {
                panic!(
                    "Failed to override bindings.rs with custom bindings from {}",
                    config.bindings_snapshot
                )
            });
        } else {
            eprintln!(
                "Warning: Custom bindings not found at: {}",
                custom_bindings_path.display()
            );
        }

        // copy generated source so docs.rs / precompile paths don't need to build C
        let _ = std::fs::create_dir_all(docs_rs);
        for rs in [&aeron, &aeron_custom, &out] {
            fs::copy(rs, docs_rs.join(rs.file_name().unwrap())).expect("Failed to copy source code for docs-rs");
        }
    }

    #[cfg(feature = "static")]
    if publish_binaries {
        let cmake_lib_dir = cmake_output;
        publish_artifacts(&cmake_lib_dir).expect("Failed to publish artifacts");
    }
}

/// When the final binary links with `-Clinker-plugin-lto`, compiles Aeron C to LLVM bitcode
/// with a clang whose LLVM matches rustc's, so the linker can inline it into Rust callers.
/// Returns the flags (`-flto`, `--target`) to add to the C flags.
#[cfg(feature = "build-from-source")]
fn cross_language_lto(cmake_config: &mut cmake::Config, link_type: &LinkType) -> Option<String> {
    use rusteron_code_gen::c_build::{c_lto_flags, llvm_major, wants_linker_plugin_lto};
    use std::process::Command;
    println!("cargo:rerun-if-env-changed=RUSTERON_CLANG");
    println!("cargo:rerun-if-env-changed=RUSTERON_C_LTO");

    if !wants_linker_plugin_lto(&env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default()) {
        return None;
    }
    if *link_type == LinkType::Dynamic {
        println!(
            "cargo:warning=-Clinker-plugin-lto is set but Aeron C is linked as a shared library, \
             so it cannot be inlined into Rust; enable rusteron's `static` feature for cross-language LTO"
        );
        return None;
    }
    let lto_flag = c_lto_flags(
        env::var("RUSTERON_C_LTO").ok().as_deref(),
        &env::var("TARGET").expect("cargo sets TARGET for build scripts"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let output = |program: &str, args: &[&str]| {
        Command::new(program)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
    };
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let rustc_llvm = output(&rustc, &["-vV"])
        .as_deref()
        .and_then(llvm_major)
        .expect("cannot read rustc's LLVM version from `rustc -vV`");
    let clang = env::var("RUSTERON_CLANG")
        .or_else(|_| env::var("CC"))
        .unwrap_or_else(|_| "clang".into());
    let fix = format!(
        "-Clinker-plugin-lto (cross-language LTO) needs clang with LLVM {rustc_llvm}, as rustc uses: \
         install it (Linux: apt.llvm.org `clang-{rustc_llvm} lld-{rustc_llvm} llvm-{rustc_llvm}`; \
         macOS: `brew install llvm@{rustc_llvm}`) and set RUSTERON_CLANG to it, \
         or remove -Clinker-plugin-lto from RUSTFLAGS"
    );
    let version = output(&clang, &["--version"]).unwrap_or_else(|| panic!("`{clang} --version` failed. {fix}"));
    // Apple's clang numbers its own LLVM fork, whose bitcode rustc's LLVM cannot read
    if version.contains("Apple clang") || llvm_major(&version) != Some(rustc_llvm) {
        panic!("{clang} is `{}`. {fix}", version.lines().next().unwrap_or_default());
    }
    let tool = |name: &str| {
        let path = output(&clang, &[&format!("-print-prog-name={name}")]).unwrap_or_default();
        let path = path.trim();
        if !path.contains(std::path::MAIN_SEPARATOR) {
            panic!("{name} not found next to {clang}. {fix}");
        }
        path.to_string()
    };
    cmake_config
        .define("CMAKE_C_COMPILER", &clang)
        .define("CMAKE_AR", tool("llvm-ar"))
        .define("CMAKE_RANLIB", tool("llvm-ranlib"));
    println!("cargo:warning=cross-language LTO: Aeron C compiled to bitcode by {clang} (LLVM {rustc_llvm}) with {lto_flag}");
    Some(lto_flag)
}

#[cfg(not(feature = "build-from-source"))]
fn build_from_source(_config: &RusteronBuildConfig, _docs_rs: &Path) {
    panic!(
        "No build method available: enable the `build-from-source` feature to build Aeron C from \
         source, or enable `precompile` + `static` to use precompiled binaries."
    );
}

#[allow(dead_code)]
fn get_artifact_path() -> PathBuf {
    let feature = if LinkType::detect() == LinkType::Static {
        "static"
    } else {
        "default"
    };
    let mut target_os = env::var("CARGO_CFG_TARGET_OS").unwrap(); // e.g., "macos", "linux", "windows"
    if target_os == "linux" {
        target_os = "ubuntu".to_string();
    }
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap(); // e.g., "x86_64", "aarch64"
    let artifacts_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts")
        .join(feature)
        .join(&target_os)
        .join(&target_arch);
    let _ = fs::create_dir_all(&artifacts_dir);
    artifacts_dir
}

#[cfg(feature = "build-from-source")]
#[allow(dead_code)]
fn publish_artifacts(cmake_build_path: &Path) -> std::io::Result<()> {
    let publish_dir = get_artifact_path();

    let lib_extensions = ["a", "so", "dylib", "lib"];

    let mut libs_copied = 0;
    for entry in WalkDir::new(cmake_build_path) {
        if entry.is_err() {
            continue;
        }
        let entry = entry.unwrap();
        if entry.file_type().is_file()
            && let Some(ext) = entry.path().extension()
            && lib_extensions.iter().any(|&e| ext == e)
        {
            // Copy file preserving its file name.
            let file_name = entry.path().file_name().unwrap();
            fs::copy(entry.path(), publish_dir.join(file_name))?;
            libs_copied += 1;
        }
    }

    assert!(libs_copied > 0, "No libraries found in the cmake build directory.");
    println!("Artifacts published to: {}", publish_dir.display());
    Ok(())
}

#[cfg(all(any(feature = "precompile", feature = "precompile-rustls"), feature = "static"))]
fn download_precompiled_binaries(artifacts_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let version = env::var("CARGO_PKG_VERSION").unwrap();
    let mut target_os = env::var("CARGO_CFG_TARGET_OS").unwrap(); // e.g., "macos", "linux", "windows"
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap(); // e.g., "x86_64", "aarch64"
    let feature = if LinkType::detect() == LinkType::Static {
        "static"
    } else {
        "default"
    };

    let mut image = if target_os == "macos" && arch == "x86_64" {
        "13"
    } else {
        "latest"
    };

    if target_os == "linux" {
        target_os = "ubuntu".to_string();
        image = "22.04";
    }

    let asset = format!("https://github.com/gsrxyz/rusteron/releases/download/v{version}/artifacts-{target_os}-{image}-{feature}.tar.gz");

    println!("downloading from {asset}");
    eprintln!("downloading from {asset}");
    // Download and extract the tar.gz to the artifacts directory
    // Download and unpack the tar.gz in one go
    let response = reqwest::blocking::get(&asset)?.error_for_status()?;
    let bytes = response.bytes()?;
    let cursor = std::io::Cursor::new(bytes);
    let decoder = flate2::bufread::GzDecoder::new(cursor);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(artifacts_dir)?;

    // move files we are interested in
    let pkg_name = std::env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME should always be set by Cargo");
    let dir = fs::read_dir(
        artifacts_dir
            .join(format!("artifacts-{target_os}-{image}-{feature}"))
            .join(pkg_name)
            .join("artifacts")
            .join(feature)
            .join(target_os)
            .join(arch),
    )?;
    for file in dir {
        let file = file?;
        fs::rename(file.path(), artifacts_dir.join(file.file_name()))?;
    }

    println!("extracted to {artifacts_dir:?}");
    eprintln!("extracted to {artifacts_dir:?}");

    Ok(())
}
