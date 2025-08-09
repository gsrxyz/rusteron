use bindgen::EnumVariation;
use cmake::Config;
use dunce::canonicalize;
use proc_macro2::TokenStream;
use rusteron_code_gen::{append_to_file, format_with_rustfmt};
use std::path::{Path, PathBuf};
use std::{env, fs};
use walkdir::WalkDir;

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
            LinkType::Static => {
                if cfg!(target_os = "linux") {
                    "" // TODO not sure why I need to do this static= should work on linux based on documentation
                } else {
                    "static="
                }
            }
        }
    }

    fn target_name(&self) -> &'static str {
        match self {
            LinkType::Dynamic => "aeron_driver",
            LinkType::Static => "aeron_driver_static",
        }
    }
}

pub fn main() {
    // Skip build script when building on docs.rs
    let docs_rs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs-rs");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    if std::env::var("DOCS_RS").is_ok() {
        println!("cargo:warning=docs.rs build detected, skipping build script");
        for entry in WalkDir::new(&docs_rs) {
            let entry = entry.unwrap();
            if entry.file_type().is_file()
                && entry.path().extension().map(|s| s == "rs").unwrap_or(false)
            {
                let file_name = entry.path().file_name().unwrap();
                let dest = out_path.join(file_name);
                fs::copy(entry.path(), dest)
                    .expect("Failed to copy generated Rust file from artifacts");
            }
        }
        return;
    }

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=bindings.h");
    // Determine the artifacts folder based on feature, OS, and architecture.
    #[cfg(all(feature = "precompile", feature = "static"))]
    let artifacts_dir = get_artifact_path();

    #[cfg(all(feature = "precompile", feature = "static"))]
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    // If the artifacts folder exists use that instead of doing cmake and requiring java to be installed
    #[cfg(all(feature = "precompile", feature = "static"))]
    if fs::read_dir(&artifacts_dir)
        .as_mut()
        .map(|s| s.next().is_none())
        .unwrap_or_default()
        && std::env::var_os("RUSTERON_BUILD_FROM_SOURCE").is_none()
    {
        if let Err(e) = download_precompiled_binaries(&artifacts_dir) {
            eprintln!("Error downloading precompiled binaries: {e:?}");
            println!("Error downloading precompiled binaries: {e:?}");
        }
    }
    #[cfg(all(feature = "precompile", feature = "static"))]
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

        println!(
            "cargo:rustc-link-arg=-Wl,-rpath,{}",
            artifacts_dir.display()
        );
        println!("cargo:rustc-link-search=native={}", artifacts_dir.display());
        let link_type = LinkType::detect();
        println!(
            "cargo:rustc-link-lib={}{}",
            link_type.link_lib(),
            link_type.target_name()
        );

        if pkg_config::probe_library("uuid").is_err() {
            eprintln!("uuid lib not found in path");
        }
        if cfg!(target_os = "linux") {
            println!("cargo:rustc-link-lib=bsd");
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
            }
        }

        // Copy generated Rust files (*.rs) from the artifacts folder into OUT_DIR.
        for entry in WalkDir::new(&docs_rs) {
            let entry = entry.unwrap();
            if entry.file_type().is_file()
                && entry.path().extension().map(|s| s == "rs").unwrap_or(false)
            {
                let file_name = entry.path().file_name().unwrap();
                let dest = out_path.join(file_name);
                fs::copy(entry.path(), dest)
                    .expect("Failed to copy generated Rust file from artifacts");
            }
        }

        // Exit early to skip rebuild since artifacts are already published.
        return;
    }
    let publish_binaries = std::env::var("PUBLISH_ARTIFACTS").is_ok();
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=bindings.h");

    if pkg_config::probe_library("uuid").is_err() {
        eprintln!("uuid lib not found in path");
    }

    let aeron_path = canonicalize(Path::new("./aeron")).unwrap();
    let header_path = aeron_path.join("aeron-driver/src/main/c");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    let link_type = LinkType::detect();
    println!(
        "cargo:rustc-link-lib={}{}",
        link_type.link_lib(),
        link_type.target_name()
    );

    if let LinkType::Static = link_type {
        // On Windows, there are some extra libraries needed for static link
        // that aren't included by Aeron.
        if cfg!(target_os = "windows") {
            println!("cargo:rustc-link-lib=shell32");
            println!("cargo:rustc-link-lib=iphlpapi");
        }
        if cfg!(target_os = "linux") {
            println!("cargo:rustc-link-lib=uuid");
        }
    }

    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-arg=-Wl,--no-as-needed");
        println!("cargo:rustc-link-lib=bsd");
        println!("cargo:rustc-link-arg=-Wl,--as-needed");
    }

    let mut config = Config::new(&aeron_path);
    if std::env::var("PROFILE").unwrap() == "release" {
        config.profile("Release");
        config.define(
            "CMAKE_CXX_FLAGS_RELEASE",
            if publish_binaries {
                "-O3 -DNDEBUG -march=native -funroll-loops"
            } else {
                "-O3 -DNDEBUG -march=native -funroll-loops -flto"
            },
        );
        config.define(
            "CMAKE_C_FLAGS_RELEASE",
            if publish_binaries {
                "-O3 -DNDEBUG -march=native -funroll-loops"
            } else {
                "-O3 -DNDEBUG -march=native -funroll-loops -flto"
            },
        );
    } else {
        config.profile("Debug");
    }
    let cmake_output = config
        .define("BUILD_AERON_DRIVER", "ON")
        .define("BUILD_AERON_ARCHIVE_API", "OFF")
        .define("AERON_TESTS", "OFF")
        .define("AERON_BUILD_SAMPLES", "OFF")
        .define("AERON_BUILD_DOCUMENTATION", "OFF")
        .build_target(link_type.target_name())
        .build();

    // Trying to figure out the final path is a bit weird;
    // For Linux/OSX, it's just build/lib
    // For Windows, the .lib file is in build/lib/{profile}, but the DLL
    // is shipped in build/binaries/{profile}
    let base_lib_dir = cmake_output.join("build");
    println!(
        "cargo:rustc-link-search=native={}",
        base_lib_dir.join("lib").display()
    );
    // Because the `cmake_output` path is different for debug/release, we're not worried
    // about accidentally linking the Debug library when this is a release build or vice-versa
    println!(
        "cargo:rustc-link-search=native={}",
        base_lib_dir.join("lib/Debug").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        base_lib_dir.join("binaries/Debug").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        base_lib_dir.join("lib/Release").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        base_lib_dir.join("binaries/Release").display()
    );

    println!("cargo:include={}", header_path.display());
    let bindings = bindgen::Builder::default()
        .clang_arg(format!("-I{}", header_path.display()))
        // We need to include some of the headers from `aeron c client`, so update the include path here
        .clang_arg(format!(
            "-I{}",
            aeron_path.join("aeron-client/src/main/c").display()
        ))
        .header("bindings.h")
        .allowlist_function("aeron_.*")
        .allowlist_type("aeron_.*")
        .allowlist_var("AERON_.*")
        .rustified_enum("aeron_.*_enum")
        .default_enum_style(EnumVariation::Rust {
            non_exhaustive: false,
        })
        .derive_debug(true)
        .derive_copy(true)
        .derive_eq(true)
        .derive_default(true)
        .derive_hash(true)
        .derive_partialeq(true)
        .generate()
        .expect("Unable to generate aeron_driver bindings");

    let out = out_path.join("bindings.rs");
    bindings
        .write_to_file(out.clone())
        .expect("Couldn't write bindings!");

    let mut bindings = rusteron_code_gen::parse_bindings(&out);
    assert_eq!(
        "aeron_driver_conductor_t",
        bindings
            .wrappers
            .get("aeron_driver_conductor_t")
            .unwrap()
            .type_name
    );
    let aeron = out_path.join("aeron.rs");
    let _ = fs::remove_file(aeron.clone());

    // include custom aeron code
    let aeron_custom = out_path.join("aeron_custom.rs");
    let _ = fs::remove_file(aeron_custom.clone());
    append_to_file(
        aeron_custom.to_str().unwrap(),
        rusteron_code_gen::CUSTOM_AERON_CODE,
    )
    .unwrap();

    let mut stream = TokenStream::new();
    let bindings_copy = bindings.clone();
    for handler in bindings.handlers.iter_mut() {
        // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
        let _ = rusteron_code_gen::generate_handlers(handler, &bindings_copy);
    }
    for (p, w) in bindings
        .wrappers
        .values()
        .filter(|w| !w.type_name.contains("_t_") && w.type_name != "in_addr")
        .enumerate()
    {
        let code = rusteron_code_gen::generate_rust_code(
            w,
            &bindings.wrappers,
            p == 0,
            false,
            true,
            &bindings.handlers,
        );
        stream.extend(code);
    }
    let bindings_copy = bindings.clone();
    for handler in bindings.handlers.iter_mut() {
        let code = rusteron_code_gen::generate_handlers(handler, &bindings_copy);
        stream.extend(code);
    }
    append_to_file(
        aeron.to_str().unwrap(),
        &format_with_rustfmt(&stream.to_string()).unwrap(),
    )
    .unwrap();
    if std::env::var("COPY_BINDINGS").is_ok() {
        copy_binds(out.clone());
    }

    #[cfg(feature = "static")]
    // media driver libs are too big there is 10meg limit so only do mac os
    if publish_binaries {
        let cmake_lib_dir = cmake_output;
        publish_artifacts(&cmake_lib_dir).expect("Failed to publish artifacts");
    }

    // copy source code so docs-rs does not need to compile it
    let docs_rs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs-rs");
    let _ = std::fs::create_dir_all(&docs_rs);

    for rs in [&aeron, &aeron_custom, &out] {
        fs::copy(rs, docs_rs.join(rs.file_name().unwrap()))
            .expect("Failed to copy source code for docs-rs");
    }
}

// helps with easier testing
fn copy_binds(out: PathBuf) {
    let cargo_base_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let custom_bindings_path = cargo_base_dir.join("../rusteron-code-gen/bindings/media-driver.rs");

    if custom_bindings_path.exists() {
        fs::copy(out.clone(), custom_bindings_path.clone())
            .expect("Failed to override bindings.rs with custom bindings from media-driver.rs");
    } else {
        eprintln!(
            "Warning: Custom bindings not found at: {}",
            custom_bindings_path.display()
        );
    }
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
        if entry.file_type().is_file() {
            if let Some(ext) = entry.path().extension() {
                if lib_extensions.iter().any(|&e| ext == e) {
                    // Copy file preserving its file name.
                    let file_name = entry.path().file_name().unwrap();
                    fs::copy(entry.path(), publish_dir.join(file_name))?;
                    libs_copied += 1;
                }
            }
        }
    }

    assert!(
        libs_copied > 0,
        "No libraries found in the cmake build directory."
    );
    println!("Artifacts published to: {}", publish_dir.display());
    Ok(())
}

#[cfg(all(feature = "precompile", feature = "static"))]
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
    let pkg_name =
        std::env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME should always be set by Cargo");
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
