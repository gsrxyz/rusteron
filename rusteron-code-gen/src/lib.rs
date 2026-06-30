#![allow(improper_ctypes_definitions)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]

mod common;
mod generator;
mod parser;
pub mod test_logger;

pub use common::*;
pub use generator::*;
pub use parser::*;

use proc_macro2::TokenStream;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub const CUSTOM_AERON_CODE: &str = include_str!("./aeron_custom.rs");
pub const COMMON_CODE: &str = include_str!("./common.rs");

pub fn append_to_file(file_path: &str, code: &str) -> std::io::Result<()> {
    let path = Path::new(file_path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut file = OpenOptions::new().create(true).write(true).append(true).open(path)?;

    writeln!(file, "\n{}", code)?;

    Ok(())
}

#[allow(dead_code)]
pub fn format_with_rustfmt(code: &str) -> Result<String, std::io::Error> {
    let mut rustfmt = Command::new("rustfmt")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = rustfmt.stdin.take() {
        stdin.write_all(code.as_bytes())?;
    }

    let output = rustfmt.wait_with_output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("rustfmt failed: {}", stderr),
        ));
    }

    let formatted_code = String::from_utf8_lossy(&output.stdout).to_string();

    // If the input was non-empty but the output is empty, that's an error
    // But if the input was empty/whitespace only, empty output is fine
    if !code.trim().is_empty() && formatted_code.trim().is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "rustfmt produced empty output",
        ))
    } else {
        Ok(formatted_code)
    }
}

#[allow(dead_code)]
pub fn format_token_stream(tokens: TokenStream) -> String {
    let code = tokens.to_string();

    match format_with_rustfmt(&code) {
        Ok(formatted_code) if !formatted_code.trim().is_empty() => formatted_code,
        _ => code.replace("{", "{\n"), // Fallback to unformatted code in case of error
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::MEDIA_DRIVER_BINDINGS;
    use crate::parser::parse_bindings;
    use crate::{
        append_to_file, format_token_stream, format_with_rustfmt, ARCHIVE_BINDINGS, CLIENT_BINDINGS, CUSTOM_AERON_CODE,
    };
    use proc_macro2::TokenStream;
    use std::fs;

    // valgrind can give false positives, so we don't want to run on tests which are 100% rust
    // and do not have any chance of any undefined behaviour i.e. parsing rs and generating code
    fn running_under_valgrind() -> bool {
        std::env::var_os("RUSTERON_VALGRIND").is_some()
    }

    #[test]
    #[cfg(not(target_os = "windows"))] // the generated bindings have different sizes
    fn client() {
        if running_under_valgrind() {
            return;
        }

        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("bindings")
            .join("client.rs");
        let mut bindings = parse_bindings(&path);
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );
        assert_eq!(
            0,
            bindings.methods.len(),
            "expected all methods to have been matched {:#?}",
            bindings.methods
        );

        let file = write_to_file(TokenStream::new(), true, "client.rs");
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        for (p, w) in bindings.wrappers.values().enumerate() {
            let code = crate::generate_rust_code(w, &bindings.wrappers, p == 0, true, true, &bindings.handlers);
            write_to_file(code, false, "client.rs");
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            append_to_file(&file, &format_with_rustfmt(&code.to_string()).unwrap()).unwrap();
        }

        let t = trybuild::TestCases::new();
        append_to_file(&file, "use bindings::*; mod bindings { ").unwrap();
        append_to_file(&file, CLIENT_BINDINGS).unwrap();
        append_to_file(&file, "}").unwrap();
        append_to_file(&file, CUSTOM_AERON_CODE).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(file)
    }

    #[test]
    #[cfg(not(target_os = "windows"))] // the generated bindings have different sizes
    fn media_driver() {
        if running_under_valgrind() {
            return;
        }

        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("bindings")
            .join("media-driver.rs");
        let mut bindings = parse_bindings(&path);
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );

        let file = write_to_file(TokenStream::new(), true, "md.rs");

        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        for (p, w) in bindings
            .wrappers
            .values()
            .filter(|w| !w.type_name.contains("_t_") && w.type_name != "in_addr")
            .enumerate()
        {
            let code = crate::generate_rust_code(w, &bindings.wrappers, p == 0, true, true, &bindings.handlers);
            write_to_file(code, false, "md.rs");
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            append_to_file(&file, &format_with_rustfmt(&code.to_string()).unwrap()).unwrap();
        }
        let t = trybuild::TestCases::new();
        append_to_file(&file, "use bindings::*; mod bindings { ").unwrap();
        append_to_file(&file, MEDIA_DRIVER_BINDINGS).unwrap();
        append_to_file(&file, "}").unwrap();
        append_to_file(&file, CUSTOM_AERON_CODE).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(&file)
    }

    #[test]
    #[cfg(not(target_os = "windows"))] // the generated bindings have different sizes
    fn archive() {
        if running_under_valgrind() {
            return;
        }

        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("bindings")
            .join("archive.rs");
        let mut bindings = parse_bindings(&path);
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );

        let file = write_to_file(TokenStream::new(), true, "archive.rs");
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        for (p, w) in bindings.wrappers.values().enumerate() {
            let code = crate::generate_rust_code(w, &bindings.wrappers, p == 0, true, true, &bindings.handlers);
            write_to_file(code, false, "archive.rs");
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            append_to_file(&file, &format_with_rustfmt(&code.to_string()).unwrap()).unwrap();
        }
        let t = trybuild::TestCases::new();
        append_to_file(&file, "use bindings::*; mod bindings { ").unwrap();
        append_to_file(&file, ARCHIVE_BINDINGS).unwrap();
        append_to_file(&file, "}").unwrap();
        append_to_file(&file, CUSTOM_AERON_CODE).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(file)
    }

    /// Regenerate `aeron.rs` exactly as `build.rs` does (single rustfmt pass over
    /// the whole stream, test-modules disabled) and assert it byte-matches the
    /// committed `docs-rs/aeron.rs`. Catches "forgot to rebuild with
    /// `COPY_BINDINGS=true`" drift as a CI failure instead of a stale docs.rs.
    fn assert_aeron_rs_snapshot_matches(bindings_file: &str, snapshot_rel_path: &str, skip_filter: fn(&str) -> bool) {
        if running_under_valgrind() {
            return;
        }

        let bindings_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("bindings")
            .join(bindings_file);
        let snapshot_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(snapshot_rel_path);

        let mut bindings = parse_bindings(&bindings_path);
        // First pass: populate FnMut(...) names required by generate_rust_code.
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }

        // Mirror build.rs: build ONE stream, format in a single rustfmt pass.
        let mut stream = TokenStream::new();
        for (p, w) in bindings
            .wrappers
            .values()
            .filter(|w| skip_filter(&w.type_name))
            .enumerate()
        {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                false, // build.rs uses `false` (no test modules) for the committed snapshot
                true,
                &bindings.handlers,
            );
            stream.extend(code);
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            stream.extend(code);
        }

        let generated = format_with_rustfmt(&stream.to_string())
            .unwrap_or_else(|_| panic!("rustfmt failed on regenerated {bindings_file}"));
        let committed = fs::read_to_string(&snapshot_path)
            .unwrap_or_else(|_| panic!("missing committed snapshot: {}", snapshot_path.display()));

        // Normalise surrounding whitespace: build.rs writes the snapshot via
        // `append_to_file`, which wraps content in `\n{}\n`, so leading/trailing
        // blank lines are a write artifact, not content drift.
        let norm = |s: &str| s.trim().to_string();
        let (generated, committed) = (norm(&generated), norm(&committed));
        if generated != committed {
            // First diverging line gives a focused failure message.
            let mut line_no = 0;
            for (i, (g, c)) in generated.lines().zip(committed.lines()).enumerate() {
                if g != c {
                    line_no = i + 1;
                    break;
                }
            }
            if line_no == 0 {
                line_no = generated.lines().count().min(committed.lines().count()) + 1;
            }
            panic!(
                "docs-rs snapshot drift in {}: generated aeron.rs differs from committed \
                 {} (first divergence near line {}). GENERATED first 3 lines:\n{}\n\
                 COMMITTED first 3 lines:\n{}\nRebuild with `COPY_BINDINGS=true` \
                 (e.g. `just build`) and commit the regenerated snapshot.",
                bindings_file,
                snapshot_path.display(),
                line_no,
                generated.lines().take(3).collect::<Vec<_>>().join("\n"),
                committed.lines().take(3).collect::<Vec<_>>().join("\n"),
            );
        }
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn client_aeron_rs_matches_committed_snapshot() {
        assert_aeron_rs_snapshot_matches("client.rs", "../rusteron-client/docs-rs/aeron.rs", |_| true);
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    #[cfg(target_os = "macos")]
    fn archive_aeron_rs_matches_committed_snapshot() {
        assert_aeron_rs_snapshot_matches("archive.rs", "../rusteron-archive/docs-rs/aeron.rs", |_| true);
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    #[cfg(target_os = "macos")]
    fn media_driver_aeron_rs_matches_committed_snapshot() {
        // Mirrors the media_driver trybuild test's filter.
        assert_aeron_rs_snapshot_matches("media-driver.rs", "../rusteron-media-driver/docs-rs/aeron.rs", |t| {
            !t.contains("_t_") && t != "in_addr"
        });
    }

    /// `aeron_custom.rs` is a verbatim copy (not generated) into each crate's
    /// `docs-rs/`. Assert the committed copies stay byte-identical to the source
    /// of truth (`rusteron-code-gen/src/aeron_custom.rs`).
    #[test]
    fn aeron_custom_rs_snapshots_match_source() {
        let source = CUSTOM_AERON_CODE.trim();
        for crate_name in ["client", "archive", "media-driver"] {
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../rusteron-{crate_name}/docs-rs/aeron_custom.rs"));
            let committed = fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("missing committed aeron_custom.rs: {}", path.display()));
            assert_eq!(
                committed.trim(),
                source,
                "rusteron-{crate_name}/docs-rs/aeron_custom.rs drifted from \
                 rusteron-code-gen/src/aeron_custom.rs. Rebuild with `COPY_BINDINGS=true`.",
            );
        }
    }

    fn write_to_file(rust_code: TokenStream, delete: bool, name: &str) -> String {
        let src = format_token_stream(rust_code);
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../target/{}", name));
        let path = path.to_str().unwrap();
        if delete {
            let _ = fs::remove_file(path);
        }
        append_to_file(path, &src).unwrap();
        path.to_string()
    }
}

#[cfg(test)]
mod test {
    use crate::ManagedCResource;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn make_resource(val: i32) -> *mut i32 {
        Box::into_raw(Box::new(val))
    }

    unsafe fn reclaim_resource(ptr: *mut i32) {
        if !ptr.is_null() {
            let _ = Box::from_raw(ptr);
        }
    }

    #[test]
    fn test_drop_calls_cleanup_non_borrowed_no_cleanup_struct() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(10);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            // Set the resource to null to simulate cleanup.
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                false,
                None,
            );
            assert!(_resource.is_ok())
        }
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_calls_cleanup_non_borrowed_with_cleanup_struct() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(20);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                true,
                None,
            );
            assert!(_resource.is_ok())
        }
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_does_not_call_cleanup_if_already_closed() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(30);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let mut resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
            None,
        );
        assert!(resource.is_ok());

        if let Ok(ref mut resource) = &mut resource {
            assert!(resource.close().is_ok())
        }

        // Reset the flag to ensure drop does not call cleanup a second time.
        flag.store(false, Ordering::SeqCst);
        drop(resource);
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_does_not_call_cleanup_if_check_for_is_closed_returns_true() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(60);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let check_fn = Some(|_res: *mut i32| -> bool { true } as fn(_) -> bool);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                false,
                check_fn,
            );
            assert!(_resource.is_ok());
        }
        assert!(!flag.load(Ordering::SeqCst));
        unsafe {
            reclaim_resource(resource_ptr);
        }
    }

    #[test]
    fn test_drop_does_call_cleanup_if_check_for_is_closed_returns_false() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(60);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let check_fn = Some(|_res: *mut i32| -> bool { false } as fn(*mut i32) -> bool);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                false,
                check_fn,
            );
            assert!(_resource.is_ok())
        }
        assert!(flag.load(Ordering::SeqCst));
    }
}
