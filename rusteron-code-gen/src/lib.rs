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
mod plan;
pub mod test_logger;

pub use common::*;
pub use generator::*;
pub use parser::*;
pub use plan::*;

use proc_macro2::TokenStream;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub const CUSTOM_AERON_CODE: &str = include_str!("./aeron_custom.rs");
pub const CUSTOM_ARCHIVE_CODE: &str = include_str!("./aeron_custom_archive.rs");
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
        // each crate's committed aeron_custom.rs = the common custom code plus any
        // per-crate custom code appended by its build config (archive only, today)
        let no_extra: &[&str] = &[];
        for (crate_name, extra) in [
            ("client", no_extra),
            ("archive", &[crate::CUSTOM_ARCHIVE_CODE][..]),
            ("media-driver", no_extra),
        ] {
            // emulate append_to_file: each chunk is written as "\n{chunk}\n"
            let mut source = format!("\n{}\n", CUSTOM_AERON_CODE);
            for code in extra {
                source.push_str(&format!("\n{}\n", code));
            }
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../rusteron-{crate_name}/docs-rs/aeron_custom.rs"));
            let committed = fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("missing committed aeron_custom.rs: {}", path.display()));
            assert_eq!(
                committed.trim(),
                source.trim(),
                "rusteron-{crate_name}/docs-rs/aeron_custom.rs drifted from \
                 rusteron-code-gen/src/aeron_custom*.rs. Rebuild with `COPY_BINDINGS=true`.",
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
    use crate::{CResource, ManagedCResource};

    use std::cell::Cell;
    use std::rc::Rc;
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

        let resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
        );
        assert!(resource.is_ok());

        if let Ok(ref resource) = resource {
            assert!(resource.close_shared().is_ok());
        }

        // Reset the flag to ensure drop does not call cleanup a second time.
        flag.store(false, Ordering::SeqCst);
        drop(resource);
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn close_resource_closes_shared_resource_once_across_clones() {
        let close_count = Rc::new(Cell::new(0));
        let close_count_for_cleanup = close_count.clone();
        let resource_ptr = make_resource(40);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            close_count_for_cleanup.set(close_count_for_cleanup.get() + 1);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
        );
        assert!(resource.is_ok());
        let resource = resource.ok().unwrap();
        let inner = Rc::new(resource);
        let handle_one = CResource::OwnedOnHeap(inner.clone());
        let handle_two = CResource::OwnedOnHeap(inner);

        assert!(!handle_one.get().is_null());
        assert!(!handle_two.get().is_null());

        assert!(handle_one.close_resource().is_ok());
        assert_eq!(1, close_count.get());
        assert!(handle_one.get().is_null());
        assert!(handle_two.get().is_null());

        assert!(handle_two.close_resource().is_ok());
        assert_eq!(1, close_count.get());
    }

    #[test]
    fn close_resource_defers_owner_close_while_dependent_is_alive() {
        let close_count = Rc::new(Cell::new(0));
        let close_count_for_cleanup = close_count.clone();
        let owner_ptr = make_resource(60);
        let child_ptr = make_resource(61);

        let owner_cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            close_count_for_cleanup.set(close_count_for_cleanup.get() + 1);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let owner = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = owner_ptr;
                }
                0
            },
            owner_cleanup,
            false,
        );
        assert!(owner.is_ok());
        let owner = CResource::OwnedOnHeap(Rc::new(owner.ok().unwrap()));

        let child = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = child_ptr;
                }
                0
            },
            Some(Box::new(move |res| {
                unsafe {
                    reclaim_resource(*res);
                    *res = std::ptr::null_mut();
                }
                0
            })),
            false,
        );
        assert!(child.is_ok());
        let child = CResource::OwnedOnHeap(Rc::new(child.ok().unwrap()));
        child.add_dependency(owner.clone());

        assert!(owner.close_resource_deferred_if_shared().is_ok());
        assert_eq!(0, close_count.get());
        assert!(!owner.get().is_null());

        drop(owner);
        drop(child);
        assert_eq!(1, close_count.get());
    }

    #[test]
    fn close_resource_deferral_is_retryable_if_owner_close_fails_after_dependent_drops() {
        let close_count = Rc::new(Cell::new(0));
        let close_count_for_cleanup = close_count.clone();
        let owner_ptr = make_resource(70);
        let child_ptr = make_resource(71);

        let owner_cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            let count = close_count_for_cleanup.get() + 1;
            close_count_for_cleanup.set(count);
            if count == 1 {
                return -1;
            }
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let owner = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = owner_ptr;
                }
                0
            },
            owner_cleanup,
            false,
        );
        assert!(owner.is_ok());
        let owner = CResource::OwnedOnHeap(Rc::new(owner.ok().unwrap()));
        let owner_retry_handle = owner.clone();

        let child = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = child_ptr;
                }
                0
            },
            Some(Box::new(move |res| {
                unsafe {
                    reclaim_resource(*res);
                    *res = std::ptr::null_mut();
                }
                0
            })),
            false,
        );
        assert!(child.is_ok());
        let child = CResource::OwnedOnHeap(Rc::new(child.ok().unwrap()));
        child.add_dependency(owner.clone());

        assert!(owner.close_resource_deferred_if_shared().is_ok());
        drop(owner);
        drop(child);
        assert_eq!(0, close_count.get());
        assert!(!owner_retry_handle.get().is_null());

        assert!(owner_retry_handle.close_resource_deferred_if_shared().is_err());
        assert_eq!(1, close_count.get());
        assert!(!owner_retry_handle.get().is_null());

        assert!(owner_retry_handle.close_resource_deferred_if_shared().is_ok());
        assert_eq!(2, close_count.get());
        assert!(owner_retry_handle.get().is_null());
    }

    #[test]
    fn close_resource_propagates_failure_and_allows_retry() {
        let close_count = Rc::new(Cell::new(0));
        let close_count_for_cleanup = close_count.clone();
        let resource_ptr = make_resource(50);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            let count = close_count_for_cleanup.get() + 1;
            close_count_for_cleanup.set(count);
            if count == 1 {
                return -1;
            }
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
        );
        assert!(resource.is_ok());
        let resource = resource.ok().unwrap();
        let handle = CResource::OwnedOnHeap(Rc::new(resource));

        assert!(handle.close_resource().is_err());
        assert_eq!(1, close_count.get());
        assert!(!handle.get().is_null());

        assert!(handle.close_resource().is_ok());
        assert_eq!(2, close_count.get());
        assert!(handle.get().is_null());
    }

    #[test]
    fn close_resource_with_uses_custom_cleanup_once_across_clones() {
        let default_close_count = Rc::new(Cell::new(0));
        let custom_close_count = Rc::new(Cell::new(0));
        let default_close_count_for_cleanup = default_close_count.clone();
        let resource_ptr = make_resource(80);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            default_close_count_for_cleanup.set(default_close_count_for_cleanup.get() + 1);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
        );
        assert!(resource.is_ok());
        let inner = Rc::new(resource.ok().unwrap());
        let handle_one = CResource::OwnedOnHeap(inner.clone());
        let handle_two = CResource::OwnedOnHeap(inner);

        let custom_close_count_for_cleanup = custom_close_count.clone();
        assert!(handle_one
            .close_resource_with(move |res| {
                custom_close_count_for_cleanup.set(custom_close_count_for_cleanup.get() + 1);
                unsafe {
                    reclaim_resource(*res);
                    *res = std::ptr::null_mut();
                }
                0
            })
            .is_ok());

        assert_eq!(0, default_close_count.get());
        assert_eq!(1, custom_close_count.get());
        assert!(handle_one.get().is_null());
        assert!(handle_two.get().is_null());

        assert!(handle_two.close_resource().is_ok());
        assert_eq!(0, default_close_count.get());
        assert_eq!(1, custom_close_count.get());
    }

    #[test]
    fn close_resource_with_failure_restores_default_cleanup_for_retry() {
        let default_close_count = Rc::new(Cell::new(0));
        let custom_close_count = Rc::new(Cell::new(0));
        let default_close_count_for_cleanup = default_close_count.clone();
        let resource_ptr = make_resource(90);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            default_close_count_for_cleanup.set(default_close_count_for_cleanup.get() + 1);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
        );
        assert!(resource.is_ok());
        let handle = CResource::OwnedOnHeap(Rc::new(resource.ok().unwrap()));

        let custom_close_count_for_cleanup = custom_close_count.clone();
        assert!(handle
            .close_resource_with(move |_res| {
                custom_close_count_for_cleanup.set(custom_close_count_for_cleanup.get() + 1);
                -1
            })
            .is_err());

        assert_eq!(0, default_close_count.get());
        assert_eq!(1, custom_close_count.get());
        assert!(!handle.get().is_null());

        assert!(handle.close_resource().is_ok());
        assert_eq!(1, default_close_count.get());
        assert_eq!(1, custom_close_count.get());
        assert!(handle.get().is_null());
    }

    #[test]
    fn close_resource_is_noop_for_stack_and_borrowed_resources() {
        let mut borrowed_value = 10;
        let borrowed = CResource::Borrowed(&mut borrowed_value as *mut i32);
        assert!(borrowed.close_resource().is_ok());
        assert_eq!(10, borrowed_value);

        let stack = CResource::OwnedOnStack(std::mem::MaybeUninit::new(20));
        assert!(stack.close_resource().is_ok());
        assert_eq!(20, unsafe { *stack.get() });
    }

    // NOTE: test_drop_does_not_call_cleanup_if_check_for_is_closed_* removed
    // because check_for_is_closed was deleted — the cleanup closure + Rc graph
    // are now the sole teardown mechanism (see rusteron-code-gen/src/common.rs).
}
