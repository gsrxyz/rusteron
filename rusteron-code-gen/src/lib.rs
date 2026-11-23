#![allow(improper_ctypes_definitions)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]

mod arg_classifier;
pub mod artifacts;
pub mod c_build;
mod common;
mod constant_enums;
mod convenience;
mod generator;
mod native_enums;
mod native_types;
mod parser;
mod string_views;
pub mod test_logger;
mod thread_safety;

pub use arg_classifier::*;
pub use common::*;
pub use generator::*;
pub use native_types::NativeTypeMetadata;
pub use parser::*;

use proc_macro2::TokenStream;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

pub const CUSTOM_AERON_CODE: &str = include_str!("./aeron_custom.rs");
pub const CUSTOM_ARCHIVE_CODE: &str = include_str!("./aeron_custom_archive.rs");
pub const COMMON_CODE: &str = include_str!("./common.rs");

/// The owned assemblers in `aeron_custom.rs` (`FragmentAssembler`, `AeronImageFragmentAssembler`
/// and their controlled forms) drive the native assemblers directly, so drop the generated
/// wrappers that would otherwise share their names
fn remove_native_assemblers(bindings: &mut CBinding) {
    bindings
        .wrappers
        .retain(|name, _| !name.ends_with("fragment_assembler_t"));
    // Fields pointing at a removed assembler would otherwise get a public raw pointer getter
    for wrapper in bindings.wrappers.values_mut() {
        wrapper
            .fields
            .retain(|field| !field.c_type.contains("fragment_assembler_t"));
    }
    // Without their wrappers the C docs would name the native assembler types, so point
    // them at the hand-written replacements instead
    for wrapper in bindings.wrappers.values_mut() {
        for docs in std::iter::once(&mut wrapper.docs).chain(wrapper.methods.iter_mut().map(|m| &mut m.docs)) {
            *docs = std::mem::take(docs)
                .into_iter()
                .map(|doc| {
                    // Image names first, as the generic names are substrings of them
                    doc.replace(
                        "aeron_image_controlled_fragment_assembler_t",
                        "`AeronImageControlledFragmentAssembler`",
                    )
                    .replace("aeron_image_fragment_assembler_t", "`AeronImageFragmentAssembler`")
                    .replace("aeron_controlled_fragment_assembler_t", "`ControlledFragmentAssembler`")
                    .replace("aeron_fragment_assembler_t", "`FragmentAssembler`")
                })
                .collect();
        }
    }
}

/// Generate the current Rust wrappers from parsed, target-specific C bindings.
/// This does not run bindgen, rustfmt, or any native build tools.
pub fn generate_wrappers(bindings: &mut CBinding, wrapper_type_filter: fn(&str) -> bool) -> TokenStream {
    // Populate callback signatures before generating methods that use them.
    let bindings_copy = bindings.clone();
    for handler in bindings.handlers.iter_mut() {
        let _ = generate_handlers(handler, &bindings_copy);
    }

    remove_native_assemblers(bindings);

    let mut stream = TokenStream::new();
    for (p, wrapper) in bindings
        .wrappers
        .values()
        .filter(|wrapper| wrapper_type_filter(&wrapper.type_name))
        .enumerate()
    {
        stream.extend(generate_rust_code(
            wrapper,
            &bindings.wrappers,
            p == 0,
            false,
            true,
            &bindings.handlers,
            &bindings.constants,
        ));
    }
    stream.extend(convenience::system_counter_enum(&bindings.constants));
    stream.extend(constant_enums::generate(&bindings.constants));
    stream.extend(native_enums::generate(&bindings.enums));
    let bindings_copy = bindings.clone();
    for handler in bindings.handlers.iter_mut() {
        stream.extend(generate_handlers(handler, &bindings_copy));
    }
    stream
}

/// Minimal `AeronArchiveError` stub for the `archive` trybuild test only. The real type
/// lives in `aeron_custom_archive.rs` and depends on hand-written `rusteron-archive` types,
/// which the isolated trybuild compile cannot see. Generated `aeron_archive_t` methods
/// only call `from_code` and `with_message`, so a bare struct suffices.
#[cfg(test)]
const TRYBUILD_ARCHIVE_ERROR_STUB: &str = r#"
    #[derive(Debug, Clone)]
    pub struct AeronArchiveError { pub code: i32, pub message: std::borrow::Cow<'static, str> }
    impl AeronArchiveError { pub fn from_code(code: i32) -> Self { Self { code, message: "".into() } }
        pub fn with_message(code: i32, message: &'static str) -> Self { Self { code, message: message.into() } } }
    impl std::fmt::Display for AeronArchiveError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "archive error {}", self.code) }
    }
    impl std::error::Error for AeronArchiveError {}
"#;

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

    // Empty output is an error unless the input was empty or whitespace only
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
        ARCHIVE_BINDINGS, CLIENT_BINDINGS, CUSTOM_AERON_CODE, TRYBUILD_ARCHIVE_ERROR_STUB, append_to_file,
        format_token_stream, format_with_rustfmt,
    };
    use proc_macro2::TokenStream;
    use quote::ToTokens;
    use std::fs;

    // Valgrind reports false positives on these pure-Rust parsing and code generation tests
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
            // Populates the FnMut signatures that generate_rust_code needs
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        crate::remove_native_assemblers(&mut bindings);
        for (p, w) in bindings.wrappers.values().enumerate() {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                true,
                true,
                &bindings.handlers,
                &bindings.constants,
            );
            write_to_file(code, false, "client.rs");
        }
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::convenience::system_counter_enum(&bindings.constants).to_string()).unwrap(),
        )
        .unwrap();
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::constant_enums::generate(&bindings.constants).to_string()).unwrap(),
        )
        .unwrap();
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::native_enums::generate(&bindings.enums).to_string()).unwrap(),
        )
        .unwrap();
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
            // Populates the FnMut signatures that generate_rust_code needs
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        crate::remove_native_assemblers(&mut bindings);
        for (p, w) in bindings
            .wrappers
            .values()
            .filter(|w| !w.type_name.contains("pthread") && !w.type_name.contains("_t_"))
            .enumerate()
        {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                true,
                true,
                &bindings.handlers,
                &bindings.constants,
            );
            write_to_file(code, false, "md.rs");
        }
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::convenience::system_counter_enum(&bindings.constants).to_string()).unwrap(),
        )
        .unwrap();
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::constant_enums::generate(&bindings.constants).to_string()).unwrap(),
        )
        .unwrap();
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::native_enums::generate(&bindings.enums).to_string()).unwrap(),
        )
        .unwrap();
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
            // Populates the FnMut signatures that generate_rust_code needs
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        crate::remove_native_assemblers(&mut bindings);
        for (p, w) in bindings.wrappers.values().enumerate() {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                true,
                true,
                &bindings.handlers,
                &bindings.constants,
            );
            write_to_file(code, false, "archive.rs");
        }
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::convenience::system_counter_enum(&bindings.constants).to_string()).unwrap(),
        )
        .unwrap();
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::constant_enums::generate(&bindings.constants).to_string()).unwrap(),
        )
        .unwrap();
        append_to_file(
            &file,
            &format_with_rustfmt(&crate::native_enums::generate(&bindings.enums).to_string()).unwrap(),
        )
        .unwrap();
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
        // The real `AeronArchiveError` comes from CUSTOM_ARCHIVE_CODE, which references
        // hand-written `rusteron-archive` types (e.g. `AeronArchiveAsyncConnect`) and cannot
        // compile standalone. Inject a stub: this test only checks that generated signatures
        // compile, and `rusteron-archive` tests cover the real type
        append_to_file(&file, TRYBUILD_ARCHIVE_ERROR_STUB).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(file)
    }

    /// Generates `aeron.rs` the way build.rs does, from the bindings snapshot packaged with
    /// this crate, so tests that use it still run from the published crate
    fn generate_aeron_rs(bindings_file: &str, skip_filter: fn(&str) -> bool, extra_custom_code: &[&str]) -> String {
        let bindings_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("bindings")
            .join(bindings_file);

        let mut bindings = crate::parse_bindings_with_custom(&bindings_path, extra_custom_code);
        format_with_rustfmt(&crate::generate_wrappers(&mut bindings, skip_filter).to_string())
            .unwrap_or_else(|_| panic!("rustfmt failed on regenerated {bindings_file}"))
    }

    /// Regenerates `aeron.rs` as `build.rs` does (one rustfmt pass over the whole stream,
    /// test modules disabled) and asserts it byte-matches the committed `docs-rs/aeron.rs`
    fn assert_aeron_rs_snapshot_matches(
        bindings_file: &str,
        snapshot_rel_path: &str,
        skip_filter: fn(&str) -> bool,
        extra_custom_code: &[&str],
    ) {
        if running_under_valgrind() {
            return;
        }

        let snapshot_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(snapshot_rel_path);
        let generated = generate_aeron_rs(bindings_file, skip_filter, extra_custom_code);
        // `COPY_BINDINGS=true` regenerates from the host's bindings, which differ per platform,
        // so refresh from the committed bindings instead
        if std::env::var_os("UPDATE_SNAPSHOTS").is_some() {
            fs::write(&snapshot_path, &generated).expect("write the regenerated snapshot");
            return;
        }
        let committed = fs::read_to_string(&snapshot_path)
            .unwrap_or_else(|_| panic!("missing committed snapshot: {}", snapshot_path.display()));

        // `append_to_file` wraps content in `\n{}\n`, so leading and trailing blank lines are not drift
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
                 COMMITTED first 3 lines:\n{}\nRun `UPDATE_SNAPSHOTS=1 cargo test -p \
                 rusteron-code-gen snapshot` and commit the regenerated snapshot.",
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
        assert_aeron_rs_snapshot_matches(
            "client.rs",
            "../rusteron-client/docs-rs/aeron.rs",
            |t| t.starts_with("aeron_"),
            &[],
        );
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn archive_aeron_rs_matches_committed_snapshot() {
        // Archive's build.rs merges CUSTOM_ARCHIVE_CODE into the generator skip-list, so
        // hand-written methods like `ReplayMerge::poll_fn` suppress the generated variant.
        // The regeneration must do the same to match the snapshot.
        //
        // Not macOS-gated: the test parses the committed `bindings/archive.rs`, so the
        // output does not depend on the host platform
        assert_aeron_rs_snapshot_matches(
            "archive.rs",
            "../rusteron-archive/docs-rs/aeron.rs",
            |t| t.starts_with("aeron_"),
            &[crate::CUSTOM_ARCHIVE_CODE],
        );
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn media_driver_aeron_rs_matches_committed_snapshot() {
        // Mirrors the media_driver trybuild test's filter.
        assert_aeron_rs_snapshot_matches(
            "media-driver.rs",
            "../rusteron-media-driver/docs-rs/aeron.rs",
            |t| !t.contains("pthread") && !t.contains("_t_"),
            &[],
        );
    }

    /// `_unwind` trampolines are a plain call, `_catch` ones store the panic, plain ones log it.
    /// None of them abort
    #[test]
    #[cfg(not(target_os = "windows"))]
    fn trampolines_unwind_catch_or_log() {
        if running_under_valgrind() {
            return;
        }
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("bindings")
            .join("client.rs");
        let mut bindings = parse_bindings(&path);
        // first pass populates the FnMut signatures, as in the snapshot test
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        let bindings_copy = bindings.clone();
        let mut stream = TokenStream::new();
        for handler in bindings.handlers.iter_mut() {
            stream.extend(crate::generate_handlers(handler, &bindings_copy));
        }
        let file: syn::File = syn::parse2(stream).expect("generated handlers parse");
        // (abi, body tokens) of the generated fn with this exact name
        let trampoline = |name: &str| {
            file.items
                .iter()
                .find_map(|item| match item {
                    syn::Item::Fn(f) if f.sig.ident == name => {
                        let abi = f.sig.abi.as_ref().and_then(|a| a.name.as_ref()).map(|n| n.value());
                        let block = &f.block;
                        Some((abi, quote::quote!(#block).to_string()))
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("missing {name}"))
        };
        for handler in crate::UNWIND_HANDLER_TYPES {
            for suffix in ["_callback", "_callback_for_once_closure"] {
                let (abi, body) = trampoline(&format!("{handler}{suffix}_unwind"));
                assert_eq!(abi.as_deref(), Some("C-unwind"));
                assert!(!body.contains("_panic") && !body.contains("catch_unwind"), "{body}");
                let (abi, body) = trampoline(&format!("{handler}{suffix}_catch"));
                assert_eq!(abi.as_deref(), Some("C-unwind"));
                assert!(body.contains("catch_panic"), "{body}");
            }
        }
        let (_, body) = trampoline("aeron_controlled_fragment_handler_t_callback_catch");
        assert!(body.contains("AERON_ACTION_ABORT"), "{body}");
        let (abi, body) = trampoline("aeron_error_handler_t_callback");
        assert_eq!(abi.as_deref(), Some("C"));
        assert!(body.contains("log_panic"), "{body}");
        let (_, body) = trampoline("aeron_reserved_value_supplier_t_callback_catch");
        assert!(body.contains("catch_panic (Default :: default ()"), "{body}");
    }

    /// Precompiled builds reuse the committed bindings on every platform, so a macOS
    /// `link_name` (`\u{1}_aeron_...`) would leave Linux with undefined symbols
    #[test]
    fn snapshots_have_no_platform_link_names() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        for file in ["client.rs", "archive.rs", "media-driver.rs"] {
            let path = root.join("bindings").join(file);
            let bindings = std::fs::read_to_string(&path).unwrap();
            assert!(!bindings.contains("#[link_name"), "{} has a link_name", path.display());
        }
    }

    /// Descriptor callbacks outlive reset. Both pollers must use the retained-callback
    /// template, with no stack closure variant or unrestricted native constructor.
    #[test]
    #[cfg(not(target_os = "windows"))]
    fn descriptor_pollers_use_retained_callback_generation() {
        let generated = generate_aeron_rs("archive.rs", |t| t.starts_with("aeron_"), &[crate::CUSTOM_ARCHIVE_CODE]);
        let custom = crate::CUSTOM_ARCHIVE_CODE;
        for poller in [
            "recording_descriptor_poller",
            "recording_subscription_descriptor_poller",
        ] {
            assert!(
                generated.contains(&format!("aeron_archive_{poller}_t")),
                "{poller} not generated"
            );
            for op in ["reset", "poll"] {
                let call = format!("aeron_archive_{poller}_{op}(");
                assert!(generated.contains(&call), "missing generated {call}");
                assert!(!custom.contains(&call), "duplicate custom {call}");
            }
        }
        for class in [
            "AeronArchiveRecordingDescriptorPoller",
            "AeronArchiveRecordingSubscriptionDescriptorPoller",
        ] {
            let syntax = syn::parse_file(&generated).unwrap();
            let methods: Vec<_> = syntax
                .items
                .iter()
                .filter_map(|item| match item {
                    syn::Item::Impl(i) if i.self_ty.to_token_stream().to_string().starts_with(class) => Some(&i.items),
                    _ => None,
                })
                .flatten()
                .filter_map(|item| match item {
                    syn::ImplItem::Fn(method) => Some(method),
                    _ => None,
                })
                .collect();
            assert_eq!(methods.iter().filter(|m| m.sig.ident == "new").count(), 1);
            assert!(!methods.iter().any(|m| m.sig.ident == "reset_fn"));
            let new = methods.iter().find(|m| m.sig.ident == "new").unwrap();
            assert!(
                new.sig
                    .to_token_stream()
                    .to_string()
                    .contains("archive : & AeronArchive")
            );
        }
    }

    /// Each crate's `docs-rs/aeron_custom.rs` is a verbatim copy and must match
    /// `rusteron-code-gen/src/aeron_custom.rs`
    #[test]
    fn aeron_custom_rs_snapshots_match_source() {
        // each crate's committed aeron_custom.rs is the common custom code plus any
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
            if std::env::var_os("UPDATE_SNAPSHOTS").is_some() {
                fs::write(&path, &source).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
                continue;
            }
            let committed = fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("missing committed aeron_custom.rs: {}", path.display()));
            assert_eq!(
                committed.trim(),
                source.trim(),
                "rusteron-{crate_name}/docs-rs/aeron_custom.rs drifted from \
                 rusteron-code-gen/src/aeron_custom*.rs. Run `UPDATE_SNAPSHOTS=1 cargo test -p rusteron-code-gen snapshot`.",
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
    use crate::common::ResourceStorageExt;
    use crate::{Borrowed, CResource, CleanupBox, Inline, ManagedCResource, ResourceStorage};

    use crate::common::ResourceRef;
    #[allow(unused_imports)]
    use ResourceRef as Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn make_resource(val: i32) -> *mut i32 {
        Box::into_raw(Box::new(val))
    }

    unsafe fn reclaim_resource(ptr: *mut i32) {
        if !ptr.is_null() {
            let _ = unsafe { Box::from_raw(ptr) };
        }
    }

    fn callback_owner() -> ManagedCResource<i32> {
        ManagedCResource::<i32>::new(
            |ptr: *mut *mut i32| {
                unsafe { *ptr = make_resource(1) };
                0
            },
            Some(Box::new(|ptr: *mut *mut i32| {
                unsafe { reclaim_resource(*ptr) };
                0
            })),
            false,
        )
        .unwrap_or_else(|err| panic!("callback owner init failed: {}", err.code))
    }

    #[test]
    fn callback_snapshots_keep_only_the_registration_they_copied() {
        let context = callback_owner();
        let first = Arc::new(());
        let probe = Arc::downgrade(&first);
        context.replace_callback_dependency("error", Some(first));
        let connection = callback_owner();
        connection.copy_dependencies_from(&context);
        context.replace_callback_dependency("error", Some(42u32));
        assert!(probe.upgrade().is_some());
        assert_eq!(context.dependency_len(), 1);
        drop(connection);
        assert!(probe.upgrade().is_none());
        assert_eq!(context.get_dependency::<u32>(), Some(42));
        context.replace_callback_dependency::<u32>("error", None);
        assert_eq!(context.dependency_len(), 0);
    }

    #[test]
    fn a_completed_operation_transfers_its_callback_dependencies() {
        let source = CResource::new(Arc::new(callback_owner()));
        let target = CResource::new(Arc::new(callback_owner()));
        let capture = Arc::new(());
        let probe = Arc::downgrade(&capture);
        source
            .as_owned()
            .unwrap()
            .replace_callback_dependency("error", Some(capture));
        target.take_dependencies_from(&source);
        assert_eq!(source.dependency_len(), 0);
        assert_eq!(target.dependency_len(), 1);
        drop(target);
        assert!(probe.upgrade().is_none());
    }

    #[test]
    fn replacing_a_callback_does_not_remove_other_dependencies() {
        let context = callback_owner();
        context.add_dependency(123i32);
        context.replace_callback_dependency("error", Some(42u32));
        context.replace_callback_dependency("signal", Some(7u32));
        for value in 0..100 {
            context.replace_callback_dependency("error", Some(value as u64));
        }
        assert_eq!(context.dependency_len(), 3);
        assert_eq!(context.get_dependency::<i32>(), Some(123));
        assert_eq!(context.get_dependency::<u32>(), Some(7));
        context.replace_callback_dependency::<u64>("error", None);
        assert_eq!(context.dependency_len(), 2);
    }

    #[test]
    fn replaced_captures_drop_after_the_dependency_lock_is_released() {
        struct Capture(std::sync::Weak<ManagedCResource<i32>>);
        impl Drop for Capture {
            fn drop(&mut self) {
                // This would deadlock if replacement held the dependency lock.
                self.0.upgrade().unwrap().add_dependency(123i32);
            }
        }
        let context = Arc::new(callback_owner());
        context.replace_callback_dependency("error", Some(Capture(Arc::downgrade(&context))));
        context.replace_callback_dependency::<Capture>("error", None);
        assert_eq!(context.get_dependency::<i32>(), Some(123));
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
        }) as CleanupBox<i32>);

        {
            let _resource = ManagedCResource::<i32>::new(
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
        }) as CleanupBox<i32>);

        {
            let _resource = ManagedCResource::<i32>::new(
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
        }) as CleanupBox<i32>);

        let resource = ManagedCResource::<i32>::new(
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
        let close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let close_count_for_cleanup = close_count.clone();
        let resource_ptr = make_resource(40);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            close_count_for_cleanup.fetch_add(1, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as CleanupBox<i32>);

        let resource = ManagedCResource::<i32>::new(
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
        let handle_one = CResource::new(inner.clone());
        let handle_two = CResource::new(inner);

        assert!(!handle_one.get().is_null());
        assert!(!handle_two.get().is_null());

        assert!(handle_one.close_resource().is_ok());
        assert_eq!(1, close_count.load(Ordering::SeqCst));
        assert!(handle_one.get().is_null());
        assert!(handle_two.get().is_null());

        assert!(handle_two.close_resource().is_ok());
        assert_eq!(1, close_count.load(Ordering::SeqCst));
    }

    #[test]
    fn close_resource_defers_owner_close_while_dependent_is_alive() {
        let close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let close_count_for_cleanup = close_count.clone();
        let owner_ptr = make_resource(60);
        let child_ptr = make_resource(61);

        let owner_cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            close_count_for_cleanup.fetch_add(1, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as CleanupBox<i32>);

        let owner = ManagedCResource::<i32>::new(
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
        let owner = CResource::new(Rc::new(owner.ok().unwrap()));

        let child = ManagedCResource::<i32>::new(
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
        let child = CResource::new(Rc::new(child.ok().unwrap()));
        child.add_dependency(owner.clone());

        assert!(owner.close_resource_deferred_if_shared().is_ok());
        assert_eq!(0, close_count.load(Ordering::SeqCst));
        assert!(!owner.get().is_null());

        drop(owner);
        drop(child);
        assert_eq!(1, close_count.load(Ordering::SeqCst));
    }

    #[test]
    fn close_resource_deferral_is_retryable_if_owner_close_fails_after_dependent_drops() {
        let close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let close_count_for_cleanup = close_count.clone();
        let owner_ptr = make_resource(70);
        let child_ptr = make_resource(71);

        let owner_cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            let count = close_count_for_cleanup.fetch_add(1, Ordering::SeqCst) + 1;
            if count == 1 {
                return -1;
            }
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as CleanupBox<i32>);

        let owner = ManagedCResource::<i32>::new(
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
        let owner = CResource::new(Rc::new(owner.ok().unwrap()));
        let owner_retry_handle = owner.clone();

        let child = ManagedCResource::<i32>::new(
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
        let child = CResource::new(Rc::new(child.ok().unwrap()));
        child.add_dependency(owner.clone());

        assert!(owner.close_resource_deferred_if_shared().is_ok());
        drop(owner);
        drop(child);
        assert_eq!(0, close_count.load(Ordering::SeqCst));
        assert!(!owner_retry_handle.get().is_null());

        assert!(owner_retry_handle.close_resource_deferred_if_shared().is_err());
        assert_eq!(1, close_count.load(Ordering::SeqCst));
        assert!(!owner_retry_handle.get().is_null());

        assert!(owner_retry_handle.close_resource_deferred_if_shared().is_ok());
        assert_eq!(2, close_count.load(Ordering::SeqCst));
        assert!(owner_retry_handle.get().is_null());
    }

    #[test]
    fn failed_drop_keeps_native_dependencies_and_cleanup_captures_alive() {
        // This error path leaks because C has not released its pointers.
        // Exercise it in the normal suite, outside Valgrind's no-leaks job.
        if std::env::var_os("RUSTERON_VALGRIND").is_some() {
            return;
        }
        let dependency = Rc::new(());
        let capture = Rc::new(());
        let dependency_weak = Rc::downgrade(&dependency);
        let capture_weak = Rc::downgrade(&capture);
        let pointer = make_resource(51);
        let capture_for_cleanup = capture.clone();
        let resource = ManagedCResource::<i32>::new(
            |output| {
                unsafe { *output = pointer };
                0
            },
            Some(Box::new(move |_| {
                std::hint::black_box(&capture_for_cleanup);
                -1
            })),
            false,
        )
        .ok()
        .unwrap();
        resource.add_dependency(dependency.clone());
        drop(dependency);
        drop(capture);
        drop(resource);
        assert!(dependency_weak.upgrade().is_some());
        assert!(capture_weak.upgrade().is_some());
        // The fake native resource has stopped using these values. Production
        // cleanup failures retain them because C may still use them.
        unsafe {
            reclaim_resource(pointer);
        }
    }

    #[test]
    fn close_resource_propagates_failure_and_allows_retry() {
        let close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let close_count_for_cleanup = close_count.clone();
        let resource_ptr = make_resource(50);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            let count = close_count_for_cleanup.fetch_add(1, Ordering::SeqCst) + 1;
            if count == 1 {
                return -1;
            }
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as CleanupBox<i32>);

        let resource = ManagedCResource::<i32>::new(
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
        let handle = CResource::new(Rc::new(resource));

        assert!(handle.close_resource().is_err());
        assert_eq!(1, close_count.load(Ordering::SeqCst));
        assert!(!handle.get().is_null());

        assert!(handle.close_resource().is_ok());
        assert_eq!(2, close_count.load(Ordering::SeqCst));
        assert!(handle.get().is_null());
    }

    #[test]
    fn close_resource_with_uses_custom_cleanup_once_across_clones() {
        let default_close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let custom_close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let default_close_count_for_cleanup = default_close_count.clone();
        let resource_ptr = make_resource(80);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            default_close_count_for_cleanup.fetch_add(1, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as CleanupBox<i32>);

        let resource = ManagedCResource::<i32>::new(
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
        let handle_one = CResource::new(inner.clone());
        let handle_two = CResource::new(inner);

        let custom_close_count_for_cleanup = custom_close_count.clone();
        assert!(
            handle_one
                .close_resource_with(move |res| {
                    custom_close_count_for_cleanup.fetch_add(1, Ordering::SeqCst);
                    unsafe {
                        reclaim_resource(*res);
                        *res = std::ptr::null_mut();
                    }
                    0
                })
                .is_ok()
        );

        assert_eq!(0, default_close_count.load(Ordering::SeqCst));
        assert_eq!(1, custom_close_count.load(Ordering::SeqCst));
        assert!(handle_one.get().is_null());
        assert!(handle_two.get().is_null());

        assert!(handle_two.close_resource().is_ok());
        assert_eq!(0, default_close_count.load(Ordering::SeqCst));
        assert_eq!(1, custom_close_count.load(Ordering::SeqCst));
    }

    #[test]
    fn close_resource_with_failure_restores_default_cleanup_for_retry() {
        let default_close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let custom_close_count = ResourceRef::new(std::sync::atomic::AtomicI32::new(0));
        let default_close_count_for_cleanup = default_close_count.clone();
        let resource_ptr = make_resource(90);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            default_close_count_for_cleanup.fetch_add(1, Ordering::SeqCst);
            unsafe {
                reclaim_resource(*res);
                *res = std::ptr::null_mut();
            }
            0
        }) as CleanupBox<i32>);

        let resource = ManagedCResource::<i32>::new(
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
        let handle = CResource::new(Rc::new(resource.ok().unwrap()));

        let custom_close_count_for_cleanup = custom_close_count.clone();
        assert!(
            handle
                .close_resource_with(move |_res| {
                    custom_close_count_for_cleanup.fetch_add(1, Ordering::SeqCst);
                    -1
                })
                .is_err()
        );

        assert_eq!(0, default_close_count.load(Ordering::SeqCst));
        assert_eq!(1, custom_close_count.load(Ordering::SeqCst));
        assert!(!handle.get().is_null());

        assert!(handle.close_resource().is_ok());
        assert_eq!(1, default_close_count.load(Ordering::SeqCst));
        assert_eq!(1, custom_close_count.load(Ordering::SeqCst));
        assert!(handle.get().is_null());
    }

    #[test]
    fn close_resource_is_noop_for_stack_and_borrowed_resources() {
        let mut borrowed_value = 10;
        let borrowed = unsafe { Borrowed::new(&mut borrowed_value as *mut i32) };
        assert!(borrowed.close_resource().is_ok());
        assert_eq!(10, borrowed_value);

        let stack = Inline::new(20);
        assert!(stack.close_resource().is_ok());
        assert_eq!(20, unsafe { *stack.get() });
    }

    // C fills stack structs (claims, constants, header values) through `get()`
    // on `&self`; run under Miri to catch a write through a read-only borrow.
    #[test]
    fn stack_resource_is_writable_through_shared_get() {
        let stack = Inline::new(20);
        let shared = &stack;
        unsafe { *shared.get() = 30 };
        assert_eq!(30, unsafe { *stack.get() });
        // A separate explicit copy is independent; inline ownership has no Clone.
        let copy = Inline::new(unsafe { *stack.get() });
        unsafe { *stack.get() = 40 };
        assert_eq!(30, unsafe { *copy.get() });
    }
}
