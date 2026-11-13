//! Each download contains compiled C libraries and Rust bindings for the same platform.
//! For example, a Linux ARM64 download contains Linux ARM64 libraries and bindings.
//! Consumers keep the raw C bindings and regenerate wrappers with their current code generator.

use std::fs;
use std::io;
use std::path::Path;

pub const RUST_FILES: [&str; 3] = ["bindings.rs", "aeron.rs", "aeron_custom.rs"];
pub const BINDINGS_DIR: &str = "rust-bindings";
const METADATA_FILE: &str = "bindings-target.txt";

fn metadata(target: &str, version: &str) -> String {
    format!("{target}\n{version}\n")
}

fn require_file(path: &Path) -> io::Result<()> {
    let file = fs::metadata(path)?;
    if !file.is_file() || file.len() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Missing or empty artifact: {}", path.display()),
        ));
    }
    Ok(())
}

pub fn copy_bindings(source: &Path, destination: &Path) -> io::Result<()> {
    for file in RUST_FILES {
        require_file(&source.join(file))?;
    }
    fs::create_dir_all(destination)?;
    for file in RUST_FILES {
        fs::copy(source.join(file), destination.join(file))?;
    }
    Ok(())
}

/// Copy `bindings.rs` from the downloaded files and generate fresh `aeron.rs` and
/// `aeron_custom.rs` files using the current code generator.
///
/// For example, an older download may have no `Ref` type. Generating these files
/// again adds `Ref` when the current Rust code needs it.
pub fn prepare_bindings(
    source: &Path,
    destination: &Path,
    wrapper_type_filter: fn(&str) -> bool,
    extra_custom_code: &[&str],
) -> io::Result<()> {
    let raw_bindings = source.join("bindings.rs");
    require_file(&raw_bindings)?;
    fs::create_dir_all(destination)?;
    let output_bindings = destination.join("bindings.rs");
    fs::copy(raw_bindings, &output_bindings)?;

    // Keep bindings.rs because it describes the C functions and types in the downloaded libraries.
    // Generate the other Rust files again so they match the current Rust code.
    let mut bindings = crate::parse_bindings_with_custom(&output_bindings, extra_custom_code);
    let wrappers = crate::generate_wrappers(&mut bindings, wrapper_type_filter);
    if wrappers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Generated Rust wrappers are empty",
        ));
    }
    // Format in-process so generated diagnostics and docs remain readable without rustfmt.
    let syntax =
        syn::parse2(wrappers).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    fs::write(destination.join("aeron.rs"), prettyplease::unparse(&syntax))?;
    let custom_code = std::iter::once(crate::CUSTOM_AERON_CODE)
        .chain(extra_custom_code.iter().copied())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(destination.join("aeron_custom.rs"), custom_code)
}

/// Copy generated bindings into the bundle and record the target and crate version.
pub fn publish_bindings(out_dir: &Path, bundle: &Path, target: &str, version: &str) -> io::Result<()> {
    copy_bindings(out_dir, &bundle.join(BINDINGS_DIR))?;
    match fs::remove_dir_all(bundle.join("docs-rs")) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    fs::write(bundle.join(METADATA_FILE), metadata(target, version))
}

/// Reject bundles with missing or empty files or a different target or crate version.
pub fn validate_bundle(bundle: &Path, target: &str, version: &str, libraries: &[String]) -> io::Result<()> {
    if fs::read_to_string(bundle.join(METADATA_FILE))? != metadata(target, version) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Bindings in {} do not match target {target} and version {version}",
                bundle.display()
            ),
        ));
    }
    for file in RUST_FILES {
        require_file(&bundle.join(BINDINGS_DIR).join(file))?;
    }
    for library in libraries {
        require_file(&bundle.join(library))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "rusteron-artifacts-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn bundle(&self, target: &str) -> PathBuf {
            let source = self.0.join(format!("generated-{target}"));
            fs::create_dir_all(&source).unwrap();
            for file in RUST_FILES {
                fs::write(source.join(file), format!("// {target}: {file}\n")).unwrap();
            }
            let bundle = self.0.join(target);
            publish_bindings(&source, &bundle, target, "0.2.20").unwrap();
            fs::write(bundle.join("libaeron_static.a"), b"native library").unwrap();
            bundle
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn libraries() -> Vec<String> {
        vec!["libaeron_static.a".into()]
    }

    #[test]
    fn each_target_keeps_its_own_generated_bindings() {
        let temp = TestDir::new();
        for target in [
            "x86_64-unknown-linux-gnu",
            "aarch64-unknown-linux-gnu",
            "aarch64-apple-darwin",
        ] {
            let bundle = temp.bundle(target);
            validate_bundle(&bundle, target, "0.2.20", &libraries()).unwrap();
            let output = temp.0.join(format!("consumer-{target}"));
            assert!(!bundle.join("docs-rs").exists());
            copy_bindings(&bundle.join(BINDINGS_DIR), &output).unwrap();
            assert_eq!(
                fs::read_to_string(output.join("bindings.rs")).unwrap(),
                format!("// {target}: bindings.rs\n")
            );
        }
    }

    #[test]
    fn prepares_current_wrappers_from_the_bundles_raw_bindings() {
        let temp = TestDir::new();
        let bundle = temp.bundle("x86_64-unknown-linux-gnu");
        let source = bundle.join(BINDINGS_DIR);
        fs::write(source.join("bindings.rs"), crate::ARCHIVE_BINDINGS).unwrap();
        fs::write(source.join("aeron.rs"), "compile_error!(\"stale wrappers\");").unwrap();
        fs::write(source.join("aeron_custom.rs"), "compile_error!(\"stale custom code\");").unwrap();

        let output = temp.0.join("consumer");
        prepare_bindings(
            &source,
            &output,
            |name| name.starts_with("aeron_"),
            &[crate::CUSTOM_ARCHIVE_CODE],
        )
        .unwrap();

        // Keep the exact ABI supplied with the native libraries.
        assert_eq!(
            fs::read(output.join("bindings.rs")).unwrap(),
            crate::ARCHIVE_BINDINGS.as_bytes()
        );
        let wrappers = fs::read_to_string(output.join("aeron.rs")).unwrap();
        let parsed = syn::parse_file(&wrappers).unwrap();
        assert!(
            parsed
                .items
                .iter()
                .any(|item| matches!(item, syn::Item::Struct(item) if item.ident == "Ref"))
        );
        assert!(!wrappers.contains("stale wrappers"));

        let custom = fs::read_to_string(output.join("aeron_custom.rs")).unwrap();
        assert!(custom.contains(crate::CUSTOM_AERON_CODE));
        assert!(custom.contains(crate::CUSTOM_ARCHIVE_CODE));
        assert!(!custom.contains("stale custom code"));
        // Reading a release bundle must not modify it.
        assert!(
            fs::read_to_string(source.join("aeron.rs"))
                .unwrap()
                .contains("stale wrappers")
        );
    }

    #[test]
    fn rejects_bindings_for_another_target_or_version() {
        let temp = TestDir::new();
        let bundle = temp.bundle("aarch64-unknown-linux-gnu");
        for (target, version) in [
            ("aarch64-unknown-linux-musl", "0.2.20"),
            ("x86_64-unknown-linux-gnu", "0.2.20"),
            ("aarch64-unknown-linux-gnu", "0.2.19"),
        ] {
            assert!(validate_bundle(&bundle, target, version, &libraries()).is_err());
        }
    }

    #[test]
    fn rejects_native_only_legacy_artifacts() {
        let temp = TestDir::new();
        fs::write(temp.0.join("libaeron_static.a"), b"native library").unwrap();
        assert!(validate_bundle(&temp.0, "x86_64-unknown-linux-gnu", "0.2.20", &libraries()).is_err());
    }

    #[test]
    fn republishing_removes_the_old_docs_directory() {
        let temp = TestDir::new();
        let target = "aarch64-unknown-linux-gnu";
        let bundle = temp.bundle(target);
        fs::rename(bundle.join(BINDINGS_DIR), bundle.join("docs-rs")).unwrap();
        assert!(validate_bundle(&bundle, target, "0.2.20", &libraries()).is_err());
        let bundle = temp.bundle(target);
        assert!(!bundle.join("docs-rs").exists());
        validate_bundle(&bundle, target, "0.2.20", &libraries()).unwrap();
    }

    #[test]
    fn rejects_incomplete_or_empty_artifacts() {
        let temp = TestDir::new();
        let target = "aarch64-unknown-linux-gnu";
        let bundle = temp.bundle(target);
        fs::remove_file(bundle.join(BINDINGS_DIR).join("aeron.rs")).unwrap();
        assert!(validate_bundle(&bundle, target, "0.2.20", &libraries()).is_err());
        let bundle = temp.bundle(target);
        fs::write(bundle.join("libaeron_static.a"), b"").unwrap();
        assert!(validate_bundle(&bundle, target, "0.2.20", &libraries()).is_err());
    }
}
