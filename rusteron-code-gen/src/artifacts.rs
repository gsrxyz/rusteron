//! Each download contains compiled C libraries and Rust bindings for the same platform.
//! For example, a Linux ARM64 download contains Linux ARM64 libraries and bindings.

use std::fs;
use std::io;
use std::path::Path;

pub const RUST_FILES: [&str; 3] = ["bindings.rs", "aeron.rs", "aeron_custom.rs"];
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

/// Copy generated bindings into the bundle and record the target and crate version.
pub fn publish_bindings(out_dir: &Path, bundle: &Path, target: &str, version: &str) -> io::Result<()> {
    copy_bindings(out_dir, &bundle.join("docs-rs"))?;
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
        require_file(&bundle.join("docs-rs").join(file))?;
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
            copy_bindings(&bundle.join("docs-rs"), &output).unwrap();
            assert_eq!(
                fs::read_to_string(output.join("bindings.rs")).unwrap(),
                format!("// {target}: bindings.rs\n")
            );
        }
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
    fn rejects_incomplete_or_empty_artifacts() {
        let temp = TestDir::new();
        let target = "aarch64-unknown-linux-gnu";
        let bundle = temp.bundle(target);
        fs::remove_file(bundle.join("docs-rs/aeron.rs")).unwrap();
        assert!(validate_bundle(&bundle, target, "0.2.20", &libraries()).is_err());
        let bundle = temp.bundle(target);
        fs::write(bundle.join("libaeron_static.a"), b"").unwrap();
        assert!(validate_bundle(&bundle, target, "0.2.20", &libraries()).is_err());
    }
}
