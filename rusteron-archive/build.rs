// Thin build script: all shared logic lives in rusteron-code-gen/src/build_common.rs,
// include!d here so cfg!(feature) / env!(CARGO_MANIFEST_DIR) resolve against this crate.
include!("build_common.rs");

/// The archive C client needs the aeron jars (built via gradle) for its cmake build.
fn run_gradle_build_if_missing(aeron_path: &Path) {
    use std::process::{Command, Stdio};
    if !aeron_path.join("aeron-all").join("build").join("libs").exists() {
        let path = std::path::MAIN_SEPARATOR;
        let gradle = if cfg!(target_os = "windows") {
            &format!("{}{path}aeron{path}gradlew.bat", env!("CARGO_MANIFEST_DIR"),)
        } else {
            "./gradlew"
        };
        let dir = format!("{}{path}aeron", env!("CARGO_MANIFEST_DIR"),);
        eprintln!("running {gradle} in {dir}");

        Command::new(gradle)
            .current_dir(dir)
            .args([
                ":aeron-agent:jar",
                ":aeron-samples:jar",
                ":aeron-archive:jar",
                ":aeron-all:jar",
                ":buildSrc:jar",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn().expect("failed to run gradle, which is required to build aeron-archive c lib. Please refer to wiki page regarding build setup")
            .wait().expect("gradle returned an error");
    }
    println!("cargo:rerun-if-changed=aeron/aeron-all/build/libs");
}

pub fn main() {
    rusteron_build_main(&RusteronBuildConfig {
        header_subdir: "aeron-archive/src/main/c",
        target_dynamic: "aeron_archive_c_client",
        target_static: "aeron_archive_c_client_static",
        base_target_dynamic: Some("aeron"),
        base_target_static: Some("aeron_static"),
        extra_clang_include: Some("aeron-client/src/main/c"),
        extra_allowlist_vars: &["ARCHIVE_ERROR_CODE_.*"],
        cmake_defines: &[
            ("BUILD_AERON_ARCHIVE_API", "ON"),
            // needed for mac os
            ("CMAKE_OSX_DEPLOYMENT_TARGET", "14.0"),
            ("BUILD_SHARED_LIBS", "ON"),
        ],
        allow_multiple_definition: true,
        precompile_linux_extra_lib: None,
        wrapper_type_filter: RusteronBuildConfig::aeron_only,
        expected_wrapper: None,
        extra_custom_code: &[rusteron_code_gen::CUSTOM_ARCHIVE_CODE],
        bindings_snapshot: "archive.rs",
        pre_build: run_gradle_build_if_missing,
    });
}
