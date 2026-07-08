// Thin build script: all shared logic lives in rusteron-code-gen/src/build_common.rs,
// include!d here so cfg!(feature) / env!(CARGO_MANIFEST_DIR) resolve against this crate.
include!("build_common.rs");

pub fn main() {
    rusteron_build_main(&RusteronBuildConfig {
        header_subdir: "aeron-client/src/main/c",
        target_dynamic: "aeron",
        target_static: "aeron_static",
        base_target_dynamic: None,
        base_target_static: None,
        extra_clang_include: None,
        extra_allowlist_vars: &[],
        cmake_defines: &[],
        allow_multiple_definition: false,
        precompile_linux_extra_lib: None,
        wrapper_type_filter: RusteronBuildConfig::aeron_only,
        expected_wrapper: None,
        extra_custom_code: &[],
        bindings_snapshot: "client.rs",
        pre_build: RusteronBuildConfig::no_pre_build,
    });
}
