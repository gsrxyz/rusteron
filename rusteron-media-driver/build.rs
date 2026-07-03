// Thin build script: all shared logic lives in rusteron-code-gen/src/build_common.rs,
// include!d here so cfg!(feature) / env!(CARGO_MANIFEST_DIR) resolve against this crate.
include!("../rusteron-code-gen/src/build_common.rs");

fn driver_wrapper_filter(type_name: &str) -> bool {
    !type_name.contains("_t_") && type_name != "in_addr"
}

pub fn main() {
    rusteron_build_main(&RusteronBuildConfig {
        header_subdir: "aeron-driver/src/main/c",
        target_dynamic: "aeron_driver",
        target_static: "aeron_driver_static",
        base_target_dynamic: Some("aeron"),
        base_target_static: None,
        extra_clang_include: Some("aeron-client/src/main/c"),
        extra_allowlist_vars: &[],
        cmake_defines: &[("BUILD_AERON_DRIVER", "ON")],
        allow_multiple_definition: false,
        precompile_linux_extra_lib: Some("bsd"),
        wrapper_type_filter: driver_wrapper_filter,
        expected_wrapper: Some("aeron_driver_conductor_t"),
        extra_custom_code: &[],
        bindings_snapshot: "media-driver.rs",
        pre_build: RusteronBuildConfig::no_pre_build,
    });
}
