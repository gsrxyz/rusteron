// Compiles a C file against the Aeron headers rusteron-media-driver exports through
// its `links` key, as an out-of-tree UDP transport (e.g. kernel bypass) does.
fn main() {
    let dep = |key: &str| {
        std::env::var(format!("DEP_AERON_DRIVER_{key}"))
            .unwrap_or_else(|_| panic!("rusteron-media-driver must export DEP_AERON_DRIVER_{key}"))
    };
    println!("cargo:rerun-if-changed=transport.c");
    cc::Build::new()
        .file("transport.c")
        .include(dep("INCLUDE"))
        .include(dep("CLIENT_INCLUDE"))
        // As in Aeron's own cmake build: its headers have unused hook parameters.
        .flag_if_supported("-Wno-unused-parameter")
        .compile("links_test_transport");
}
