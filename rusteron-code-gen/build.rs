// Copies build_common.rs into each sibling crate so they can
// `include!("build_common.rs")` in their build scripts.  The include! must
// be inlined (not a library call) because cfg!(feature) / env!(CARGO_MANIFEST_DIR)
// resolve against the consuming crate, not rusteron-code-gen.
//
// Publishing crates that include! a workspace-relative path is a known pain
// point: "../rusteron-code-gen/src/build_common.rs" works in the monorepo but
// the directory is "rusteron-code-gen-{version}" on crates.io.  By placing a
// copy in each crate's own root we sidestep the versioned-dir problem
// entirely — "build_common.rs" is always right next to build.rs.

use std::{env, fs, path::PathBuf};

fn main() {
    let src = PathBuf::from("src/build_common.rs");
    let dst_name = "build_common.rs";

    println!("cargo:rerun-if-changed=src/build_common.rs");

    // Write to our own OUT_DIR (belt-and-suspenders).
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::copy(&src, out_dir.join(dst_name)).ok();

    // Push a copy into each sibling crate that needs it.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ws_root = manifest_dir.parent().expect("workspace root");

    for crate_name in &[
        "rusteron-client",
        "rusteron-archive",
        "rusteron-media-driver",
    ] {
        let dst = ws_root.join(crate_name).join(dst_name);
        // Silently skip if the dir doesn't exist (e.g. crate not checked out).
        if dst.parent().map(|p| p.exists()).unwrap_or(false) {
            fs::copy(&src, &dst).ok();
        }
    }
}
