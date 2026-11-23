//! Guards `rusteron-media-driver`'s `links` export. A custom UDP transport (kernel bypass,
//! for example) is C compiled against the Aeron header paths that export hands to its build
//! script. This crate's build script does the same with `transport.c`, so a broken export
//! fails here rather than in a transport crate.

#[cfg(test)]
mod tests {
    unsafe extern "C" {
        fn links_test_transport_bindings_size() -> usize;
    }

    #[test]
    fn transport_header_compiles_against_exported_paths() {
        // The C header and the Rust bindings must agree on the table layout
        assert_eq!(
            // SAFETY: a pure function that build.rs compiled from transport.c.
            unsafe { links_test_transport_bindings_size() },
            size_of::<rusteron_media_driver::bindings::aeron_udp_channel_transport_bindings_t>()
        );
    }
}
