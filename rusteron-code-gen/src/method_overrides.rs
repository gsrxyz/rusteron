//! Metadata for native methods replaced by handwritten Rust APIs.

use crate::CBinding;

/// Suppress a generated wrapper method in favor of the named Rust methods.
/// Raw FFI declarations remain available in `bindings`.
#[derive(Debug, Clone, Copy)]
pub struct MethodOverride {
    pub native_method: &'static str,
    /// Handwritten replacements, including methods on a different owner type.
    pub rust_methods: &'static [&'static str],
}

/// Driver lifecycle methods require ownership states that C signatures omit.
/// Shared by driver builds and the packaged bindings snapshot tests.
pub const MEDIA_DRIVER_METHOD_OVERRIDES: &[MethodOverride] = &[
    MethodOverride {
        native_method: "aeron_driver_start",
        rust_methods: &["AeronDriver::start", "AeronDriver::start_manual"],
    },
    MethodOverride {
        native_method: "aeron_driver_main_do_work",
        rust_methods: &["ManualDriver::do_work"],
    },
    MethodOverride {
        native_method: "aeron_driver_main_idle_strategy",
        rust_methods: &["ManualDriver::idle"],
    },
];

impl CBinding {
    /// Add explicit overrides to methods detected in handwritten source.
    /// Match native names so typedef aliases receive the same override.
    ///
    /// # Panics
    ///
    /// Panics if an override names no parsed wrapper method. This catches stale
    /// metadata after a native API change instead of emitting the old wrapper.
    pub fn apply_method_overrides(&mut self, overrides: &[MethodOverride]) {
        for entry in overrides {
            let mut found = false;
            for wrapper in self.wrappers.values_mut() {
                for method in &wrapper.methods {
                    if method.fn_name == entry.native_method {
                        wrapper.skipped_methods.insert(method.struct_method_name.clone());
                        found = true;
                    }
                }
            }
            assert!(
                found,
                "method override for {} has no parsed wrapper method; Rust replacements: {:?}",
                entry.native_method, entry.rust_methods,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Arg, ArgProcessing, CWrapper, Method};
    use std::collections::{BTreeMap, BTreeSet};

    fn wrapper(native_method: &str) -> CWrapper {
        CWrapper {
            skipped_methods: BTreeSet::from(["handwritten".into()]),
            methods: vec![Method {
                fn_name: native_method.into(),
                struct_method_name: "poll".into(),
                return_type: Arg {
                    name: String::new(),
                    c_type: "i32".into(),
                    processing: ArgProcessing::Default,
                },
                arguments: vec![],
                docs: BTreeSet::new(),
            }],
            ..CWrapper::default()
        }
    }

    #[test]
    fn overrides_match_native_names_across_aliases_and_preserve_existing_methods() {
        let mut bindings = CBinding {
            wrappers: BTreeMap::from([
                ("native_queue_t".into(), wrapper("native_queue_poll")),
                ("native_queue_alias_t".into(), wrapper("native_queue_poll")),
                ("native_other_t".into(), wrapper("native_other_poll")),
            ]),
            ..CBinding::default()
        };
        bindings.apply_method_overrides(&[MethodOverride {
            native_method: "native_queue_poll",
            rust_methods: &["QueueReader::poll"],
        }]);
        for name in ["native_queue_t", "native_queue_alias_t"] {
            assert_eq!(
                bindings.wrappers[name].skipped_methods,
                BTreeSet::from(["handwritten".into(), "poll".into()])
            );
        }
        assert_eq!(
            bindings.wrappers["native_other_t"].skipped_methods,
            BTreeSet::from(["handwritten".into()])
        );
    }

    #[test]
    #[should_panic(expected = "method override for native_missing_poll has no parsed wrapper method")]
    fn unknown_methods_fail_instead_of_silently_disabling_the_override() {
        CBinding::default().apply_method_overrides(&[MethodOverride {
            native_method: "native_missing_poll",
            rust_methods: &["QueueReader::poll"],
        }]);
    }

    #[test]
    fn driver_methods_are_generated_unless_the_build_selects_their_overrides() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/media-driver.rs");
        let mut bindings = crate::parse_bindings(&path);
        let method_names = |bindings: &CBinding| {
            let file = syn::parse2::<syn::File>(crate::generate_rust_code(
                &bindings.wrappers["aeron_driver_t"],
                &bindings.wrappers,
                false,
                false,
                true,
                &bindings.handlers,
                &bindings.constants,
            ))
            .unwrap();
            file.items
                .into_iter()
                .filter_map(|item| match item {
                    syn::Item::Impl(item) if item.trait_.is_none() => Some(item.items),
                    _ => None,
                })
                .flatten()
                .filter_map(|item| match item {
                    syn::ImplItem::Fn(method) => Some(method.sig.ident.to_string()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
        };
        let original = method_names(&bindings);
        for name in ["start", "main_do_work", "main_idle_strategy"] {
            assert!(original.contains(name), "{name}");
        }
        bindings.apply_method_overrides(MEDIA_DRIVER_METHOD_OVERRIDES);
        let overridden = method_names(&bindings);
        for name in ["start", "main_do_work", "main_idle_strategy"] {
            assert!(!overridden.contains(name), "{name}");
        }
        assert_eq!(
            original.difference(&overridden).cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["start".into(), "main_do_work".into(), "main_idle_strategy".into()])
        );
    }
}
