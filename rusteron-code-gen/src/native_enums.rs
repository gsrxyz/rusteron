//! Rust names and checked integer conversions for native enums.

use crate::NativeEnum;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

fn rust_names(item: &NativeEnum) -> Option<(syn::Ident, Vec<syn::Ident>)> {
    let name = ["_stct", "_enum", "_en"]
        .iter()
        .find_map(|suffix| item.name.strip_suffix(suffix))
        .unwrap_or(&item.name);
    let alias = syn::parse_str(&crate::snake_to_pascal_case(name)).ok()?;
    let words: Vec<Vec<_>> = item.variants.iter().map(|name| name.split('_').collect()).collect();
    let first = words.first()?;
    // Keep at least one word, including when the enum has a single variant.
    let prefix = (0..first.len().saturating_sub(1))
        .take_while(|&index| words.iter().all(|words| words.get(index) == first.get(index)))
        .count();
    let names: Vec<syn::Ident> = words
        .iter()
        .map(|words| {
            let name = words[prefix..].join("_").to_ascii_lowercase();
            syn::parse_str(&crate::snake_to_pascal_case(&name))
        })
        .collect::<Result<_, _>>()
        .ok()?;
    let unique: std::collections::BTreeSet<_> = names.iter().map(ToString::to_string).collect();
    (unique.len() == names.len()).then_some((alias, names))
}

pub(crate) fn parse(item: &syn::ItemEnum) -> Option<NativeEnum> {
    if !item.ident.to_string().starts_with("aeron_")
        || !matches!(item.vis, syn::Visibility::Public(_))
        || !item.generics.params.is_empty()
        || item.variants.is_empty()
        || item
            .variants
            .iter()
            .any(|variant| !matches!(variant.fields, syn::Fields::Unit))
    {
        return None;
    }
    let repr = item.attrs.iter().find_map(|attr| {
        if !attr.path().is_ident("repr") {
            return None;
        }
        let repr = attr.parse_args::<syn::Ident>().ok()?.to_string();
        matches!(
            repr.as_str(),
            "i8" | "u8" | "i16" | "u16" | "i32" | "u32" | "i64" | "u64" | "i128" | "u128" | "isize" | "usize"
        )
        .then_some(repr)
    })?;
    Some(NativeEnum {
        name: item.ident.to_string(),
        repr,
        variants: item.variants.iter().map(|variant| variant.ident.to_string()).collect(),
    })
}

pub(crate) fn generate(enums: &[NativeEnum]) -> TokenStream {
    enums
        .iter()
        .map(|item| {
            let name = format_ident!("{}", item.name);
            let repr = format_ident!("{}", item.repr);
            let variants: Vec<_> = item.variants.iter().map(|name| format_ident!("{name}")).collect();
            let patterns: Vec<_> = (0..variants.len()).map(|i| format_ident!("VALUE_{i}")).collect();
            let rust_names = rust_names(item).map(|(alias, names)| {
                let doc = format!("Rust name for [`{name}`]. This is the native enum, with the same representation.");
                let constants = names.iter().zip(&variants).map(|(short, native)| {
                    let native_name = native.to_string();
                    let doc = format!("Native [`{name}::{native}`] value.");
                    quote! {
                        #[doc = #doc]
                        #[doc(alias = #native_name)]
                        pub const #short: Self = Self::#native;
                    }
                });
                quote! {
                    #[doc = #doc]
                    pub use bindings::#name as #alias;

                    #[allow(non_upper_case_globals)]
                    impl #name { #(#constants)* }
                }
            });
            quote! {
                #rust_names
                impl std::convert::TryFrom<#repr> for #name {
                    type Error = #repr;

                    /// Convert a known native value, or return the unrecognised integer.
                    #[inline]
                    fn try_from(value: #repr) -> Result<Self, Self::Error> {
                        #(const #patterns: #repr = #name::#variants as #repr;)*
                        match value {
                            #(#patterns => Ok(Self::#variants),)*
                            _ => Err(value),
                        }
                    }
                }

                impl From<#name> for #repr {
                    #[inline]
                    fn from(value: #name) -> Self { value as Self }
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_bindings;

    #[test]
    fn rust_names_strip_c_suffixes_and_the_shared_variant_prefix() {
        for (name, variants, expected_alias, expected_names) in [
            (
                "aeron_controlled_fragment_handler_action_en",
                vec![
                    "AERON_ACTION_ABORT",
                    "AERON_ACTION_BREAK",
                    "AERON_ACTION_COMMIT",
                    "AERON_ACTION_CONTINUE",
                ],
                "AeronControlledFragmentHandlerAction",
                vec!["Abort", "Break", "Commit", "Continue"],
            ),
            (
                "aeron_threading_mode_enum",
                vec!["AERON_THREADING_MODE_DEDICATED", "AERON_THREADING_MODE_SHARED"],
                "AeronThreadingMode",
                vec!["Dedicated", "Shared"],
            ),
            (
                "aeron_system_counter_enum_stct",
                vec!["AERON_COUNTER_SENT", "AERON_COUNTER_RECEIVED"],
                "AeronSystemCounterEnum",
                vec!["Sent", "Received"],
            ),
            (
                "aeron_single_en",
                vec!["AERON_SINGLE_VALUE"],
                "AeronSingle",
                vec!["Value"],
            ),
        ] {
            let item = NativeEnum {
                name: name.into(),
                repr: "u32".into(),
                variants: variants.iter().map(|name| (*name).into()).collect(),
            };
            let (alias, names) = rust_names(&item).unwrap();
            assert_eq!(alias, expected_alias);
            assert_eq!(
                names.iter().map(ToString::to_string).collect::<Vec<_>>(),
                expected_names
            );
            let generated = generate(&[item]);
            let syntax = syn::parse2::<syn::File>(generated.clone()).unwrap();
            let syn::Item::Use(reexport) = &syntax.items[0] else {
                panic!("expected a native enum re-export")
            };
            let syn::UseTree::Path(path) = &reexport.tree else {
                panic!("expected a bindings path")
            };
            assert_eq!(path.ident, "bindings");
            let syn::UseTree::Rename(rename) = &*path.tree else {
                panic!("expected a renamed re-export")
            };
            assert_eq!(rename.ident, name);
            assert_eq!(rename.rename, expected_alias);
            let code = generated.to_string();
            for (short, native) in expected_names.iter().zip(variants) {
                assert!(
                    code.contains(&format!("pub const {short} : Self = Self :: {native}")),
                    "{code}"
                );
            }
            assert!(
                !syntax
                    .items
                    .iter()
                    .any(|item| matches!(item, syn::Item::Enum(_) | syn::Item::Struct(_)))
            );
        }
    }

    #[test]
    fn invalid_rust_names_leave_the_existing_integer_conversions_available() {
        for variants in [
            vec![],
            vec!["AERON_SELF"],
            vec!["AERON_ACTION_1"],
            vec!["AERON_A", "AERON_a"],
        ] {
            let item = NativeEnum {
                name: "aeron_example_en".into(),
                repr: "u32".into(),
                variants: variants.into_iter().map(Into::into).collect(),
            };
            assert!(rust_names(&item).is_none());
            let code = generate(&[item]).to_string();
            assert!(code.contains("TryFrom"));
            assert!(!code.contains("pub use"));
        }
    }

    #[test]
    fn only_public_aeron_unit_enums_with_an_integer_repr_get_conversions() {
        for code in [
            "pub enum aeron_example { Value }",
            "#[repr(C)] pub enum aeron_example { Value }",
            "#[repr(u8)] enum aeron_example { Value }",
            "#[repr(u8)] pub enum other_example { Value }",
            "#[repr(u8)] pub enum aeron_example { Value(u8) }",
            "#[repr(u8)] pub enum aeron_example<T> { Value(T) }",
            "#[repr(u8)] pub enum aeron_example {}",
        ] {
            let item = syn::parse_str(code).unwrap();
            assert!(parse(&item).is_none(), "{code}");
        }
        assert!(generate(&[]).is_empty());
    }

    #[test]
    fn conversions_use_the_declared_repr_and_variant_values() {
        for repr in [
            "i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "i128", "u128", "isize", "usize",
        ] {
            let item = syn::parse_str(&format!(
                "#[repr({repr})] pub enum aeron_example {{ First = 3, Next = 9, Future = 123 }}"
            ))
            .unwrap();
            let metadata = parse(&item).unwrap();
            assert_eq!(metadata.repr, repr);
            assert_eq!(metadata.variants, ["First", "Next", "Future"]);
            let generated = generate(&[metadata]);
            let code = generated.to_string();
            syn::parse2::<syn::File>(generated).unwrap();
            assert!(code.contains(&format!("TryFrom < {repr} >")));
            assert!(code.contains(&format!("From < aeron_example > for {repr}")));
            for variant in ["First", "Next", "Future"] {
                assert!(code.contains(&format!("aeron_example :: {variant} as {repr}")));
                assert!(code.contains(&format!("Ok (Self :: {variant})")));
            }
            assert!(code.contains("Err (value)"));
            assert!(!code.contains("unsafe"));
            assert!(!code.contains("transmute"));
        }
    }

    #[test]
    fn every_native_enum_in_the_committed_bindings_is_discovered() {
        for (file, expected) in [("client.rs", 4), ("archive.rs", 6), ("media-driver.rs", 27)] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("bindings")
                .join(file);
            let bindings = parse_bindings(&path);
            let source = std::fs::read_to_string(path).unwrap();
            let native: Vec<_> = syn::parse_file(&source)
                .unwrap()
                .items
                .into_iter()
                .filter_map(|item| match item {
                    syn::Item::Enum(item) if item.ident.to_string().starts_with("aeron_") => Some(item),
                    _ => None,
                })
                .collect();
            assert_eq!(native.len(), expected, "{file}");
            assert_eq!(bindings.enums.len(), native.len(), "{file}");
            for (metadata, item) in bindings.enums.iter().zip(native) {
                assert_eq!(metadata.name, item.ident.to_string());
                assert_eq!(metadata.variants.len(), item.variants.len());
                let (alias, _) = rust_names(metadata).unwrap();
                assert!(
                    bindings.wrappers.values().all(|wrapper| alias != wrapper.class_name),
                    "{alias}"
                );
            }
        }
    }
}
