//! Enums for native constant families with distinct integer values.

use crate::Arg;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

fn variants(constants: &[Arg], prefixes: &[&str], suffix: &str) -> Vec<(Ident, Ident)> {
    constants
        .iter()
        .filter(|constant| matches!(constant.c_type.as_str(), "i32" | "u32"))
        .filter_map(|constant| {
            let name = prefixes.iter().find_map(|prefix| constant.name.strip_prefix(prefix))?;
            let name = name.strip_suffix(suffix)?;
            if name.is_empty() {
                return None;
            }
            let name: String = name
                .to_ascii_lowercase()
                .split('_')
                .map(|word| {
                    let mut letters = word.chars();
                    letters
                        .next()
                        .map(|first| first.to_ascii_uppercase().to_string() + letters.as_str())
                        .unwrap_or_default()
                })
                .collect();
            Some((format_ident!("{name}"), constant.as_ident()))
        })
        .collect()
}

fn declarations(variants: &[(Ident, Ident)]) -> TokenStream {
    let declarations = variants.iter().map(|(variant, constant)| {
        let doc = format!("Native constant `{constant}`.");
        quote! { #[doc = #doc] #variant }
    });
    quote! { #(#declarations,)* }
}

fn integer_enum(constants: &[Arg], name: &str, prefix: &str, suffix: &str, doc: &str) -> TokenStream {
    let variants = variants(constants, &[prefix], suffix);
    if variants.is_empty() {
        return quote! {};
    }
    let name = format_ident!("{name}");
    let declarations = declarations(&variants);
    // Native macros mix i32 and u32. Local constants give match patterns one type.
    let patterns: Vec<_> = (0..variants.len()).map(|i| format_ident!("CODE_{i}")).collect();
    let natives: Vec<_> = variants.iter().map(|(_, native)| native).collect();
    let names: Vec<_> = variants.iter().map(|(name, _)| name).collect();
    quote! {
        #[doc = #doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum #name {
            #declarations
            /// An unrecognised value, preserved unchanged.
            Unknown(i32),
        }

        impl #name {
            /// Convert a raw value. This preserves its sign and namespace.
            #[inline]
            pub const fn from_code(code: i32) -> Self {
                #(const #patterns: i32 = #natives as i32;)*
                match code { #(#patterns => Self::#names,)* _ => Self::Unknown(code) }
            }

            /// Return the original native value.
            #[inline]
            pub const fn code(self) -> i32 {
                match self { #(Self::#names => #natives as i32,)* Self::Unknown(code) => code }
            }
        }

        impl From<i32> for #name {
            #[inline]
            fn from(code: i32) -> Self { Self::from_code(code) }
        }

        impl From<#name> for i32 {
            #[inline]
            fn from(value: #name) -> Self { value.code() }
        }
    }
}

fn archive_error_enum(constants: &[Arg]) -> TokenStream {
    let variants = variants(
        constants,
        &["AERON_ARCHIVE_ERROR_CODE_", "AERON_ARCHIVE_CLIENT_ERROR_"],
        "",
    );
    if variants.is_empty() {
        return quote! {};
    }
    let declarations = declarations(&variants);
    let names: Vec<_> = variants.iter().map(|(name, _)| name).collect();
    let natives: Vec<_> = variants.iter().map(|(_, native)| native).collect();
    quote! {
        /// Archive server and client errors, read from the native bindings.
        /// Known codes accept either sign. Unknown codes keep their value and sign.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum AeronArchiveErrorCode {
            #declarations
            /// An unrecognised code, preserved unchanged.
            Unknown(i32),
        }

        impl AeronArchiveErrorCode {
            /// Convert a code from `aeron_errcode()` or an Archive error handler.
            /// System errors and unknown codes retain their value and sign.
            pub const fn from_native_code(code: i32) -> Self {
                match code.unsigned_abs() { #(#natives => Self::#names,)* _ => Self::Unknown(code) }
            }

            /// Return the negative native code for a known Archive error.
            /// Unknown codes retain their value and sign.
            pub const fn as_native_code(self) -> i32 {
                let code = match self { #(Self::#names => #natives,)* Self::Unknown(code) => return code };
                -(code as i32)
            }
        }
    }
}

pub(crate) fn generate(constants: &[Arg]) -> TokenStream {
    let counter_types = integer_enum(
        constants,
        "AeronCounterTypeId",
        "AERON_COUNTER_",
        "_TYPE_ID",
        "Built-in counter type IDs. Application-defined IDs remain `Unknown(id)`.",
    );
    let counter_states = integer_enum(
        constants,
        "AeronCounterState",
        "AERON_COUNTER_RECORD_",
        "",
        "Counter metadata record states. These are not counter IDs or type IDs.",
    );
    let driver_errors = integer_enum(
        constants,
        "AeronDriverErrorCode",
        "AERON_ERROR_CODE_",
        "",
        "Driver protocol error codes. Use the wire value, not an offer result or a system error.",
    );
    let archive_errors = archive_error_enum(constants);
    quote! { #counter_types #counter_states #driver_errors #archive_errors }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArgProcessing, parse_bindings};

    fn constant(name: &str, c_type: &str) -> Arg {
        Arg {
            name: name.into(),
            c_type: c_type.into(),
            processing: ArgProcessing::Default,
        }
    }

    fn enums(tokens: TokenStream) -> Vec<syn::ItemEnum> {
        syn::parse2::<syn::File>(tokens)
            .unwrap()
            .items
            .into_iter()
            .filter_map(|item| match item {
                syn::Item::Enum(item) => Some(item),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn families_only_accept_integer_constants_with_the_required_prefix_and_suffix() {
        let constants = [
            constant("AERON_COUNTER_NEW_MEASUREMENT_TYPE_ID", "u32"),
            constant("AERON_COUNTER_MAX_LABEL_LENGTH", "u32"),
            constant("AERON_COUNTER_TYPE_ID_OFFSET", "u32"),
            constant("AERON_COUNTER_RECORD_RECLAIMED", "i32"),
            constant("AERON_ERROR_CODE_NEW_FAILURE", "u32"),
            constant("AERON_ERROR_CODE_UNKNOWN_CODE_VALUE", "i32"),
            constant("AERON_ERROR_CODE_TEXT", "&[u8; 4]"),
            constant("AERON_ERROR_MAX_LENGTH", "u32"),
            constant("AERON_UNRELATED_TYPE_ID", "u32"),
        ];
        let generated = enums(generate(&constants));
        let names: Vec<_> = generated.iter().map(|item| item.ident.to_string()).collect();
        assert_eq!(
            names,
            ["AeronCounterTypeId", "AeronCounterState", "AeronDriverErrorCode"]
        );
        let variants: Vec<Vec<_>> = generated
            .iter()
            .map(|item| item.variants.iter().map(|variant| variant.ident.to_string()).collect())
            .collect();
        assert_eq!(
            variants,
            [
                vec!["NewMeasurement", "Unknown"],
                vec!["Reclaimed", "Unknown"],
                vec!["NewFailure", "UnknownCodeValue", "Unknown"],
            ]
        );
        assert!(generate(&[]).is_empty());
    }

    #[test]
    fn native_families_have_all_expected_variants_without_name_collisions() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/archive.rs");
        let bindings = parse_bindings(&path);
        let generated = enums(generate(&bindings.constants));
        assert_eq!(generated.len(), 4);
        for (item, expected) in generated.iter().zip([184, 3, 16, 20]) {
            let names: std::collections::BTreeSet<_> =
                item.variants.iter().map(|variant| variant.ident.to_string()).collect();
            assert_eq!(item.variants.len(), expected + 1, "{}", item.ident);
            assert_eq!(names.len(), item.variants.len(), "{}", item.ident);
            assert!(matches!(item.variants.last().unwrap().fields, syn::Fields::Unnamed(_)));
            assert!(
                item.attrs
                    .iter()
                    .any(|attribute| attribute.path().is_ident("non_exhaustive"))
            );
        }
    }

    #[test]
    fn archive_enum_uses_both_native_error_families() {
        let constants = [
            constant("AERON_ARCHIVE_ERROR_CODE_FUTURE_FAILURE", "u32"),
            constant("AERON_ARCHIVE_CLIENT_ERROR_FUTURE_CLIENT_FAILURE", "u32"),
            constant("AERON_ERROR_CODE_INVALID_CHANNEL", "u32"),
        ];
        let generated = archive_error_enum(&constants);
        let item = enums(generated.clone()).remove(0);
        let names: Vec<_> = item.variants.iter().map(|variant| variant.ident.to_string()).collect();
        assert_eq!(names, ["FutureFailure", "FutureClientFailure", "Unknown"]);
        let output = generated.to_string();
        assert!(output.contains("code . unsigned_abs ()"));
        assert!(output.contains("Self :: Unknown (code) => return code"));
        assert!(!output.contains("AERON_ERROR_CODE_INVALID_CHANNEL"));
    }

    #[test]
    fn constant_variant_names_preserve_all_words() {
        let constants = [constant("AERON_ERROR_CODE_ON_T", "u32")];
        let generated = enums(generate(&constants));
        assert_eq!(generated[0].variants[0].ident, "OnT");
    }
}
