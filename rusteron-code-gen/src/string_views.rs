//! Borrowing rules for text returned by native getters.

use crate::Method;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StringView {
    Borrowed,
    BorrowedWithLength(&'static str),
    Static,
    Unchecked,
}

pub(crate) fn method(wrapper: &str, method: &Method) -> StringView {
    match method.fn_name.as_str() {
        "aeron_version_full"
        | "aeron_version_text"
        | "aeron_version_gitsha"
        | "aeron_archive_client_version_text"
        | "aeron_archive_client_version_git_sha"
        | "aeron_driver_version_text"
        | "aeron_driver_version_git_sha"
        | "aeron_driver_threading_mode_to_string"
        | "aeron_driver_uri_get_offset_info"
        | "aeron_error_code_str" => StringView::Static,
        "aeron_publication_channel" | "aeron_cnc_filename" => StringView::Borrowed,
        _ => {
            let _ = wrapper;
            StringView::Unchecked
        }
    }
}

pub(crate) fn field(wrapper: &str, field: &str) -> StringView {
    match (wrapper, field) {
        ("aeron_archive_encoded_credentials_t", "data") => StringView::BorrowedWithLength("length"),
        // The role name stays fixed until unsafe native close. Runner constructors
        // require its storage to remain valid for every use of the runner.
        ("aeron_agent_runner_t", "role_name")
        | ("aeron_publication_constants_t" | "aeron_subscription_constants_t", "channel")
        | ("aeron_image_constants_t", "source_identity")
        | ("aeron_archive_recording_descriptor_t", "stripped_channel" | "original_channel" | "source_identity")
        | ("aeron_archive_recording_subscription_descriptor_t", "stripped_channel")
        | ("aeron_archive_replication_params_t", "live_destination" | "replication_channel" | "src_response_channel") => {
            StringView::Borrowed
        }
        _ => StringView::Unchecked,
    }
}

pub(crate) fn has_owned_method(wrapper: &str, method: &Method) -> bool {
    (wrapper.ends_with("_context_t") && method.struct_method_name.starts_with("get_"))
        || method.fn_name == "aeron_errmsg"
        || method.fn_name == "aeron_uri_string_builder_get"
}

pub(crate) fn has_owned_field(wrapper: &str) -> bool {
    wrapper.ends_with("_context_t") || wrapper == "aeron_archive_control_response_poller_t"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native(name: &str) -> Method {
        Method {
            fn_name: name.into(),
            struct_method_name: name.rsplit('_').next().unwrap().into(),
            arguments: vec![],
            return_type: crate::Arg {
                name: String::new(),
                c_type: "* const :: std :: os :: raw :: c_char".into(),
                processing: crate::ArgProcessing::Default,
            },
            docs: Default::default(),
        }
    }

    #[test]
    fn unknown_text_requires_an_explicit_borrow_contract() {
        assert_eq!(method("future_t", &native("aeron_future_text")), StringView::Unchecked);
        assert_eq!(field("future_t", "text"), StringView::Unchecked);
        assert_eq!(method("aeron_t", &native("aeron_errmsg")), StringView::Unchecked);
        assert_eq!(method("aeron_t", &native("aeron_version_text")), StringView::Static);
        assert_eq!(
            method("aeron_publication_t", &native("aeron_publication_channel")),
            StringView::Borrowed
        );
        assert_eq!(
            field("aeron_archive_recording_descriptor_t", "original_channel"),
            StringView::Borrowed
        );
        assert_eq!(
            field("aeron_archive_control_response_poller_t", "error_message"),
            StringView::Unchecked
        );
    }

    #[test]
    fn generated_text_getters_only_copy_when_the_name_requests_ownership() {
        use quote::ToTokens;
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/archive.rs");
        let bindings = crate::parse_bindings_with_custom(&path, &[crate::CUSTOM_ARCHIVE_CODE]);
        for (native, name, borrowed_safety) in [
            ("aeron_archive_context_t", "get_control_response_channel", false),
            ("aeron_archive_recording_descriptor_t", "original_channel", true),
            ("aeron_publication_t", "channel", true),
            ("aeron_t", "errmsg", false),
        ] {
            let generated = crate::generate_rust_code(
                &bindings.wrappers[native],
                &bindings.wrappers,
                false,
                false,
                true,
                &bindings.handlers,
                &bindings.constants,
            );
            let file = syn::parse2::<syn::File>(generated).unwrap();
            let methods: Vec<_> = file
                .items
                .into_iter()
                .filter_map(|item| match item {
                    syn::Item::Impl(item) => Some(item.items),
                    _ => None,
                })
                .flatten()
                .filter_map(|item| match item {
                    syn::ImplItem::Fn(item) => Some(item),
                    _ => None,
                })
                .collect();
            let getter = methods.iter().find(|method| method.sig.ident == name).unwrap();
            assert_eq!(
                matches!(getter.sig.safety, syn::Safety::Default),
                borrowed_safety,
                "{native}::{name}"
            );
            assert!(getter.sig.output.to_token_stream().to_string().ends_with("str"));
            let body = getter.block.to_token_stream().to_string();
            assert!(!body.contains("to_owned"), "{native}::{name}");
            assert!(!body.contains("String ::"), "{native}::{name}");
            if !borrowed_safety {
                let owned_name = format!("{name}_owned");
                let owned = methods.iter().find(|method| method.sig.ident == owned_name).unwrap();
                assert!(matches!(owned.sig.safety, syn::Safety::Default));
                assert!(owned.block.to_token_stream().to_string().contains("to_owned"));
            }
        }
    }
}
