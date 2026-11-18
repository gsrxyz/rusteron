//! Shared templates for methods built from native signatures and URI keys.

use crate::{ArgProcessing, CWrapper, Method};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::BTreeMap;

/// Native counter IDs are constants, so they do not enter the struct generator.
pub(crate) fn system_counter_enum(constants: &[crate::Arg]) -> TokenStream {
    let variants: Vec<_> = constants
        .iter()
        .filter(|constant| constant.c_type == "u32")
        .filter_map(|constant| {
            constant.name.strip_prefix("AERON_SYSTEM_COUNTER_ID_").map(|name| {
                (
                    format_ident!("{}", crate::snake_to_pascal_case(&name.to_ascii_lowercase())),
                    constant.as_ident(),
                )
            })
        })
        .collect();
    if variants.is_empty() {
        return quote! {};
    }
    let declarations = variants.iter().map(|(variant, constant)| {
        let doc = format!("Native counter `{constant}`.");
        quote! { #[doc = #doc] #variant = #constant }
    });
    let conversions = variants
        .iter()
        .map(|(variant, constant)| quote! { #constant => Ok(Self::#variant) });
    quote! {
        /// Aeron Media Driver system counter IDs, read from the native bindings.
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum AeronSystemCounterType { #(#declarations),* }
        impl std::convert::TryFrom<i32> for AeronSystemCounterType {
            type Error = AeronCError;
            fn try_from(value: i32) -> Result<Self, Self::Error> {
                if value < 0 { return Err(AeronCError::from_code(value)) }
                match value as u32 { #(#conversions,)* _ => Err(AeronCError::from_code(-1)) }
            }
        }
    }
}

pub(crate) fn generate(
    wrapper: &CWrapper,
    wrappers: &BTreeMap<String, CWrapper>,
    constants: &[crate::Arg],
) -> TokenStream {
    let class = format_ident!("{}", wrapper.class_name);
    let native = format_ident!("{}", wrapper.type_name);
    let durations = duration_methods(wrapper);
    let destinations = destination_methods(wrapper, wrappers);
    let uri = uri_methods(wrapper, constants);
    let poller = descriptor_poller_methods(wrapper);
    let durations = if durations.is_empty() {
        quote! {}
    } else {
        quote! { impl<S: ResourceStorage<#native>> #class<S> { #durations } }
    };
    quote! {
        #durations
        #destinations
        #uri
        #poller
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Arg, parse_bindings_with_custom};

    #[test]
    fn system_counter_enum_uses_native_constants_and_preserves_variant_names() {
        let constants = [
            Arg {
                name: "AERON_SYSTEM_COUNTER_ID_BYTES_SENT".into(),
                c_type: "u32".into(),
                processing: ArgProcessing::Default,
            },
            Arg {
                name: "AERON_SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED".into(),
                c_type: "u32".into(),
                processing: ArgProcessing::Default,
            },
            Arg {
                name: "AERON_UNRELATED".into(),
                c_type: "u32".into(),
                processing: ArgProcessing::Default,
            },
        ];
        let syntax = syn::parse2::<syn::File>(system_counter_enum(&constants)).unwrap();
        let syn::Item::Enum(counter) = &syntax.items[0] else {
            panic!()
        };
        assert_eq!(counter.variants.len(), 2);
        assert_eq!(counter.variants[0].ident, "BytesSent");
        assert_eq!(counter.variants[1].ident, "PublicationsRevoked");
        for (variant, constant) in counter.variants.iter().zip(&constants) {
            let syn::Expr::Path(path) = &variant.discriminant.as_ref().unwrap().1 else {
                panic!()
            };
            assert!(path.path.is_ident(&constant.name));
        }
        assert!(system_counter_enum(&[]).is_empty());
    }

    fn methods(tokens: TokenStream) -> BTreeMap<String, syn::ImplItemFn> {
        syn::parse2::<syn::ItemImpl>(quote! { impl Example { #tokens } })
            .unwrap()
            .items
            .into_iter()
            .filter_map(|item| match item {
                syn::ImplItem::Fn(method) => Some((method.sig.ident.to_string(), method)),
                _ => None,
            })
            .collect()
    }

    fn context() -> CWrapper {
        let arg = |name: &str, c_type: &str| Arg {
            name: name.into(),
            c_type: c_type.into(),
            processing: ArgProcessing::Default,
        };
        CWrapper {
            type_name: "example_context_t".into(),
            methods: vec![
                Method {
                    fn_name: "example_context_set_timeout_ms".into(),
                    struct_method_name: "set_timeout_ms".into(),
                    arguments: vec![arg("context", "* mut example_context_t"), arg("value", "u64")],
                    return_type: arg("", ":: std :: os :: raw :: c_int"),
                    docs: Default::default(),
                },
                Method {
                    fn_name: "example_context_get_timeout_ms".into(),
                    struct_method_name: "get_timeout_ms".into(),
                    arguments: vec![arg("context", "* mut example_context_t")],
                    return_type: arg("", "u64"),
                    docs: Default::default(),
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn duration_helpers_use_matching_units_and_only_accept_valid_native_pairs() {
        let mut wrapper = context();
        let generated = methods(duration_methods(&wrapper));
        assert_eq!(generated.len(), 2);
        assert!(
            quote::ToTokens::to_token_stream(&generated["set_timeout"])
                .to_string()
                .contains("duration_to_millis")
        );
        assert!(
            quote::ToTokens::to_token_stream(&generated["get_timeout"])
                .to_string()
                .contains("from_millis")
        );
        wrapper.methods[0].arguments[1].c_type = "bool".into();
        assert!(duration_methods(&wrapper).is_empty());
        wrapper.methods[0].arguments[1].c_type = "u64".into();
        wrapper.methods[1].return_type.c_type = "i64".into();
        assert!(duration_methods(&wrapper).is_empty());
        wrapper.methods.pop();
        assert!(duration_methods(&wrapper).is_empty());
    }

    #[test]
    fn duration_helpers_preserve_native_and_custom_overrides() {
        let mut wrapper = context();
        wrapper.skipped_methods.insert("set_timeout".into());
        assert_eq!(
            methods(duration_methods(&wrapper))
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["get_timeout"]
        );
        wrapper.methods[1].struct_method_name = "get_timeout".into();
        assert!(duration_methods(&wrapper).is_empty());
    }

    #[test]
    fn uri_keys_have_unique_methods_and_use_checked_duration_conversion() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let bindings = parse_bindings_with_custom(&path, &[]);
        let wrapper = CWrapper {
            type_name: "aeron_uri_string_builder_t".into(),
            ..Default::default()
        };
        let syntax = syn::parse2::<syn::File>(uri_methods(&wrapper, &bindings.constants)).unwrap();
        let syn::Item::Impl(implementation) = &syntax.items[0] else {
            panic!()
        };
        let names: std::collections::BTreeSet<_> = implementation
            .items
            .iter()
            .map(|item| {
                let syn::ImplItem::Fn(method) = item else { panic!() };
                method.sig.ident.to_string()
            })
            .collect();
        assert_eq!(names.len(), 42 + 5);
        assert_eq!(names.len(), implementation.items.len());
        assert!(!names.contains("timestamp_offset_reserved"));
        let output = uri_methods(&wrapper, &bindings.constants).to_string();
        assert_eq!(output.matches("duration_to_nanos").count(), 5);
        assert!(uri_methods(&context(), &bindings.constants).is_empty());
    }

    #[test]
    fn uri_names_strip_native_prefixes_and_only_accept_string_keys() {
        for (constant, c_type, expected) in [
            ("AERON_URI_ALIAS_KEY", "&[u8; 6]", Some("alias")),
            (
                "AERON_UDP_CHANNEL_CONTROL_MODE_KEY",
                "& [u8 ; 13usize]",
                Some("control_mode"),
            ),
            ("AERON_URI_STRING_BUILDER_MEDIA_KEY", "&[u8; 8]", Some("media")),
            ("AERON_URI_STRING_BUILDER_PREFIX_KEY", "&[u8; 9]", Some("prefix")),
            ("AERON_URI_TIMESTAMP_OFFSET_RESERVED", "&[u8; 9]", None),
            ("AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE", "&[u8; 7]", None),
            ("AERON_URI_MAX_LENGTH", "u32", None),
            ("AERON_CONTEXT_ALIAS_KEY", "&[u8; 6]", None),
            ("AERON_URI_NUMERIC_KEY", "u32", None),
            ("AERON_URI_WIDE_KEY", "&[u16; 6]", None),
            ("AERON_URI_MUTABLE_KEY", "&mut [u8; 6]", None),
            ("AERON_URI_EMPTY_KEY", "not a type", None),
            ("AERON_URI__KEY", "&[u8; 1]", None),
        ] {
            let constant = Arg {
                name: constant.into(),
                c_type: c_type.into(),
                processing: ArgProcessing::Default,
            };
            assert_eq!(uri_parameter_name(&constant).as_deref(), expected, "{}", constant.name);
        }
    }

    #[test]
    fn uri_generation_accepts_new_keys_and_preserves_overrides() {
        let constants = ["AERON_URI_FUTURE_OPTION_KEY", "AERON_URI_NAK_DELAY_KEY"].map(|name| Arg {
            name: name.into(),
            c_type: "&[u8; 16]".into(),
            processing: ArgProcessing::Default,
        });
        let mut wrapper = CWrapper {
            type_name: "aeron_uri_string_builder_t".into(),
            ..Default::default()
        };
        let generated = uri_methods(&wrapper, &constants);
        let output = generated.to_string();
        syn::parse2::<syn::File>(generated).unwrap();
        assert!(output.contains("fn future_option (& self , value : & str)"));
        assert!(output.contains("from_bytes_until_nul (AERON_URI_FUTURE_OPTION_KEY)"));
        assert!(output.contains("fn nak_delay (& self , value : i64)"));
        assert!(output.contains("fn nak_delay_duration"));
        assert!(!output.contains("fn alias"));

        wrapper
            .skipped_methods
            .extend(["future_option".into(), "nak_delay_duration".into()]);
        wrapper.methods.push(Method {
            struct_method_name: "nak_delay".into(),
            ..context().methods.remove(0)
        });
        let syn::Item::Impl(implementation) = syn::parse2::<syn::Item>(uri_methods(&wrapper, &constants)).unwrap()
        else {
            panic!()
        };
        assert!(implementation.items.is_empty());
    }

    #[test]
    fn destination_helpers_follow_available_native_operations() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let mut bindings = parse_bindings_with_custom(&path, &[]);
        for (native, expected) in [
            ("aeron_subscription_t", 4),
            ("aeron_publication_t", 6),
            ("aeron_exclusive_publication_t", 6),
        ] {
            let code = destination_methods(&bindings.wrappers[native], &bindings.wrappers);
            let syn::Item::Impl(implementation) = syn::parse2::<syn::Item>(code).unwrap() else {
                panic!()
            };
            assert_eq!(implementation.items.len(), expected);
        }
        let wrapper = bindings.wrappers["aeron_publication_t"].clone();
        bindings.wrappers.remove("aeron_async_destination_t");
        assert!(destination_methods(&wrapper, &bindings.wrappers).is_empty());
    }
}

fn duration_methods(wrapper: &CWrapper) -> TokenStream {
    let mut output = TokenStream::new();
    for setter in &wrapper.methods {
        let Some(name) = setter.struct_method_name.strip_prefix("set_") else {
            continue;
        };
        let Some((name, unit)) = name.rsplit_once('_') else {
            continue;
        };
        if !matches!(unit, "ns" | "ms")
            || setter.arguments.len() != 2
            || setter.arguments[0].base_type() != wrapper.type_name
            || setter.arguments[1].c_type != "u64"
            || !setter.return_type.is_c_raw_int()
        {
            continue;
        }
        let getter_name = format!("get_{name}_{unit}");
        let Some(getter) = wrapper
            .methods
            .iter()
            .find(|method| method.struct_method_name == getter_name)
        else {
            continue;
        };
        if getter.return_type.c_type != "u64"
            || getter.arguments.len() != 1
            || getter.arguments[0].base_type() != wrapper.type_name
        {
            continue;
        }
        let set = format_ident!("set_{name}");
        let get = format_ident!("get_{name}");
        let native_set = format_ident!("{}", setter.struct_method_name);
        let native_get = format_ident!("{}", getter.struct_method_name);
        let conversion = if unit == "ns" {
            quote! { duration_to_nanos(value)? as u64 }
        } else {
            quote! { duration_to_millis(value)? }
        };
        let from = format_ident!("from_{}", if unit == "ns" { "nanos" } else { "millis" });
        if !has_method(wrapper, &set.to_string()) {
            output.extend(quote! {
                /// Set a duration. Millisecond settings round fractional milliseconds up.
                /// Returns an error if the value exceeds Aeron's signed nanosecond clock range.
                #[inline]
                pub fn #set(&self, value: std::time::Duration) -> Result<i32, AeronCError> {
                    self.#native_set(#conversion)
                }
            });
        }
        if !has_method(wrapper, &get.to_string()) {
            output.extend(quote! {
                /// Read the configured duration.
                #[inline]
                pub fn #get(&self) -> std::time::Duration {
                    std::time::Duration::#from(self.#native_get())
                }
            });
        }
    }
    output
}

fn has_method(wrapper: &CWrapper, name: &str) -> bool {
    wrapper.skipped_methods.contains(name) || wrapper.methods.iter().any(|method| method.struct_method_name == name)
}

fn destination_methods(wrapper: &CWrapper, wrappers: &BTreeMap<String, CWrapper>) -> TokenStream {
    let Some(destination) = wrappers.get("aeron_async_destination_t") else {
        return quote! {};
    };
    let class = format_ident!("{}", wrapper.class_name);
    let mut methods = TokenStream::new();
    for operation in ["add_destination", "remove_destination", "remove_destination_by_id"] {
        let native_name = format!("{}_async_{operation}", wrapper.without_name);
        let Some(method) = destination.methods.iter().find(|method| method.fn_name == native_name) else {
            continue;
        };
        // Destination operations retain a client, this handle and one address or id.
        if method.arguments.len() != 4
            || method.arguments[1].base_type() != "aeron_t"
            || method.arguments[2].base_type() != wrapper.type_name
        {
            continue;
        }
        let argument = &method.arguments[3];
        if !argument.is_c_string() && argument.c_type != "i64" {
            continue;
        }
        let argument_name = argument.as_ident();
        let argument_type = if argument.is_c_string() {
            quote! { &std::ffi::CStr }
        } else {
            quote! { i64 }
        };
        let native = format_ident!("{}", native_name);
        let asynchronous = format_ident!("async_{operation}");
        let blocking = format_ident!("{operation}");
        if !has_method(wrapper, &asynchronous.to_string()) {
            methods.extend(quote! {
                /// Start a destination change. Dropping the operation does not cancel it.
                #[inline]
                pub fn #asynchronous(&self, client: &Aeron, #argument_name: #argument_type)
                    -> Result<AeronAsyncDestination, AeronCError>
                {
                    AeronAsyncDestination::#native(client, self, #argument_name)
                }
            });
        }
        if !has_method(wrapper, &blocking.to_string()) {
            methods.extend(quote! {
                /// Wait for a destination change or the timeout. Drives an invoker conductor when needed.
                /// The handle retains its owning client. A timeout does not cancel the native operation.
                pub fn #blocking(&self, #argument_name: #argument_type, timeout: std::time::Duration)
                    -> Result<(), AeronCError>
                {
                    let client = self.inner.get_handle_dependency::<Aeron>()
                        .ok_or_else(|| AeronCError::with_message(-1, "handle has no owning Aeron client"))?;
                    let result = self.#asynchronous(&client, #argument_name)?;
                    poll_destination_op_to_completion(&client, &result, timeout, (#argument_name, self))
                }
            });
        }
    }
    if methods.is_empty() {
        quote! {}
    } else {
        quote! { impl #class { #methods } }
    }
}

// These pollers retain their reset callback between calls. Archive's internal pollers
// only borrow storage and can hold callbacks whose Rust owner has already returned.
pub(crate) fn is_descriptor_poller(wrapper: &CWrapper) -> bool {
    matches!(
        wrapper.type_name.as_str(),
        "aeron_archive_recording_descriptor_poller_t" | "aeron_archive_recording_subscription_descriptor_poller_t"
    )
}

pub(crate) fn replaces_method(wrapper: &CWrapper, method: &Method) -> bool {
    is_descriptor_poller(wrapper) && matches!(method.struct_method_name.as_str(), "reset" | "poll")
}

fn descriptor_poller_methods(wrapper: &CWrapper) -> TokenStream {
    if !is_descriptor_poller(wrapper) {
        return quote! {};
    }
    let class = format_ident!("{}", wrapper.class_name);
    let native = format_ident!("{}", wrapper.type_name);
    let stem = &wrapper.without_name;
    let create = format_ident!("{stem}_create");
    let close = format_ident!("{stem}_close");
    let reset = wrapper
        .methods
        .iter()
        .find(|method| method.struct_method_name == "reset")
        .expect("descriptor reset");
    let poll = format_ident!("{stem}_poll");
    let count = reset.arguments[2].as_ident();
    let consumer = reset
        .arguments
        .iter()
        .find(|arg| matches!(arg.processing, ArgProcessing::Handler(_)))
        .expect("descriptor consumer");
    let consumer_name = consumer.as_ident();
    let callback_type = format_ident!("{}", consumer.c_type);
    let callback_trait = format_ident!("{}Callback", crate::snake_to_pascal_case(&consumer.c_type));
    let trampoline = format_ident!("{}_callback_catch", consumer.c_type);
    let reset = format_ident!("{}", reset.fn_name);
    let borrowed_error = format!("{reset}: this poller is borrowed, so it cannot keep the consumer alive");
    quote! {
        impl #class {
            /// Create a descriptor poller that retains the Archive and its callback.
            /// Send a request through the Archive proxy, then reset and poll this poller.
            /// It shares the Archive response stream. Do not use a blocking Archive call at the same time.
            pub fn new(archive: &AeronArchive, fragment_limit: i32) -> Result<Self, AeronCError> {
                let context = archive.get_archive_context().get_inner();
                let subscription = archive.get_control_response_subscription().get_inner();
                let control_session_id = archive.control_session_id();
                let resource = ManagedCResource::<#native>::new(
                    move |poller| unsafe { #create(poller, context, subscription, control_session_id, fragment_limit) },
                    Some(Box::new(move |poller| unsafe { #close(*poller) })), false,
                )?;
                let poller = Self { inner: CResource::new(ResourceRef::new(resource)) };
                poller.inner.add_handle_dependency(archive);
                Ok(poller)
            }
        }
        impl<S: ResourceStorage<#native>> #class<S> {
            /// Install the consumer for the next poll. Borrowed pollers reject this call.
            #[inline]
            pub fn reset<H: #callback_trait + 'static>(&self, correlation_id: i64, #count: i32,
                #consumer_name: Option<&Handler<H>>) -> Result<(), AeronCError>
            {
                let Some(inner) = self.inner.as_owned() else {
                    return Err(AeronCError::with_message(-1, #borrowed_error));
                };
                let Some(handler) = #consumer_name else {
                    return Err(AeronCError::with_message(-1, "descriptor polling requires a consumer"));
                };
                let callback: #callback_type = Some(#trampoline::<H, LocalCallback> as _);
                #[cfg(feature = "log-c-bindings")]
                log::info!("{}({correlation_id}, {})", stringify!(#reset), #count);
                unsafe { #reset(self.get_inner(), correlation_id, #count, callback, handler.as_raw()) };
                // C stores this pointer until the poller closes or another reset replaces it.
                inner.add_dependency(handler.clone());
                Ok(())
            }

            /// Poll with the installed consumer. Resumes a consumer panic after C returns.
            #[inline]
            pub fn poll(&mut self) -> Result<i32, AeronCError> {
                if self.inner.as_owned().is_none() {
                    return Err(AeronCError::with_message(-1, "a borrowed descriptor poller cannot be polled"));
                }
                if unsafe { (*self.get_inner()).#consumer_name.is_none() } {
                    return Err(AeronCError::with_message(-1, "reset the descriptor poller before polling"));
                }
                let result = unsafe { #poll(self.get_inner()) };
                #[cfg(feature = "log-c-bindings")]
                log::info!("{}() -> {result}", stringify!(#poll));
                resume_caught_panic();
                if result < 0 { Err(AeronCError::from_code(result)) } else { Ok(result) }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum UriValue {
    Text,
    Int32,
    Int64,
    Bool,
    Byte,
    Media,
    ControlMode,
    Nanos,
}

// Constant names identify parameters. Their C type is always a byte string,
// so the value types and time units below come from Aeron's URI definitions.
fn uri_value(name: &str) -> UriValue {
    match name {
        "media" => UriValue::Media,
        "control_mode" => UriValue::ControlMode,
        "sparse_term" | "reliable" | "eos" | "tether" | "group" | "rejoin" | "spies_simulate_connection" | "ats" => {
            UriValue::Bool
        }
        "initial_term_id" | "term_id" | "term_offset" | "mtu_length" | "ttl" | "socket_sndbuf" | "socket_rcvbuf"
        | "receiver_window" | "max_resend" | "stream_id" | "publication_window" => UriValue::Int32,
        "socket_tos" => UriValue::Byte,
        "response_correlation_id" => UriValue::Int64,
        "linger_timeout"
        | "nak_delay"
        | "untethered_window_limit_timeout"
        | "untethered_resting_timeout"
        | "untethered_linger_timeout" => UriValue::Nanos,
        _ => UriValue::Text,
    }
}

fn uri_parameter_name(constant: &crate::Arg) -> Option<String> {
    let key = constant.name.strip_suffix("_KEY")?;
    let name = key
        .strip_prefix("AERON_URI_STRING_BUILDER_")
        .or_else(|| key.strip_prefix("AERON_URI_"))
        .or_else(|| key.strip_prefix("AERON_UDP_CHANNEL_"))?;
    // URI values and numeric limits are not parameter keys.
    let syn::Type::Reference(reference) = syn::parse_str(&constant.c_type).ok()? else {
        return None;
    };
    let syn::Type::Array(array) = *reference.elem else {
        return None;
    };
    let syn::Type::Path(element) = *array.elem else {
        return None;
    };
    if reference.mutability.is_some() || !element.path.is_ident("u8") || name.is_empty() {
        return None;
    }
    Some(name.to_ascii_lowercase())
}

fn uri_methods(wrapper: &CWrapper, constants: &[crate::Arg]) -> TokenStream {
    if wrapper.type_name != "aeron_uri_string_builder_t" {
        return quote! {};
    }
    let methods = constants
        .iter()
        .filter_map(|constant| uri_parameter_name(constant).map(|name| (name, constant.as_ident())))
        .map(|(name, key)| {
            let kind = uri_value(&name);
            let method = format_ident!("{name}");
            let (ty, put, value) = match kind {
                UriValue::Text => (quote! { &str }, quote! { put_str }, quote! { value }),
                UriValue::Int32 => (quote! { i32 }, quote! { put_int32 }, quote! { value }),
                UriValue::Int64 | UriValue::Nanos => (quote! { i64 }, quote! { put_int64 }, quote! { value }),
                UriValue::Byte => (quote! { u8 }, quote! { put_int32 }, quote! { i32::from(value) }),
                UriValue::Bool => (
                    quote! { bool },
                    quote! { put_str },
                    quote! { if value { "true" } else { "false" } },
                ),
                UriValue::Media => (quote! { Media }, quote! { put_str }, quote! { value.as_str() }),
                UriValue::ControlMode => (quote! { ControlMode }, quote! { put_str }, quote! { value.as_str() }),
            };
            let duration = if matches!(kind, UriValue::Nanos) && !has_method(wrapper, &format!("{name}_duration")) {
                let duration = format_ident!("{name}_duration");
                quote! {
                    /// Set the URI parameter in nanoseconds. Rejects values above `i64::MAX`.
                    pub fn #duration(&self, value: std::time::Duration) -> Result<&Self, AeronCError> {
                        self.#method(duration_to_nanos(value)?)
                    }
                }
            } else {
                quote! {}
            };
            let setter = if has_method(wrapper, &name) {
                quote! {}
            } else {
                let doc = format!("Set the URI parameter named by `{key}`.");
                quote! {
                    #[doc = #doc]
                    pub fn #method(&self, value: #ty) -> Result<&Self, AeronCError> {
                        let key = std::ffi::CStr::from_bytes_until_nul(#key)
                            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
                        self.#put(key, #value)?;
                        Ok(self)
                    }
                }
            };
            quote! { #setter #duration }
        });
    quote! { impl AeronUriStringBuilder { #(#methods)* } }
}
