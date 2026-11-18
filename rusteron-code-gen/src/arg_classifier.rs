//! Argument classification for generated wrapper methods.
//!
//! `classify_method_args` classifies each C argument once into a [`ClassifiedArg`] carrying all token
//! fragments the emitters need (signature, FFI call, generics, retained-handler registration,
//! and the `_fn` stack-closure variant). `generate_methods` then emits purely from the classification.

use crate::generator::{
    Arg, ArgProcessing, CHandler, CWrapper, Method, ReturnType, UNWIND_HANDLER_TYPES, UNWIND_POLL_FNS,
    is_sync_handler_type,
};
use crate::snake_to_pascal_case;
use crate::thread_safety::owner_is_shared;
use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use std::collections::BTreeMap;
use syn::{Type, parse_str};

/// Role of a C argument in the generated Rust wrapper method.
///
/// Each argument is classified once: is it `&self`? A `&OtherWrapper` param? A
/// callback that must be retained? etc. The classification drives all emission
/// (signature, FFI call, handler registration, and the `_fn` variant).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgRole {
    /// `*mut <own type>`. Becomes `&self` / `self.get_inner()`.
    SelfPointer,
    /// `*mut <other wrapper>`. Becomes an `&OtherWrapper` param and a `name.get_inner()` call.
    WrapperPointer,
    /// Callback invoked only during the FFI call. Typed `Option<&Handler<T>>`, `_fn` capable.
    HandlerSync,
    /// Callback the C client retains. Borrow style (`Option<&Handler<T>>`), registered
    /// as a dependency of `self`.
    HandlerRetainedBorrowed,
    /// Callback the C client retains. Owned setter style (`Option<T>`), heap-allocated in
    /// a prelude, registered, and returned to the caller.
    HandlerRetainedOwned,
    /// clientd / length / merged arguments that emit nothing of their own.
    Absorbed,
    /// Everything else, handled by the existing `ReturnType` conversions.
    Plain,
}

/// One argument, classified once, with every emission fragment precomputed.
#[derive(Clone)]
pub struct ClassifiedArg {
    pub kind: ArgRole,
    /// `name: Type` in the generated signature (None: not part of the signature).
    pub signature: Option<TokenStream>,
    /// Argument(s) for the FFI call (None: emits nothing).
    pub call: Option<TokenStream>,
    /// Generic bound contributed to the where clause.
    pub generic: Option<TokenStream>,
    /// Post-call dependency registration for retained handlers.
    pub registration: Option<TokenStream>,
    /// Statement(s) emitted before the FFI call (e.g. the `IntoCStr` shadow for
    /// C-string args so the `Cow` outlives the call).
    pub prelude: Option<TokenStream>,
    /// Storage borrowed for the complete synchronous call, excluding `_fn` calls.
    pub handler_prelude: Option<TokenStream>,
    /// `_fn` variant fragments (sync handlers swap Handler for a stack closure).
    pub once_signature: Option<TokenStream>,
    pub once_call: Option<TokenStream>,
    pub once_generic: Option<TokenStream>,
    /// Post-call statement that raises a panic the handler trampoline caught
    pub resume: Option<TokenStream>,
    pub once_resume: Option<TokenStream>,
}

#[derive(Clone)]
pub struct ClassifiedMethodArgs {
    pub uses_self: bool,
    pub args: Vec<ClassifiedArg>,
    /// The owned retained-handler argument, when this method is an owned setter.
    pub owned_retained: Option<Arg>,
    /// Every handler argument is sync, so a `_fn` stack-closure variant is sound.
    pub once_capable: bool,
}

impl ClassifiedMethodArgs {
    /// Poll callbacks finish before C returns. An exclusive Rust borrow is
    /// sufficient; this form does not need shared callback storage or a guard.
    pub fn exclusive_poll(&self, method: &Method) -> Option<Self> {
        if !self.once_capable || !method.struct_method_name.ends_with("poll") {
            return None;
        }
        let mut direct = self.clone();
        let mut found = false;
        for (original, arg) in method.arguments.iter().zip(&mut direct.args) {
            if arg.kind != ArgRole::HandlerSync {
                continue;
            }
            let ArgProcessing::Handler(parts) = &original.processing else {
                unreachable!()
            };
            let handler = &parts[0];
            if !UNWIND_HANDLER_TYPES.contains(&handler.c_type.as_str()) {
                return None;
            }
            found = true;
            let name = original.as_ident();
            let ty = format_ident!("{}HandlerImpl", snake_to_pascal_case(&handler.c_type));
            let callback_trait = format_ident!("{}Callback", snake_to_pascal_case(&handler.c_type));
            let unwinds = UNWIND_POLL_FNS.contains(&method.fn_name.as_str());
            let suffix = if unwinds { "_unwind" } else { "_catch" };
            let shim = format_ident!("{}_callback_for_borrowed_handler{}", handler.c_type, suffix);
            arg.signature = Some(quote! { #name: &mut #ty });
            arg.generic = Some(quote! { #ty: #callback_trait });
            arg.prelude = None;
            arg.handler_prelude = None;
            arg.call = Some(quote! { Some(#shim::<#ty>), #name as *mut #ty as *mut std::os::raw::c_void });
            arg.resume = Some(if unwinds {
                quote! { if #ty::RESUMES_CAUGHT_PANIC { resume_caught_panic(); } }
            } else {
                quote! { resume_caught_panic(); }
            });
            // Native assemblers can bypass the Rust fragment trampoline.
            if unwinds && handler.c_type != "aeron_block_handler_t" {
                let callback = format_ident!("{}_callback", original.name);
                let clientd = format_ident!("{}_clientd", original.name);
                let native_type = handler.as_type();
                arg.handler_prelude = Some(quote! {
                    let (#callback, #clientd): (#native_type, *mut std::os::raw::c_void) =
                        if let Some(native) = #name.native_poll_callback() {
                            (native.callback, native.clientd)
                        } else {
                            (Some(#shim::<#ty>), #name as *mut #ty as *mut _)
                        };
                });
                arg.call = Some(quote! { #callback, #clientd });
            }
        }
        found.then_some(direct)
    }

    pub fn signatures(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.signature.clone()).collect()
    }

    pub fn calls(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.call.clone()).collect()
    }

    pub fn generics(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.generic.clone()).collect()
    }

    pub fn registrations(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.registration.clone()).collect()
    }

    pub fn resumes(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.resume.clone()).collect()
    }

    pub fn once_resumes(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.once_resume.clone()).collect()
    }

    /// Statements emitted before the FFI call (e.g. `IntoCStr` shadows).
    pub fn preludes(&self) -> Vec<TokenStream> {
        self.args
            .iter()
            .flat_map(|a| [a.prelude.clone(), a.handler_prelude.clone()])
            .flatten()
            .collect()
    }

    pub fn once_signatures(&self) -> Vec<TokenStream> {
        self.args
            .iter()
            .filter_map(|a| a.once_signature.clone().or_else(|| a.signature.clone()))
            .collect()
    }

    pub fn once_calls(&self) -> Vec<TokenStream> {
        self.args
            .iter()
            .filter_map(|a| a.once_call.clone().or_else(|| a.call.clone()))
            .collect()
    }

    pub fn once_generics(&self) -> Vec<TokenStream> {
        self.args
            .iter()
            .filter_map(|a| a.once_generic.clone().or_else(|| a.generic.clone()))
            .collect()
    }
}

/// Classify every argument of `method` once. `owned_retained` mirrors the owned-setter
/// rule: `&self` method, int return, exactly one retained handler, no `&mut` primitives.
pub fn classify_method_args(
    method: &Method,
    own_type_name: &str,
    wrappers: &BTreeMap<String, CWrapper>,
    closure_handlers: &[CHandler],
) -> ClassifiedMethodArgs {
    let uses_self_precheck = method
        .arguments
        .iter()
        .any(|arg| arg.is_single_mut_pointer() && arg.c_type.ends_with(own_type_name));
    let handler_value_args: Vec<&Arg> = method
        .arguments
        .iter()
        .filter(|a| matches!(a.processing, ArgProcessing::Handler(_)) && !a.is_mut_pointer())
        .collect();
    let has_mut_primitive = method.arguments.iter().any(|a| a.is_mut_pointer() && a.is_primitive());
    let owned_retained: Option<Arg> = if uses_self_precheck
        && !has_mut_primitive
        && method.return_type.is_c_raw_int()
        && handler_value_args.len() == 1
    {
        let a = handler_value_args[0];
        match &a.processing {
            ArgProcessing::Handler(hc) if !is_sync_handler_type(&hc[0].c_type) => Some(a.clone()),
            _ => None,
        }
    } else {
        None
    };
    let once_capable = !handler_value_args.is_empty()
        && handler_value_args.iter().all(|arg| match &arg.processing {
            ArgProcessing::Handler(hc) => is_sync_handler_type(&hc[0].c_type),
            _ => false,
        });

    let mut uses_self = false;
    let mut args: Vec<ClassifiedArg> = method
        .arguments
        .iter()
        .map(|arg| {
            classify_arg(
                arg,
                own_type_name,
                wrappers,
                closure_handlers,
                &owned_retained,
                &method.fn_name,
                &mut uses_self,
            )
        })
        .collect();

    for (arg, classified) in method.arguments.iter().zip(args.iter_mut()) {
        if classified.kind == ArgRole::WrapperPointer && setter_borrows_arg(method, arg) {
            let name = arg.as_ident();
            let keep_alive = quote! {
                if let Some(__inner) = self.inner.as_owned() {
                    __inner.add_handle_dependency(#name);
                }
            };
            // keep it only if C accepted it (void setters cannot fail)
            classified.registration = Some(if method.return_type.is_c_raw_int() {
                quote! { if result >= 0 { #keep_alive } }
            } else {
                keep_alive
            });
            // Both allocations need stable, retained owners. Cloning a stack
            // wrapper would copy its contents, leaving C with the original address.
            let msg = format!(
                "{}: both this wrapper and `{}` require owning heap storage so the retained pointer stays valid",
                method.fn_name, arg.name
            );
            if method.return_type.is_c_raw_int() {
                let bind_conductor = if arg.base_type() == "aeron_t" {
                    quote! { #name.resource().as_owned().unwrap().bind_conductor_thread()?; }
                } else {
                    quote! {}
                };
                classified.prelude = Some(quote! {
                    if self.inner.as_owned().is_none() || #name.resource().as_owned().is_none() {
                        return Err(AeronCError::with_message(-1, #msg));
                    }
                    #bind_conductor
                });
            } else {
                classified.prelude = Some(quote! {
                    assert!(self.inner.as_owned().is_some() && #name.resource().as_owned().is_some(), #msg);
                });
            }
        }
    }

    ClassifiedMethodArgs {
        uses_self,
        args,
        owned_retained,
        once_capable,
    }
}

// exceptions to the general ownership rule
/// Types a C setter takes ownership of. C frees them, so `self` must not keep them too.
/// The generator also derives from this list that their setters are `unsafe` and their
/// getters return `Option`, as C holds NULL until the setter runs
const C_OWNED_SET_TYPES: &[&str] = &["aeron_counter_t", "aeron_udp_channel_interceptor_bindings_t"];

pub(crate) fn is_c_owned_set_type(c_type: &str) -> bool {
    C_OWNED_SET_TYPES.iter().any(|t| c_type.ends_with(t))
}

/// Whether a C setter keeps a pointer to `arg` without owning it (e.g.
/// aeron_archive_context_set_aeron). If so, `self` must keep `arg` alive
fn setter_borrows_arg(method: &Method, arg: &Arg) -> bool {
    method.fn_name.contains("_set_") && !is_c_owned_set_type(&arg.c_type)
}

fn own_archive_callback_setter(name: &str) -> bool {
    name.starts_with("aeron_archive_context_set_")
}

#[allow(clippy::too_many_arguments)]
fn classify_arg(
    arg: &Arg,
    own_type_name: &str,
    wrappers: &BTreeMap<String, CWrapper>,
    closure_handlers: &[CHandler],
    owned_retained: &Option<Arg>,
    fn_name: &str,
    uses_self: &mut bool,
) -> ClassifiedArg {
    let none = ClassifiedArg {
        kind: ArgRole::Absorbed,
        signature: None,
        call: None,
        generic: None,
        registration: None,
        prelude: None,
        handler_prelude: None,
        once_signature: None,
        once_call: None,
        once_generic: None,
        resume: None,
        once_resume: None,
    };

    // wrapper pointers (incl. self)
    let pointee = if arg.is_single_mut_pointer() {
        arg.c_type.split(' ').last().unwrap_or("notfound")
    } else {
        "notfound"
    };
    if let Some(matching_wrapper) = wrappers.get(pointee) {
        let name = arg.as_ident();
        if !*uses_self && arg.c_type.ends_with(own_type_name) && matching_wrapper.type_name == own_type_name {
            *uses_self = true;
            return ClassifiedArg {
                kind: ArgRole::SelfPointer,
                call: Some(quote! { self.get_inner() }),
                ..none
            };
        }
        // These functions only borrow their output or image for this call.
        // Constructors and retaining setters still require an owning handle.
        let borrows_for_copy = fn_name.ends_with("_copy")
            && !matching_wrapper.opaque
            && !matching_wrapper.borrows_native_storage(wrappers);
        let arg_type = if ["_constants", "_buffers", "_values", "_image_retain", "_image_release"]
            .iter()
            .any(|suffix| fn_name.ends_with(suffix))
            || borrows_for_copy
        {
            let class = format_ident!("{}", matching_wrapper.class_name);
            let native = format_ident!("{}", matching_wrapper.type_name);
            quote! { &#class<impl ResourceStorage<#native>> }
        } else {
            ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, true)
        };
        return ClassifiedArg {
            kind: ArgRole::WrapperPointer,
            signature: if arg_type.is_empty() {
                None
            } else {
                Some(quote! { #name: #arg_type })
            },
            call: Some(quote! { #name.get_inner() }),
            ..none
        };
    }

    // handler arguments (the callback side; the clientd side is Absorbed below)
    if let ArgProcessing::Handler(handler_client) = &arg.processing {
        if !arg.is_mut_pointer() {
            return classify_handler_arg(
                arg,
                handler_client,
                wrappers,
                closure_handlers,
                owned_retained,
                fn_name,
                owner_is_shared(own_type_name),
                none,
            );
        }
        // clientd pointer: merged into the callback argument
        return none;
    }

    // plain arguments (strings, byte arrays, primitives, out-params) use the existing
    // ReturnType conversions already concentrate this knowledge
    let name = arg.as_ident();
    let rt = ReturnType::new(arg.clone(), wrappers.clone());
    let arg_type = rt.get_new_return_type(false, true);
    let call = rt.handle_rs_to_c_return(quote! { #name }, false);
    // Native netmask construction shifts by address_width - prefixlen.
    // Reject invalid widths before C can execute an out-of-range shift.
    let prefix_limit = match (fn_name, arg.name.as_str()) {
        ("aeron_ipv4_does_prefix_match" | "aeron_ipv4_netmask_from_prefixlen", "prefixlen") => Some(32usize),
        ("aeron_ipv6_does_prefix_match" | "aeron_ipv6_netmask_from_prefixlen", "prefixlen") => Some(128usize),
        _ => None,
    };
    ClassifiedArg {
        kind: ArgRole::Plain,
        signature: if arg_type.is_empty() {
            None
        } else {
            Some(quote! { #name: #arg_type })
        },
        call: if call.is_empty() { None } else { Some(quote! { #call }) },
        generic: rt.method_generics_for_where(false, false),
        prelude: prefix_limit.map(|limit| {
            quote! {
                assert!(#name <= #limit, "prefix length exceeds the address width");
            }
        }),
        ..none
    }
}

#[allow(clippy::too_many_arguments)]
fn classify_handler_arg(
    arg: &Arg,
    handler_client: &[Arg],
    wrappers: &BTreeMap<String, CWrapper>,
    closure_handlers: &[CHandler],
    owned_retained: &Option<Arg>,
    fn_name: &str,
    shared_owner: bool,
    none: ClassifiedArg,
) -> ClassifiedArg {
    let handler = handler_client.first().unwrap();
    let name = arg.as_ident();
    let raw_name = format_ident!("{}_raw", arg.name);
    let handler_type = handler.as_type();
    let new_type =
        parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&arg.c_type))).expect("Invalid class name");
    let sync = is_sync_handler_type(&handler.c_type);

    // How a panic in the handler gets back to Rust (see the trampolines in generator.rs):
    // - UNWIND_POLL_FNS: straight through C. A handler that caught a panic one level down
    //   (fragment assemblers) sets RESUMES_CAUGHT_PANIC so the poll raises it on return
    // - sync handlers: caught, then raised by this wrapper once C returns
    // - fragment handlers C keeps (assembler delegates): caught, then raised by the poll above
    // - anything else C keeps fires later, often on the conductor thread: logged
    let unwinds = UNWIND_POLL_FNS.contains(&fn_name);
    let fragment_handler = UNWIND_HANDLER_TYPES.contains(&handler.c_type.as_str());
    let (suffix, resume, once_resume) = if unwinds {
        (
            "_unwind",
            Some(quote! { if #new_type::RESUMES_CAUGHT_PANIC { resume_caught_panic(); } }),
            None,
        )
    } else if sync {
        (
            "_catch",
            Some(quote! { if #name.is_some() { resume_caught_panic(); } }),
            Some(quote! { resume_caught_panic(); }),
        )
    } else if fragment_handler {
        ("_catch", None, None)
    } else {
        ("", None, None)
    };
    let shim = format_ident!("{}_callback{}", handler.c_type, suffix);
    let once_shim = format_ident!("{}_callback_for_once_closure{}", handler.c_type, suffix);
    let none = ClassifiedArg {
        resume,
        once_resume,
        ..none
    };
    let is_owned = owned_retained.as_ref().map(|o| o.name == arg.name).unwrap_or(false);
    let local_callbacks = !is_owned && (sync || !shared_owner);
    let rt = ReturnType::new(arg.clone(), wrappers.clone()).with_local_callbacks(local_callbacks);
    let policy = if local_callbacks {
        let p = format_ident!("{}Policy", new_type.to_token_stream().to_string());
        quote! { #p }
    } else {
        quote! { SharedCallback }
    };

    if is_owned {
        let registration = if own_archive_callback_setter(fn_name) {
            quote! {
                if result >= 0 {
                    self.inner.as_owned().unwrap().replace_handler_dependency(#fn_name, #name.clone());
                }
            }
        } else {
            quote! {
                if result >= 0 {
                    if let Some(__handler) = &#name {
                        if let Some(__inner) = self.inner.as_owned() {
                            __inner.add_dependency(__handler.clone());
                        }
                    }
                }
            }
        };
        // owned setter: Option<T> in, prelude heap-allocates, registration keeps it alive
        return ClassifiedArg {
            kind: ArgRole::HandlerRetainedOwned,
            signature: Some(quote! { #name: Option<#new_type> }),
            call: Some(quote! {
                {
                    let callback: #handler_type = if #raw_name.is_null() {
                        None
                    } else {
                        Some(#shim::<#new_type, #policy>)
                    };
                    callback
                },
                #raw_name
            }),
            generic: rt.method_generics_for_where(false, shared_owner),
            registration: Some(registration),
            ..none
        };
    }

    // borrow style: Option<&Handler<T>>
    let borrow_call = quote! {
        {
            let callback: #handler_type = if #name.is_none() {
                None
            } else {
                Some(#shim::<#new_type, #policy>)
            };
            callback
        },
        #name.map(|m| m.as_raw()).unwrap_or_else(|| std::ptr::null_mut())
    };
    if sync {
        // Poll callbacks finish before C returns. Hold one checked mutable borrow
        // for the whole poll instead of checking it again for every fragment.
        let state = format_ident!("{}_state", arg.name);
        let direct_shim = format_ident!("{}_callback_for_borrowed_handler{}", handler.c_type, suffix);
        let (call, handler_prelude) = if unwinds
            && matches!(
                handler.c_type.as_str(),
                "aeron_fragment_handler_t" | "aeron_controlled_fragment_handler_t"
            ) {
            let callback = format_ident!("{}_callback", arg.name);
            let clientd = format_ident!("{}_clientd", arg.name);
            (
                quote! { #callback, #clientd },
                Some(quote! {
                    let mut #state = #name.map(Handler::lock);
                    let (#callback, #clientd): (#handler_type, *mut std::os::raw::c_void) =
                        if let Some(state) = #state.as_mut() {
                            if let Some(native) = state.native_poll_callback() {
                                (native.callback, native.clientd)
                            } else {
                                (Some(#direct_shim::<#new_type>), &mut **state as *mut #new_type as *mut _)
                            }
                        } else { (None, std::ptr::null_mut()) };
                }),
            )
        } else if unwinds {
            (
                quote! {
                    { let callback: #handler_type = if #state.is_some() {
                        Some(#direct_shim::<#new_type>)
                    } else { None }; callback },
                    #state.as_mut().map(|state| &mut **state as *mut #new_type as *mut std::os::raw::c_void)
                        .unwrap_or(std::ptr::null_mut())
                },
                Some(quote! { let mut #state = #name.map(Handler::lock); }),
            )
        } else {
            (borrow_call, None)
        };
        // the `_fn` variant swaps the Handler for a stack closure with the matching
        // FnMut signature (from the handler's CHandler definition)
        let fn_mut_sig = closure_handlers
            .iter()
            .find(|c| c.type_name == handler.c_type)
            .map(|c| c.fn_mut_signature.clone())
            .unwrap_or_else(|| quote! { FnMut() -> () });
        ClassifiedArg {
            kind: ArgRole::HandlerSync,
            signature: Some(quote! { #name: Option<&Handler<#new_type, #policy>> }),
            call: Some(call),
            // These native readers invoke the callback without a null check.
            // Publication suppliers are optional; native message polls already
            // reject null handlers. Neither path needs another Rust check.
            prelude: if matches!(
                handler.c_type.as_str(),
                "aeron_fragment_handler_t"
                    | "aeron_controlled_fragment_handler_t"
                    | "aeron_block_handler_t"
                    | "aeron_reserved_value_supplier_t"
            ) {
                None
            } else {
                Some(quote! { assert!(#name.is_some(), "this native reader requires a callback"); })
            },
            handler_prelude,
            generic: rt.method_generics_for_where(false, shared_owner),
            once_signature: Some(quote! { mut #name: #new_type }),
            once_call: Some(quote! {
                Some(#once_shim::<#new_type>),
                &mut #name as *mut _ as *mut std::os::raw::c_void
            }),
            once_generic: Some(quote! { #new_type: #fn_mut_sig }),
            ..none
        }
    } else {
        ClassifiedArg {
            kind: ArgRole::HandlerRetainedBorrowed,
            signature: Some(quote! { #name: Option<&Handler<#new_type, #policy>> }),
            call: Some(borrow_call),
            // registered as a dependency, so always 'static
            generic: rt.method_generics_for_where(true, shared_owner),
            registration: Some(quote! {
                if let Some(__handler) = #name {
                    if let Some(__inner) = self.inner.as_owned() {
                        __inner.add_dependency(__handler.clone());
                    }
                }
            }),
            ..none
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arg(name: &str, c_type: &str, processing: ArgProcessing) -> Arg {
        Arg {
            name: name.to_string(),
            c_type: c_type.to_string(),
            processing,
        }
    }

    fn handler_pair(name: &str, c_type: &str) -> Arg {
        let cb = arg(name, c_type, ArgProcessing::Default);
        let clientd = arg("clientd", "* mut :: std :: os :: raw :: c_void", ArgProcessing::Default);
        arg(name, c_type, ArgProcessing::Handler(vec![cb, clientd]))
    }

    fn method(name: &str, ret: &str, args: Vec<Arg>) -> Method {
        Method {
            fn_name: format!("aeron_x_{name}"),
            struct_method_name: name.to_string(),
            return_type: arg("", ret, ArgProcessing::Default),
            arguments: args,
            docs: Default::default(),
        }
    }

    #[test]
    fn copy_uses_the_destination_as_self_and_keeps_the_source_argument() {
        let wrapper = CWrapper {
            type_name: "aeron_value_t".into(),
            class_name: "AeronValue".into(),
            ..Default::default()
        };
        let mut wrappers = BTreeMap::from([("aeron_value_t".into(), wrapper)]);
        let method = method(
            "copy",
            ":: std :: os :: raw :: c_int",
            vec![
                arg("dst", "* mut aeron_value_t", ArgProcessing::Default),
                arg("src", "* mut aeron_value_t", ArgProcessing::Default),
            ],
        );
        let classified = classify_method_args(&method, "aeron_value_t", &wrappers, &[]);
        assert_eq!(classified.args[0].kind, ArgRole::SelfPointer);
        assert_eq!(classified.args[1].kind, ArgRole::WrapperPointer);
        assert_eq!(
            classified.calls().iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["self . get_inner ()", "src . get_inner ()",]
        );
        assert_eq!(
            classified.signatures()[0].to_string(),
            quote! { src: &AeronValue<impl ResourceStorage<aeron_value_t>> }.to_string()
        );
        assert!(classified.registrations().is_empty());
        assert!(classified.preludes().is_empty());
        // Copying pointers does not make their targets independent of the source.
        wrappers
            .get_mut("aeron_value_t")
            .unwrap()
            .fields
            .push(arg("data", "* mut u8", ArgProcessing::Default));
        let classified = classify_method_args(&method, "aeron_value_t", &wrappers, &[]);
        assert_eq!(
            classified.signatures()[0].to_string(),
            quote! { src: &AeronValue }.to_string()
        );
    }

    #[test]
    fn native_prefix_operations_check_their_address_width() {
        for (native, limit) in [
            ("aeron_ipv4_does_prefix_match", 32usize),
            ("aeron_ipv4_netmask_from_prefixlen", 32),
            ("aeron_ipv6_does_prefix_match", 128),
            ("aeron_ipv6_netmask_from_prefixlen", 128),
        ] {
            let mut method = method(
                "prefix",
                "bool",
                vec![arg("prefixlen", "usize", ArgProcessing::Default)],
            );
            method.fn_name = native.into();
            let classified = classify_method_args(&method, "aeron_t", &BTreeMap::new(), &[]);
            let expected = quote! { assert!(prefixlen <= #limit, "prefix length exceeds the address width"); };
            assert_eq!(classified.preludes()[0].to_string(), expected.to_string());
        }
        let method = method(
            "unrelated",
            "bool",
            vec![arg("prefixlen", "usize", ArgProcessing::Default)],
        );
        assert!(
            classify_method_args(&method, "aeron_t", &BTreeMap::new(), &[])
                .preludes()
                .is_empty()
        );
    }

    #[test]
    fn retained_handler_on_int_setter_is_owned() {
        let m = method(
            "set_error_handler",
            ":: std :: os :: raw :: c_int",
            vec![
                arg("ctx", "* mut aeron_context_t", ArgProcessing::Default),
                handler_pair("handler", "aeron_error_handler_t"),
            ],
        );
        let mut wrappers = BTreeMap::new();
        wrappers.insert(
            "aeron_context_t".to_string(),
            CWrapper {
                type_name: "aeron_context_t".to_string(),
                class_name: "AeronContext".to_string(),
                ..Default::default()
            },
        );
        let classified = classify_method_args(&m, "aeron_context_t", &wrappers, &[]);
        assert!(classified.uses_self);
        assert!(
            classified.owned_retained.is_some(),
            "error handler must be an owned setter"
        );
        assert!(
            !classified.once_capable,
            "retained callbacks must not get a _fn variant"
        );
        assert_eq!(classified.args[1].kind, ArgRole::HandlerRetainedOwned);
        assert!(classified.args[1].registration.is_some());
    }

    #[test]
    fn sync_handler_is_once_capable_and_not_registered() {
        let m = method(
            "poll",
            ":: std :: os :: raw :: c_int",
            vec![
                arg("subscription", "* mut aeron_subscription_t", ArgProcessing::Default),
                handler_pair("handler", "aeron_fragment_handler_t"),
            ],
        );
        let mut wrappers = BTreeMap::new();
        wrappers.insert(
            "aeron_subscription_t".to_string(),
            CWrapper {
                type_name: "aeron_subscription_t".to_string(),
                class_name: "AeronSubscription".to_string(),
                ..Default::default()
            },
        );
        let classified = classify_method_args(&m, "aeron_subscription_t", &wrappers, &[]);
        assert!(classified.once_capable, "sync callbacks get a _fn variant");
        assert!(classified.owned_retained.is_none());
        assert_eq!(classified.args[1].kind, ArgRole::HandlerSync);
        assert!(
            classified.args[1].registration.is_none(),
            "sync handlers are never retained"
        );
        assert!(classified.args[1].once_call.is_some());
    }

    #[test]
    fn clientd_and_self_absorb_correctly() {
        let m = method(
            "poll",
            ":: std :: os :: raw :: c_int",
            vec![arg(
                "subscription",
                "* mut aeron_subscription_t",
                ArgProcessing::Default,
            )],
        );
        let mut wrappers = BTreeMap::new();
        wrappers.insert(
            "aeron_subscription_t".to_string(),
            CWrapper {
                type_name: "aeron_subscription_t".to_string(),
                class_name: "AeronSubscription".to_string(),
                ..Default::default()
            },
        );
        let classified = classify_method_args(&m, "aeron_subscription_t", &wrappers, &[]);
        assert_eq!(classified.args[0].kind, ArgRole::SelfPointer);
        assert!(classified.args[0].signature.is_none());
        assert_eq!(classified.signatures().len(), 0);
        assert_eq!(classified.calls().len(), 1);
    }

    /// Every identifier in `tokens`, including inside groups
    fn idents(tokens: TokenStream) -> Vec<String> {
        tokens
            .into_iter()
            .flat_map(|tree| match tree {
                proc_macro2::TokenTree::Ident(ident) => vec![ident.to_string()],
                proc_macro2::TokenTree::Group(group) => idents(group.stream()),
                _ => vec![],
            })
            .collect()
    }

    /// Across the real bindings, only UNWIND_POLL_FNS get `_unwind` trampolines
    #[test]
    #[cfg(not(target_os = "windows"))]
    fn unwind_trampolines_only_on_unwind_poll_fns() {
        let mut unwinding = std::collections::BTreeSet::new();
        let mut calls_by_fn = BTreeMap::new();
        let mut registrations_by_fn = BTreeMap::new();
        let mut preludes_by_fn = BTreeMap::new();
        for file in ["client.rs", "archive.rs"] {
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("bindings")
                .join(file);
            let bindings = crate::parse_bindings(&path);
            for wrapper in bindings.wrappers.values() {
                for method in &wrapper.methods {
                    let classified =
                        classify_method_args(method, &wrapper.type_name, &bindings.wrappers, &bindings.handlers);
                    for arg in &classified.args {
                        let (call, once_call) = (&arg.call, &arg.once_call);
                        let shims: Vec<String> = idents(quote! { #call #once_call })
                            .into_iter()
                            // shims are `{c_type}_callback...` and every c_type ends in `_t`
                            .filter(|t| t.contains("_t_callback"))
                            .collect();
                        for shim in &shims {
                            // a caught panic needs a resume point, else it waits in the slot forever:
                            // this wrapper, or the poll above for fragment handlers C keeps
                            if let Some(c_type) = shim.strip_suffix("_callback_catch") {
                                assert!(
                                    arg.resume.is_some() || UNWIND_HANDLER_TYPES.contains(&c_type),
                                    "{}: {shim} has no resume point",
                                    method.fn_name
                                );
                            }
                            // a sync handler runs on the caller's thread, so its panic must reach
                            // the caller, never only the log
                            if arg.kind == ArgRole::HandlerSync {
                                assert!(
                                    shim.ends_with("_catch") || shim.ends_with("_unwind"),
                                    "{}: sync handler uses logging {shim}",
                                    method.fn_name
                                );
                            }
                        }
                    }
                    let (calls, once_calls) = (classified.calls(), classified.once_calls());
                    let tokens = idents(quote! { #(#calls)* #(#once_calls)* });
                    if tokens.iter().any(|t| t.ends_with("_unwind")) {
                        unwinding.insert(method.fn_name.clone());
                    }
                    calls_by_fn.insert(method.fn_name.clone(), tokens);
                    let registrations = classified.registrations();
                    registrations_by_fn.insert(method.fn_name.clone(), quote! { #(#registrations)* }.to_string());
                    let preludes = classified.preludes();
                    preludes_by_fn.insert(method.fn_name.clone(), quote! { #(#preludes)* }.to_string());
                }
            }
        }

        let expected: std::collections::BTreeSet<String> = UNWIND_POLL_FNS.iter().map(|s| s.to_string()).collect();
        assert_eq!(unwinding, expected);

        for (fn_name, shim) in [
            // sync handlers: caught, raised by this wrapper
            (
                "aeron_publication_offer",
                "aeron_reserved_value_supplier_t_callback_catch",
            ),
            (
                "aeron_archive_persistent_subscription_poll",
                "aeron_fragment_handler_t_callback_catch",
            ),
            // retained by C, fired later: logged
            ("aeron_context_set_error_handler", "aeron_error_handler_t_callback"),
        ] {
            let tokens = &calls_by_fn[fn_name];
            assert!(
                tokens.iter().any(|t| t == shim),
                "{fn_name} must use {shim}: {tokens:?}"
            );
        }

        // set_aeron keeps the client alive once C accepts it, unless the context is borrowed
        assert_eq!(
            registrations_by_fn["aeron_archive_context_set_aeron"],
            quote! {
                if result >= 0 {
                    if let Some(__inner) = self.inner.as_owned() {
                        __inner.add_handle_dependency(aeron);
                    }
                }
            }
            .to_string()
        );
        // a borrowed context cannot keep the client, so set_aeron refuses before calling C
        let guard = &preludes_by_fn["aeron_archive_context_set_aeron"];
        assert!(
            guard.contains("as_owned") && guard.contains("owning heap storage"),
            "{guard}"
        );
        // C frees the counter, so the context must not keep it, nor refuse a borrowed self
        for by_fn in [&registrations_by_fn, &preludes_by_fn] {
            assert_eq!(
                by_fn["aeron_archive_persistent_subscription_context_set_state_counter"],
                ""
            );
        }
    }

    /// UNWIND_HANDLER_TYPES must be exactly the handler types UNWIND_POLL_FNS take.
    /// Fails if a new Aeron version changes a poll signature
    #[test]
    #[cfg(not(target_os = "windows"))]
    fn unwind_handler_types_match_unwind_poll_fns() {
        let mut taken = std::collections::BTreeSet::new();
        for file in ["client.rs", "archive.rs"] {
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("bindings")
                .join(file);
            let bindings = crate::parse_bindings(&path);
            for method in bindings.wrappers.values().flat_map(|w| &w.methods) {
                if !UNWIND_POLL_FNS.contains(&method.fn_name.as_str()) {
                    continue;
                }
                for arg in &method.arguments {
                    if let ArgProcessing::Handler(handler) = &arg.processing {
                        taken.insert(handler[0].c_type.clone());
                    }
                }
            }
        }
        let declared: std::collections::BTreeSet<String> = UNWIND_HANDLER_TYPES.iter().map(|s| s.to_string()).collect();
        assert_eq!(taken, declared);
    }
}
