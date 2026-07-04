//! Argument classification for generated wrapper methods.
//!
//! `plan_method` classifies each C argument once into a [`PlannedArg`] carrying all token
//! fragments the emitters need (signature, FFI call, generics, retained-handler registration,
//! and the `_once` stack-closure variant). `generate_methods` then emits purely from the plan.

use crate::generator::{is_sync_handler_type, Arg, ArgProcessing, CHandler, CWrapper, Method, ReturnType};
use crate::snake_to_pascal_case;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::BTreeMap;
use syn::{parse_str, Type};

/// Role of a C argument in the generated Rust wrapper method.
///
/// Each argument is classified once: is it `&self`? A `&OtherWrapper` param? A
/// callback that must be retained? etc. The classification drives all emission
/// (signature, FFI call, handler registration, and the `_once` variant).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedArgKind {
    /// `*mut <own type>` — becomes `&self` / `self.get_inner()`.
    SelfPointer,
    /// `*mut <other wrapper>` — `&OtherWrapper` param, `name.get_inner()` call.
    WrapperPointer,
    /// Callback invoked only during the FFI call — `Option<&Handler<T>>`, `_once` capable.
    HandlerSync,
    /// Callback the C client retains — borrow style (`Option<&Handler<T>>`), registered
    /// as a dependency of `self`.
    HandlerRetainedBorrowed,
    /// Callback the C client retains — owned setter style (`Option<T>`), heap-allocated in
    /// a prelude, registered, and returned to the caller.
    HandlerRetainedOwned,
    /// clientd / length / merged arguments that emit nothing of their own.
    Absorbed,
    /// Everything else — handled by the existing `ReturnType` conversions.
    Plain,
}

/// One argument, classified once, with every emission fragment precomputed.
pub struct PlannedArg {
    pub kind: PlannedArgKind,
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
    /// `_once` variant fragments (sync handlers swap Handler for a stack closure).
    pub once_signature: Option<TokenStream>,
    pub once_call: Option<TokenStream>,
    pub once_generic: Option<TokenStream>,
}

pub struct MethodPlan {
    pub uses_self: bool,
    pub args: Vec<PlannedArg>,
    /// The owned retained-handler argument, when this method is an owned setter.
    pub owned_retained: Option<Arg>,
    /// Every handler argument is sync — a `_once` stack-closure variant is sound.
    pub once_capable: bool,
}

impl MethodPlan {
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

    /// Statements emitted before the FFI call (e.g. `IntoCStr` shadows).
    pub fn preludes(&self) -> Vec<TokenStream> {
        self.args.iter().filter_map(|a| a.prelude.clone()).collect()
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
pub fn plan_method(
    method: &Method,
    own_type_name: &str,
    wrappers: &BTreeMap<String, CWrapper>,
    closure_handlers: &[CHandler],
) -> MethodPlan {
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
    let args = method
        .arguments
        .iter()
        .map(|arg| {
            plan_arg(
                arg,
                own_type_name,
                wrappers,
                closure_handlers,
                &owned_retained,
                &mut uses_self,
            )
        })
        .collect();

    MethodPlan {
        uses_self,
        args,
        owned_retained,
        once_capable,
    }
}

fn plan_arg(
    arg: &Arg,
    own_type_name: &str,
    wrappers: &BTreeMap<String, CWrapper>,
    closure_handlers: &[CHandler],
    owned_retained: &Option<Arg>,
    uses_self: &mut bool,
) -> PlannedArg {
    let none = PlannedArg {
        kind: PlannedArgKind::Absorbed,
        signature: None,
        call: None,
        generic: None,
        registration: None,
        prelude: None,
        once_signature: None,
        once_call: None,
        once_generic: None,
    };

    // wrapper pointers (incl. self)
    let pointee = if arg.is_single_mut_pointer() {
        arg.c_type.split(' ').last().unwrap_or("notfound")
    } else {
        "notfound"
    };
    if let Some(matching_wrapper) = wrappers.get(pointee) {
        let name = arg.as_ident();
        if arg.c_type.ends_with(own_type_name) && matching_wrapper.type_name == own_type_name {
            *uses_self = true;
            return PlannedArg {
                kind: PlannedArgKind::SelfPointer,
                call: Some(quote! { self.get_inner() }),
                ..none
            };
        }
        let arg_type = ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, true);
        return PlannedArg {
            kind: PlannedArgKind::WrapperPointer,
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
            return plan_handler_arg(arg, handler_client, wrappers, closure_handlers, owned_retained, none);
        }
        // clientd pointer: merged into the callback argument
        return none;
    }

    // plain arguments (strings, byte arrays, primitives, out-params) — the existing
    // ReturnType conversions already concentrate this knowledge
    let name = arg.as_ident();
    let rt = ReturnType::new(arg.clone(), wrappers.clone());
    let arg_type = rt.get_new_return_type(false, true);
    let call = rt.handle_rs_to_c_return(quote! { #name }, false);
    PlannedArg {
        kind: PlannedArgKind::Plain,
        signature: if arg_type.is_empty() {
            None
        } else {
            Some(quote! { #name: #arg_type })
        },
        call: if call.is_empty() { None } else { Some(quote! { #call }) },
        generic: rt.method_generics_for_where(false),
        ..none
    }
}

fn plan_handler_arg(
    arg: &Arg,
    handler_client: &[Arg],
    wrappers: &BTreeMap<String, CWrapper>,
    closure_handlers: &[CHandler],
    owned_retained: &Option<Arg>,
    none: PlannedArg,
) -> PlannedArg {
    let handler = handler_client.first().unwrap();
    let name = arg.as_ident();
    let raw_name = format_ident!("{}_raw", arg.name);
    let shim = format_ident!("{}_callback", handler.c_type);
    let once_shim = format_ident!("{}_callback_for_once_closure", handler.c_type);
    let handler_type = handler.as_type();
    let new_type =
        parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&arg.c_type))).expect("Invalid class name");
    let sync = is_sync_handler_type(&handler.c_type);
    let is_owned = owned_retained.as_ref().map(|o| o.name == arg.name).unwrap_or(false);
    let rt = ReturnType::new(arg.clone(), wrappers.clone());

    if is_owned {
        // owned setter: Option<T> in, prelude heap-allocates, registration keeps it alive
        return PlannedArg {
            kind: PlannedArgKind::HandlerRetainedOwned,
            signature: Some(quote! { #name: Option<#new_type> }),
            call: Some(quote! {
                {
                    let callback: #handler_type = if #raw_name.is_null() {
                        None
                    } else {
                        Some(#shim::<#new_type>)
                    };
                    callback
                },
                #raw_name
            }),
            generic: rt.method_generics_for_where(false),
            registration: Some(quote! {
                if let Some(__handler) = &#name {
                    if let Some(__inner) = self.inner.as_owned() {
                        __inner.add_dependency(__handler.clone());
                    }
                }
            }),
            ..none
        };
    }

    // borrow style: Option<&Handler<T>>
    let borrow_call = quote! {
        {
            let callback: #handler_type = if #name.is_none() {
                None
            } else {
                Some(#shim::<#new_type>)
            };
            callback
        },
        #name.map(|m| m.as_raw()).unwrap_or_else(|| std::ptr::null_mut())
    };
    if sync {
        // the `_once` variant swaps the Handler for a stack closure with the matching
        // FnMut signature (from the handler's CHandler definition)
        let fn_mut_sig = closure_handlers
            .iter()
            .find(|c| c.type_name == handler.c_type)
            .map(|c| c.fn_mut_signature.clone())
            .unwrap_or_else(|| quote! { FnMut() -> () });
        PlannedArg {
            kind: PlannedArgKind::HandlerSync,
            signature: Some(quote! { #name: Option<&Handler<#new_type>> }),
            call: Some(borrow_call),
            generic: rt.method_generics_for_where(false),
            once_signature: Some(quote! { mut #name: #new_type }),
            once_call: Some(quote! {
                Some(#once_shim::<#new_type>),
                &mut #name as *mut _ as *mut std::os::raw::c_void
            }),
            once_generic: Some(quote! { #new_type: #fn_mut_sig }),
            ..none
        }
    } else {
        PlannedArg {
            kind: PlannedArgKind::HandlerRetainedBorrowed,
            signature: Some(quote! { #name: Option<&Handler<#new_type>> }),
            call: Some(borrow_call),
            generic: rt.method_generics_for_where(false),
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
        let plan = plan_method(&m, "aeron_context_t", &wrappers, &[]);
        assert!(plan.uses_self);
        assert!(plan.owned_retained.is_some(), "error handler must be an owned setter");
        assert!(!plan.once_capable, "retained callbacks must not get a _once variant");
        assert_eq!(plan.args[1].kind, PlannedArgKind::HandlerRetainedOwned);
        assert!(plan.args[1].registration.is_some());
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
        let plan = plan_method(&m, "aeron_subscription_t", &wrappers, &[]);
        assert!(plan.once_capable, "sync callbacks get a _once variant");
        assert!(plan.owned_retained.is_none());
        assert_eq!(plan.args[1].kind, PlannedArgKind::HandlerSync);
        assert!(plan.args[1].registration.is_none(), "sync handlers are never retained");
        assert!(plan.args[1].once_call.is_some());
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
        let plan = plan_method(&m, "aeron_subscription_t", &wrappers, &[]);
        assert_eq!(plan.args[0].kind, PlannedArgKind::SelfPointer);
        assert!(plan.args[0].signature.is_none());
        assert_eq!(plan.signatures().len(), 0);
        assert_eq!(plan.calls().len(), 1);
    }
}
