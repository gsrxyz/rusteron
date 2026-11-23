use crate::get_possible_wrappers;
#[allow(unused_imports)]
use crate::snake_to_pascal_case;
use crate::thread_safety::{
    handle_has_stable_pointer, handle_is_archive_context, handle_is_consumed_by_poll, handle_is_send_only,
    handle_is_shared, handle_is_single_owner, owner_is_shared,
};
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use quote::{ToTokens, format_ident, quote};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use std::str::FromStr;
use syn::{ImplItem, Item, Type, parse_str};

pub const COMMON_CODE: &str = include_str!("common.rs");
pub const CLIENT_BINDINGS: &str = include_str!("../bindings/client.rs");
pub const ARCHIVE_BINDINGS: &str = include_str!("../bindings/archive.rs");
pub const MEDIA_DRIVER_BINDINGS: &str = include_str!("../bindings/media-driver.rs");

#[derive(Debug, Clone, Default)]
pub struct CBinding {
    pub constants: Vec<Arg>,
    pub enums: Vec<NativeEnum>,
    pub wrappers: BTreeMap<String, CWrapper>,
    pub methods: Vec<Method>,
    pub handlers: Vec<CHandler>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NativeEnum {
    pub name: String,
    pub repr: String,
    pub variants: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Method {
    pub fn_name: String,
    pub struct_method_name: String,
    pub return_type: Arg,
    pub arguments: Vec<Arg>,
    pub docs: BTreeSet<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArgProcessing {
    Handler(Vec<Arg>),
    StringWithLength(Vec<Arg>),
    ByteArrayWithLength(Vec<Arg>),
    Default,
}

/// Callbacks Aeron invokes only during the FFI call (e.g. `poll`'s fragment handler).
/// The generator emits a `_fn` stack-closure variant for these.
///
/// Callbacks NOT listed are retained by Aeron and invoked later (conductor thread);
/// they need a heap `Handler` registered as a dependency, and get no `_fn` variant.
const SYNC_HANDLER_TYPES: &[&str] = &[
    "aeron_fragment_handler_t",
    "aeron_controlled_fragment_handler_t",
    "aeron_block_handler_t",
    "aeron_error_log_reader_func_t",
    "aeron_loss_reporter_read_entry_func_t",
    "aeron_uri_parse_callback_t",
    "aeron_reserved_value_supplier_t",
    "aeron_counters_reader_foreach_counter_func_t",
    "aeron_counters_reader_foreach_metadata_func_t",
    "aeron_str_to_ptr_hash_map_for_each_func_t",
    "aeron_rb_handler_t",
    "aeron_rb_controlled_handler_t",
    "aeron_queue_drain_func_t",
    "aeron_term_gap_scanner_on_gap_detected_func_t",
    "aeron_archive_recording_descriptor_consumer_func_t",
    "aeron_archive_recording_subscription_descriptor_consumer_func_t",
];

/// Handler types declared `extern "C-unwind"`: the handler types [`UNWIND_POLL_FNS`] take.
/// Needed before parsing, as bindgen sets the ABI; a test checks it matches the bindings
pub const UNWIND_HANDLER_TYPES: &[&str] = &[
    "aeron_fragment_handler_t",
    "aeron_controlled_fragment_handler_t",
    "aeron_block_handler_t",
];

/// C functions a handler panic may pass through, checked by reading the Aeron C source:
/// they save the position only after the handler returns, so the next poll redelivers from
/// the last saved position. Not derivable from the headers. Any other C function gets
/// trampolines that catch the panic. A missing entry is safe but adds a catch
pub const UNWIND_POLL_FNS: &[&str] = &[
    "aeron_subscription_poll",
    "aeron_subscription_controlled_poll",
    "aeron_subscription_block_poll",
    "aeron_image_poll",
    "aeron_image_controlled_poll",
    "aeron_image_bounded_poll",
    "aeron_image_bounded_controlled_poll",
    "aeron_image_block_poll",
    "aeron_archive_replay_merge_poll",
];

/// Suffixes of native lifecycle calls (destroy, free, close). The owning handle already makes
/// these calls, so a generated raw form bypasses Rust ownership and is `unsafe`
const RAW_LIFECYCLE_SUFFIXES: &[&str] = &["_delete", "_destroy", "_free", "_unmap", "_close"];

/// Lifecycle calls the suffixes miss. Each has a safe form that tracks ownership, so the raw
/// form is `unsafe`
const RAW_LIFECYCLE_FNS: &[&str] = &[
    // the owned `AeronImage` retains on creation and releases on drop
    "aeron_subscription_image_retain",
    "aeron_subscription_image_release",
    // `revoke_with_handler` checks for the last owning handle first
    "aeron_exclusive_publication_revoke",
    // the safe setter keeps the listener alive as long as the context
    "aeron_archive_persistent_subscription_context_set_listener",
];

/// Audited calls that do not release their receiver despite their suffix.
///
/// The driver entries write conductor state without a lock. They stay safe because the only
/// safe route to a driver's conductor is `AeronDriver::conductor()`, and starting the driver
/// consumes that handle, so no agent thread runs concurrently
const NOT_RAW_LIFECYCLE_FNS: &[&str] = &[
    "aeron_exclusive_publication_revoke_on_close",
    // Reclaims a bounds-checked counter slot, not the manager allocation.
    "aeron_counters_manager_free",
    // Marks one client closed and clears its heartbeat, without freeing either handle.
    "aeron_driver_conductor_on_client_close",
];

/// These native implementations never access or retain any pointer argument.
/// Pointer validity therefore adds no caller obligation. Stateful idle functions
/// are absent because they read or write their state.
const IGNORED_NATIVE_POINTERS: &[&str] = &[
    "aeron_idle_strategy_yielding_idle",
    "aeron_idle_strategy_busy_spinning_idle",
    "aeron_idle_strategy_noop_idle",
    "aeron_driver_termination_validator_default_allow",
    "aeron_driver_termination_validator_default_deny",
];

/// Native getters whose output struct holds pointers into the queried handle. The lifetime-bound
/// `get_*` forms are safe, the raw forms are `unsafe`
const POINTER_OUTPUT_METHODS: &[&str] = &["constants", "buffers", "values", "get_buffers"];

/// `(struct, field)` getters hand-written in aeron_custom.rs, so the generator must not emit them
const CUSTOM_FIELD_GETTERS: &[(&str, &str)] = &[("aeron_buffer_claim_t", "data")];

/// Why a generated method is `unsafe`, apart from returning unchecked text. Each reason adds
/// its own `# Safety` section, so a new reason needs both a flag and a doc
struct MethodUnsafety {
    raw_pointer_args: bool,
    raw_callback_args: bool,
    /// A setter passes a value C frees later (see [`crate::arg_classifier::is_c_owned_set_type`])
    c_takes_ownership: bool,
    output_has_pointers: bool,
    raw_lifecycle: bool,
}

impl MethodUnsafety {
    fn new(
        type_name: &str,
        method: &Method,
        wrappers: &BTreeMap<String, CWrapper>,
        fn_arguments: &[TokenStream],
        callback_aliases: &BTreeSet<String>,
    ) -> Self {
        let fn_name = method.fn_name.as_str();
        let raw_lifecycle = RAW_LIFECYCLE_FNS.contains(&fn_name)
            || (RAW_LIFECYCLE_SUFFIXES.iter().any(|suffix| fn_name.ends_with(suffix))
                && !NOT_RAW_LIFECYCLE_FNS.contains(&fn_name))
            || fn_name.starts_with("aeron_async_remove_")
            // `aeron_async_add_*_cancel` on the client; the async handle's own `cancel` is safe
            || (type_name == "aeron_t" && fn_name.ends_with("_cancel"))
            // `*_init` with arguments re-initialises live native state; the plain `new` is safe
            || (fn_name.ends_with("_init") && method.arguments.len() > 1);
        let output_has_pointers = POINTER_OUTPUT_METHODS.contains(&method.struct_method_name.as_str())
            && method
                .arguments
                .get(1)
                .and_then(|arg| wrappers.get(arg.base_type()))
                .is_some_and(|w| w.borrows_native_storage(wrappers));
        let c_takes_ownership = fn_name.contains("_set_")
            && method
                .arguments
                .iter()
                .skip(1)
                .any(|arg| crate::arg_classifier::is_c_owned_set_type(&arg.c_type));
        let raw_pointer_args = !IGNORED_NATIVE_POINTERS.contains(&fn_name)
            && fn_arguments.iter().any(|arg| {
                matches!(syn::parse2::<syn::FnArg>(arg.clone()),
                Ok(syn::FnArg::Typed(arg)) if matches!(*arg.ty, Type::Ptr(_)))
            });
        // Driver-context suppliers expose raw callbacks without clientd arguments.
        let raw_callback_args = type_name == "aeron_driver_context_t"
            && fn_arguments.iter().any(|arg| {
                matches!(syn::parse2::<syn::FnArg>(arg.clone()),
                    Ok(syn::FnArg::Typed(arg)) if matches!(*arg.ty, Type::Path(ref path)
                        if path.path.segments.last().is_some_and(|segment|
                            callback_aliases.contains(&segment.ident.to_string()))))
            });
        Self {
            raw_pointer_args,
            raw_callback_args,
            c_takes_ownership,
            output_has_pointers,
            raw_lifecycle,
        }
    }

    fn any(&self) -> bool {
        self.raw_pointer_args
            || self.raw_callback_args
            || self.c_takes_ownership
            || self.output_has_pointers
            || self.raw_lifecycle
    }

    fn docs(&self) -> Vec<TokenStream> {
        let reasons = [
            (
                self.raw_callback_args,
                "The callback must satisfy the native ABI, ownership, lifetime and threading contracts for every invocation. It must initialize all required outputs and must not unwind through C. Prefer a typed strategy setter when available.",
            ),
            (
                self.raw_pointer_args,
                "Raw pointer arguments must have the size, alignment, initialization and lifetime required by the native function. Writable pointers require exclusive access. Keep any retained pointers valid until native cleanup.",
            ),
            (
                self.c_takes_ownership,
                "C takes ownership of this value and frees it during native cleanup. Prevent further use or cleanup through Rust aliases, keep a counter's client alive, and release any replaced value exactly once. Do not replace it while a borrowed view is in use.",
            ),
            (
                self.output_has_pointers,
                "The output may contain borrowed native pointers. Do not use those pointers after the queried handle is dropped or changed. Prefer the lifetime-bound `get_constants`, `get_buffers` or `get_values` method.",
            ),
            (
                self.raw_lifecycle,
                "This bypasses Rust ownership. Keep all retained pointers valid until native cleanup. Initializers require exclusive access and must not overwrite live native state or invalidate borrowed views. After raw removal or deletion, prevent all further access and Rust cleanup through every alias. Prefer the owning handle's constructor, close or cancel method.",
            ),
        ];
        reasons
            .into_iter()
            .filter(|(applies, _)| *applies)
            .flat_map(|(_, text)| [quote! { #[doc = "# Safety"] }, quote! { #[doc = #text] }])
            .collect()
    }
}

pub fn is_sync_handler_type(c_type: &str) -> bool {
    SYNC_HANDLER_TYPES.iter().any(|t| c_type.contains(t))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Arg {
    pub name: String,
    pub c_type: String,
    pub processing: ArgProcessing,
}

impl Arg {
    pub fn is_primitive(&self) -> bool {
        static PRIMITIVE_TYPES: &[&str] = &[
            "i64", "u64", "f32", "f64", "i32", "i16", "u32", "u16", "bool", "usize", "isize", "c_int",
        ];
        PRIMITIVE_TYPES.iter().any(|&f| self.c_type.ends_with(f))
    }
}

impl Arg {
    const C_INT_RETURN_TYPE_STR: &'static str = ":: std :: os :: raw :: c_int";
    const C_CHAR_STR: &'static str = "* const :: std :: os :: raw :: c_char";
    const C_MUT_CHAR_STR: &'static str = "* mut :: std :: os :: raw :: c_char";
    const C_BYTE_ARRAY: &'static str = "* const u8";
    const C_BYTE_MUT_ARRAY: &'static str = "* mut u8";
    const STAR_MUT: &'static str = "* mut";
    const DOUBLE_STAR_MUT: &'static str = "* mut * mut";
    const C_VOID: &'static str = "* mut :: std :: os :: raw :: c_void";

    pub fn is_any_pointer(&self) -> bool {
        self.c_type.starts_with("* const") || self.c_type.starts_with("* mut")
    }

    pub fn is_c_string(&self) -> bool {
        self.c_type == Self::C_CHAR_STR
    }

    pub fn is_c_string_any(&self) -> bool {
        self.is_c_string() || self.is_mut_c_string()
    }

    pub fn is_mut_c_string(&self) -> bool {
        self.c_type == Self::C_MUT_CHAR_STR
    }

    pub fn is_usize(&self) -> bool {
        self.c_type == "usize"
    }

    pub fn is_byte_array(&self) -> bool {
        self.c_type == Self::C_BYTE_ARRAY || self.c_type == Self::C_BYTE_MUT_ARRAY
    }

    pub fn is_mut_byte_array(&self) -> bool {
        self.c_type == Self::C_BYTE_MUT_ARRAY
    }

    pub fn is_c_raw_int(&self) -> bool {
        self.c_type == Self::C_INT_RETURN_TYPE_STR
    }

    pub fn is_mut_pointer(&self) -> bool {
        self.c_type.starts_with(Self::STAR_MUT)
    }

    pub fn is_double_mut_pointer(&self) -> bool {
        self.c_type.starts_with(Self::DOUBLE_STAR_MUT)
    }

    pub fn is_single_mut_pointer(&self) -> bool {
        self.is_mut_pointer() && !self.is_double_mut_pointer()
    }

    pub fn is_c_void(&self) -> bool {
        self.c_type == Self::C_VOID
    }

    /// The type without pointers or qualifiers, e.g. `aeron_header_t` for `* mut aeron_header_t`
    pub fn base_type(&self) -> &str {
        self.c_type.split_whitespace().last().unwrap_or(&self.c_type)
    }
}

impl Deref for Arg {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.c_type
    }
}

impl Arg {
    pub fn as_ident(&self) -> Ident {
        Ident::new(&self.name, proc_macro2::Span::call_site())
    }

    pub fn as_type(&self) -> Type {
        parse_str(&self.c_type).expect("Invalid argument type")
    }
}

#[derive(Debug, Clone)]
pub struct CHandler {
    pub type_name: String,
    pub args: Vec<Arg>,
    pub return_type: Arg,
    pub docs: BTreeSet<String>,
    pub fn_mut_signature: TokenStream,
    pub closure_type_name: TokenStream,
}

#[derive(Debug, Clone)]
pub struct ReturnType {
    original: Arg,
    local_callbacks: bool,
    owned_strings: bool,
    wrappers: BTreeMap<String, CWrapper>,
}

impl ReturnType {
    pub fn new(original_c_type: Arg, wrappers: BTreeMap<String, CWrapper>) -> Self {
        ReturnType {
            original: original_c_type,
            local_callbacks: false,
            owned_strings: false,
            wrappers,
        }
    }

    pub fn with_local_callbacks(mut self, enabled: bool) -> Self {
        self.local_callbacks = enabled;
        self
    }

    /// Getters must return a copy because another `&self` method can replace C's buffer.
    pub fn with_owned_strings(mut self) -> Self {
        self.owned_strings = true;
        self
    }
    fn callback_policy(&self, handler: &Type) -> TokenStream {
        if self.local_callbacks {
            let p = format_ident!("{}Policy", handler.to_token_stream().to_string());
            quote! { #p }
        } else {
            quote! { SharedCallback }
        }
    }

    pub fn get_new_return_type(&self, convert_errors: bool, use_ref_for_cwrapper: bool) -> TokenStream {
        if let ArgProcessing::Handler(_) = self.original.processing {
            if self.original.name.len() > 0 {
                if !self.original.is_mut_pointer() {
                    let new_type =
                        parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&self.original.c_type)))
                            .expect("Invalid class name in wrapper");
                    let policy = self.callback_policy(&new_type);
                    return quote! { Option<&Handler<#new_type, #policy>> };
                } else {
                    return quote! {};
                }
            }
        } else if let ArgProcessing::StringWithLength(_) = self.original.processing {
            if self.original.name.len() > 0 {
                if self.original.is_c_string() {
                    return if self.owned_strings {
                        quote! { String }
                    } else {
                        quote! { &str }
                    };
                } else if self.original.is_mut_c_string() {
                    // snprintf-style fill buffer: `&mut [u8]` as arg, `&str` as field.
                    if use_ref_for_cwrapper {
                        return quote! { &mut [u8] };
                    } else {
                        return if self.owned_strings {
                            quote! { String }
                        } else {
                            quote! { &str }
                        };
                    }
                } else {
                    return quote! {};
                }
            }
        } else if let ArgProcessing::ByteArrayWithLength(_) = self.original.processing {
            if self.original.name.len() > 0 {
                if self.original.is_byte_array() {
                    if self.original.is_mut_byte_array() {
                        return quote! { &mut [u8] };
                    } else {
                        return quote! { &[u8] };
                    }
                } else {
                    return quote! {};
                }
            }
        }

        if let Some(wrapper) = self.pointee_wrapper() {
            let new_type = parse_str::<Type>(&wrapper.class_name).expect("Invalid class name in wrapper");
            if use_ref_for_cwrapper {
                return quote! { &#new_type };
            } else {
                return quote! { #new_type };
            }
        }
        if let Some(wrapper) = self.wrappers.get(&self.original.c_type) {
            let new_type = parse_str::<Type>(&wrapper.class_name).expect("Invalid wrapper");
            let native = format_ident!("{}", wrapper.type_name);
            return quote! { #new_type<Inline<#native>> };
        }
        if convert_errors && self.original.is_c_raw_int() {
            return quote! { Result<i32, AeronCError> };
        }
        if self.original.is_c_string() {
            if self.owned_strings {
                return quote! { String };
            }
            // if incoming argument use &CString
            if !convert_errors && use_ref_for_cwrapper {
                return quote! { &std::ffi::CStr };
            } else {
                return quote! { &str };
            }
        }
        let return_type: Type = parse_str(&self.original).expect("Invalid return type");
        if use_ref_for_cwrapper && self.original.is_single_mut_pointer() && self.original.is_primitive() {
            let mut_type: Type =
                parse_str(&return_type.to_token_stream().to_string().replace("* mut ", "&mut ")).unwrap();
            return quote! { #mut_type };
        }
        quote! { #return_type }
    }

    /// If this is a pointer to a C struct we have a Rust wrapper for, returns that wrapper, e.g.
    /// `aeron_header_t *header` gives `AeronHeader`. Anything else, such as an `int64_t` or a
    /// `const char *`, gives `None`
    fn pointee_wrapper(&self) -> Option<&CWrapper> {
        if !self.original.is_single_mut_pointer() {
            return None;
        }
        self.wrappers.get(self.original.base_type())
    }

    /// [`Self::pointee_wrapper`] for a callback argument, skipping handler and string/buffer args
    fn callback_wrapper(&self) -> Option<&CWrapper> {
        if !matches!(self.original.processing, ArgProcessing::Default) {
            return None;
        }
        self.pointee_wrapper()
    }

    /// The Rust type a callback gets for this argument. Structs come in as `&Wrapper`, everything
    /// else as before
    ///
    /// Why: when C calls a callback with a struct, that struct only lives until the callback
    /// returns. Often it is a local variable in the C function, and its strings are freed right
    /// after (see `aeron_archive_recording_descriptor_poller.c`). If the callback got the wrapper
    /// by value, this would compile and then read freed memory:
    ///
    /// ```ignore
    /// let mut kept = None;
    /// archive.list_recordings_fn(&mut count, 0, 10, |d| kept = Some(d))?;
    /// kept.unwrap().original_channel(); // C memory is already gone
    /// ```
    ///
    /// With `&AeronArchiveRecordingDescriptor` the compiler rejects `kept = Some(d)`, because the
    /// reference cannot outlive the callback. Copy out the fields you need instead, e.g.
    /// `d.recording_id` or `d.original_channel().to_owned()`
    pub fn get_callback_arg_type(&self) -> TokenStream {
        if let Some(wrapper) = self.callback_wrapper() {
            let class = format_ident!("{}", wrapper.class_name);
            let native = format_ident!("{}", wrapper.type_name);
            return quote! { &#class<Borrowed<#native>> };
        }
        self.get_new_return_type(false, false)
    }

    /// Turns the raw C argument into the type from [`Self::get_callback_arg_type`]. For a struct
    /// pointer `header` this gives `&AeronHeader::from_ptr(header)` when `is_callback_only` (no
    /// safe `From` there) and `&AeronImage::from(image)` otherwise
    pub fn handle_c_to_rs_callback_arg(&self, arg: TokenStream, handlers: &[CHandler]) -> TokenStream {
        let Some(wrapper) = self.callback_wrapper() else {
            return self.handle_c_to_rs_return(arg, false, false);
        };
        let class = parse_str::<Type>(&wrapper.class_name).expect("Invalid class name in wrapper");
        // SAFETY: Aeron supplies a live, non-null object for this callback.
        // Borrowed storage owns nothing, and the reference cannot escape the call.
        quote! { &unsafe { #class::from_ptr(#arg as *mut _) } }
    }

    pub fn handle_c_to_rs_return(&self, result: TokenStream, convert_errors: bool, use_self: bool) -> TokenStream {
        if let ArgProcessing::StringWithLength(args) = &self.original.processing {
            if !self.original.is_c_string_any() {
                return quote! {};
            }
            // Field read: text + stored length (buffer need not be NUL-terminated).
            let string_ptr = &args[0].as_ident();
            let length = &args[1].as_ident();
            let me = if use_self {
                quote! {unsafe { self.get_inner_ref() }.}
            } else {
                quote! {}
            };
            if self.owned_strings {
                return quote! {
                    if #me #string_ptr.is_null() { String::new() } else {
                        unsafe { std::str::from_utf8(std::slice::from_raw_parts(#me #string_ptr as *const u8, #me #length.try_into().unwrap())).unwrap_or("").to_owned() }
                    }
                };
            }
            return quote! {
                if #me #string_ptr.is_null() { "" } else { unsafe { std::str::from_utf8(std::slice::from_raw_parts(#me #string_ptr as *const u8, #me #length.try_into().unwrap())).unwrap_or("") } }
            };
        }
        if let ArgProcessing::ByteArrayWithLength(args) = &self.original.processing {
            if !self.original.is_byte_array() {
                return quote! {};
            } else {
                let star_const = &args[0].as_ident();
                let length = &args[1].as_ident();
                let me = if use_self {
                    quote! {unsafe { self.get_inner_ref() }.}
                } else {
                    quote! {}
                };
                if self.original.is_mut_byte_array() {
                    return quote! {
                        unsafe { if #me #star_const.is_null() { &mut [] as &mut [_]  } else {std::slice::from_raw_parts_mut(#me #star_const, #me #length.try_into().unwrap()) } }
                    };
                } else {
                    return quote! {
                        if #me #star_const.is_null() { &[] as &[_]  } else { std::slice::from_raw_parts(#me #star_const, #me #length.try_into().unwrap()) }
                    };
                }
            }
        }

        if convert_errors && self.original.is_c_raw_int() {
            quote! {
                if result < 0 {
                    return Err(AeronCError::from_code(result));
                } else {
                    return Ok(result)
                }
            }
        } else if self.original.is_c_string() {
            if self.owned_strings {
                if let ArgProcessing::StringWithLength(args) = &self.original.processing {
                    let length = &args[1].as_ident();
                    return quote! { if #result.is_null() { String::new() } else { std::str::from_utf8(std::slice::from_raw_parts(#result as *const u8, #length.try_into().unwrap())).unwrap_or("").to_owned() } };
                }
                return quote! { if #result.is_null() { String::new() } else { unsafe { std::ffi::CStr::from_ptr(#result).to_str().unwrap_or("").to_owned() } } };
            }
            if let ArgProcessing::StringWithLength(args) = &self.original.processing {
                let length = &args[1].as_ident();
                return quote! { if #result.is_null() { ""} else { std::str::from_utf8(std::slice::from_raw_parts(#result as *const u8, #length.try_into().unwrap())).unwrap_or("")}};
            } else {
                return quote! { if #result.is_null() { ""} else { unsafe { std::ffi::CStr::from_ptr(#result).to_str().unwrap_or("") } } };
            }
        } else if self.original.is_single_mut_pointer() && self.original.is_primitive() {
            return result;
        } else if let Some(wrapper) = self.pointee_wrapper() {
            let class = format_ident!("{}", wrapper.class_name);
            quote! { unsafe { #class::from_ptr(std::ptr::NonNull::new(#result as *mut _).expect("native getter returned null").as_ptr()) } }
        } else if let Some(wrapper) = self.wrappers.get(&self.original.c_type) {
            let class = format_ident!("{}", wrapper.class_name);
            quote! { unsafe { #class::from_native(#result) } }
        } else {
            quote! { #result.into() }
        }
    }

    /// Generic bounds for a handler argument. `force_static` is used by constructors,
    /// where the handler is always cloned into the created resource's dependencies
    /// (requiring `'static` for the `dyn Any` storage). For plain methods, only
    /// *retained* handlers (see `SYNC_HANDLER_TYPES`) get the `'static` bound.
    /// Synchronous handlers may borrow local state, and their `_fn` closure variants
    /// rely on that.
    pub fn method_generics_for_where(&self, force_static: bool, shared_owner: bool) -> Option<TokenStream> {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let new_type = parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&handler.c_type)))
                    .expect("Invalid class name in wrapper");
                let new_handler = parse_str::<Type>(&format!("{}Callback", snake_to_pascal_case(&handler.c_type)))
                    .expect("Invalid class name in wrapper");
                let policy = self.callback_policy(&new_type);
                let policy_generic = if self.local_callbacks {
                    quote! { , #policy: CallbackPolicy }
                } else {
                    quote! {}
                };
                let needs_static = force_static || !is_sync_handler_type(&handler.c_type);
                let send = if shared_owner {
                    quote! { + Send }
                } else {
                    quote! {}
                };
                return if needs_static {
                    Some(quote! {
                        #new_type: #new_handler #send + 'static #policy_generic
                    })
                } else {
                    Some(quote! {
                        #new_type: #new_handler #policy_generic
                    })
                };
            }
        }
        None
    }

    pub fn method_generics_for_method(&self) -> Option<TokenStream> {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let new_type = parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&handler.c_type)))
                    .expect("Invalid class name in wrapper");
                return Some(quote! {
                    #new_type
                });
            }
        }
        None
    }

    pub fn handle_rs_to_c_return(&self, result: TokenStream, include_field_name: bool) -> TokenStream {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let handler_name = handler.as_ident();
                let handler_type = handler.as_type();
                let clientd_name = handler_client.get(1).unwrap().as_ident();
                // constructors keep the handler: a poll-path handler panic is caught and raised by
                // the poll above, anything else C fires later is logged (as in arg_classifier)
                let suffix = if UNWIND_HANDLER_TYPES.contains(&handler.c_type.as_str()) {
                    "_catch"
                } else {
                    ""
                };
                let method_name = format_ident!("{}_callback{}", handler.c_type, suffix);
                let new_type =
                    parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&self.original.c_type)))
                        .expect("Invalid class name in wrapper");
                let policy = self.callback_policy(&new_type);
                if include_field_name {
                    return quote! {
                        #handler_name: { let callback: #handler_type = if #handler_name.is_none() { None } else { Some(#method_name::<#new_type, #policy>) }; callback },
                        #clientd_name: #handler_name.map(|m|m.as_raw()).unwrap_or_else(|| std::ptr::null_mut())
                    };
                } else {
                    return quote! {
                        { let callback: #handler_type = if #handler_name.is_none() { None } else { Some(#method_name::<#new_type, #policy>) }; callback },
                        #handler_name.map(|m|m.as_raw()).unwrap_or_else(|| std::ptr::null_mut())
                    };
                }
            } else {
                return quote! {};
            }
        }
        // Slice lengths are cast with `as _` because the C length is `usize` or `int32_t` (see
        // `process_types`); a slice over `i32::MAX` bytes would wrap for the `int32_t` case
        if let ArgProcessing::StringWithLength(handler_client) = &self.original.processing {
            // Emit the (pointer, length) pair once, on the length argument.
            if !self.original.is_c_string_any() {
                let array = handler_client.get(0).unwrap();
                let array_name = array.as_ident();
                let length_name = handler_client.get(1).unwrap().as_ident();
                // `&str` in-param for const buffers; `&mut [u8]` fill-buffer for mut ones.
                let ptr = if array.is_mut_c_string() {
                    quote! { #array_name.as_mut_ptr() as *mut _ }
                } else {
                    quote! { #array_name.as_ptr() as *const _ }
                };
                if include_field_name {
                    return quote! {
                        #array_name: #ptr,
                        #length_name: #array_name.len() as _
                    };
                } else {
                    return quote! {
                        #ptr,
                        #array_name.len() as _
                    };
                }
            } else {
                return quote! {};
            }
        }
        if let ArgProcessing::ByteArrayWithLength(handler_client) = &self.original.processing {
            if !self.original.is_byte_array() {
                let array = handler_client.get(0).unwrap();
                let array_name = array.as_ident();
                let length_name = handler_client.get(1).unwrap().as_ident();
                if include_field_name {
                    return quote! {
                        #array_name: #array_name.as_ptr() as *mut _,
                        #length_name: #array_name.len() as _
                    };
                } else {
                    return quote! {
                        #array_name.as_ptr() as *mut _,
                        #array_name.len() as _
                    };
                }
            } else {
                return quote! {};
            }
        }

        if include_field_name {
            let arg_name = self.original.as_ident();
            return if self.original.is_c_string() {
                quote! {
                    #arg_name: #result.as_ptr()
                }
            } else {
                if self.original.is_single_mut_pointer() && self.original.is_primitive() {
                    return quote! {
                        #arg_name: #result as *mut _
                    };
                }

                quote! { #arg_name: #result.into() }
            };
        }

        if self.original.is_single_mut_pointer() && self.original.is_primitive() {
            return quote! {
                #result as *mut _
            };
        }

        if self.original.is_c_string() {
            quote! {
                #result.as_ptr()
            }
        } else {
            quote! { #result.into() }
        }
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct CWrapper {
    /// Alias, enum and aggregate definitions shared by the parsed bindings.
    pub native_types: std::sync::Arc<crate::NativeTypeMetadata>,
    /// Unsafe C callback aliases, including aliases without a clientd argument.
    pub unsafe_callback_aliases: std::sync::Arc<BTreeSet<String>>,
    pub class_name: String,
    pub type_name: String,
    /// Canonical bindgen struct, used to share one policy across C typedef aliases.
    pub native_type: String,
    pub without_name: String,
    pub fields: Vec<Arg>,
    pub methods: Vec<Method>,
    pub docs: BTreeSet<String>,
    /// Rust method names replaced by handwritten code or explicit override metadata.
    pub skipped_methods: BTreeSet<String>,
    /// The C struct is forward-declared only (bindgen's `_unused: [u8; 0]`), directly or via a
    /// typedef. bindgen derives no `Copy`/`Clone` for it, and copying one by value copies nothing
    pub opaque: bool,
}

/// Parse `aeron_custom.rs` source and return a map of `ClassName -> {method_names}`
/// for every `impl ClassName { fn method_name ... }` block found.
/// This lets the generator skip auto-generating methods that have hand-written overrides.
pub fn parse_custom_methods(src: &str) -> HashMap<String, BTreeSet<String>> {
    let mut result: HashMap<String, BTreeSet<String>> = HashMap::new();
    let file = syn::parse_file(src).unwrap_or_else(|e| {
        panic!(
            "rusteron codegen: failed to parse aeron_custom.rs while extracting custom method \
             names (this would silently empty the generator skip-list and emit duplicate \
             methods): {e}"
        )
    });
    for item in &file.items {
        if let Item::Impl(impl_block) = item {
            // Only plain `impl TypeName { ... }`, no trait impls
            if impl_block.trait_.is_some() {
                continue;
            }
            let syn::Type::Path(path) = impl_block.self_ty.as_ref() else {
                continue;
            };
            let type_name = path.path.segments.last().unwrap().ident.to_string();
            let entry = result.entry(type_name).or_default();
            for impl_item in &impl_block.items {
                if let ImplItem::Fn(method) = impl_item {
                    entry.insert(method.sig.ident.to_string());
                }
            }
        }
    }
    result
}

impl CWrapper {
    /// Unknown value types can hide pointers in unions, aliases or arrays.
    /// Only recognise scalar types and aggregates that we can inspect.
    fn type_borrows_native_storage(&self, ty: &Type, wrappers: &BTreeMap<String, CWrapper>) -> bool {
        match ty {
            Type::Array(array) => self.type_borrows_native_storage(&array.elem, wrappers),
            Type::Path(path) => {
                let name = path.to_token_stream().to_string();
                if self.native_types.contains(&name) {
                    return self.native_types.facts(ty).borrows_storage;
                }
                if let Some(nested) = wrappers.get(&name) {
                    return nested.borrows_native_storage(wrappers);
                }
                self.native_types.facts(ty).borrows_storage
            }
            _ => self.native_types.facts(ty).borrows_storage,
        }
    }

    fn field_borrows_native_storage(&self, field: &Arg, wrappers: &BTreeMap<String, CWrapper>) -> bool {
        self.type_borrows_native_storage(&field.as_type(), wrappers)
    }

    // C structs cannot contain themselves by value, so embedded value types
    // form an acyclic graph. Pointers stop the traversal here.
    pub(crate) fn borrows_native_storage(&self, wrappers: &BTreeMap<String, CWrapper>) -> bool {
        self.fields
            .iter()
            .any(|field| self.field_borrows_native_storage(field, wrappers))
    }

    /// Zero must be a valid Rust value and need no native initialization.
    /// Method-free text records also accept null pointers as empty strings.
    pub(crate) fn zeroed_is_valid(&self, wrappers: &BTreeMap<String, CWrapper>) -> bool {
        (!self.opaque || self.methods.is_empty())
            && self
                .fields
                .iter()
                .all(|field| self.native_types.facts(&field.as_type()).zero_valid == Some(true))
            && (self.methods.is_empty() || !self.borrows_native_storage(wrappers))
            && self
                .fields
                .iter()
                .all(|field| field.is_c_string_any() || !self.field_borrows_native_storage(field, wrappers))
    }

    fn has_invalid_zero(&self) -> bool {
        self.fields
            .iter()
            .any(|field| self.native_types.facts(&field.as_type()).zero_valid == Some(false))
    }

    pub fn find_methods(&self, name: &str) -> Vec<Method> {
        self.methods
            .iter()
            .filter(|m| m.struct_method_name == name)
            .cloned()
            .collect_vec()
    }

    pub fn find_unique_method(&self, name: &str) -> Option<Method> {
        let results = self.find_methods(name);
        if results.len() == 1 {
            results.into_iter().next()
        } else {
            None
        }
    }

    fn get_close_method(&self) -> Option<Method> {
        self.find_unique_method("close")
    }

    /// Generate logging expressions for method arguments
    fn generate_arg_logging(arguments: &[Arg], arg_names: &[TokenStream]) -> TokenStream {
        let mut arg_names_idx = 0;
        let mut arg_names_for_logging = vec![];

        for (arg_idx, arg) in arguments.iter().enumerate() {
            if arg_names_idx >= arg_names.len() {
                break;
            }

            let arg_name_str = &arg.name;
            let arg_type = arg.as_type();
            let arg_ident = arg.as_ident();

            // Determine how to log this argument
            match &arg.processing {
                ArgProcessing::Handler(_) if !arg.is_mut_pointer() => {
                    // Show the type for handlers.
                    arg_names_for_logging.push(quote! {
                        concat!(#arg_name_str, ": ", stringify!(#arg_type)).to_string()
                    });
                    arg_names_idx += 2; // Skip BOTH expanded values (callback + clientd)
                }
                ArgProcessing::StringWithLength(_args) => {
                    // Check if this is the length argument (second in pair) - skip it
                    if arg_idx > 0 && arguments[arg_idx - 1].processing == arg.processing {
                        continue;
                    }
                    // This is the string argument - show the actual string value
                    arg_names_for_logging.push(quote! {
                        format!("{} = {:?}", #arg_name_str, #arg_ident)
                    });
                    arg_names_idx += 2;
                }
                ArgProcessing::ByteArrayWithLength(_args) => {
                    // Check if this is the length argument (second in pair) - skip it
                    if arg_idx > 0 && arguments[arg_idx - 1].processing == arg.processing {
                        continue;
                    }
                    // This is the byte array argument - show name, type, and length
                    arg_names_for_logging.push(quote! {
                        format!("{}: {} (len={})", #arg_name_str, stringify!(#arg_type), #arg_ident.len())
                    });
                    arg_names_idx += 2;
                }
                _ => {
                    // For primitive types, show value. For pointers and structs, show name:type
                    if arg.is_primitive() && !arg.is_any_pointer() {
                        arg_names_for_logging.push(quote! {
                            format!("{} = {:?}", #arg_name_str, #arg_ident)
                        });
                    } else {
                        arg_names_for_logging.push(quote! {
                            concat!(#arg_name_str, ": ", stringify!(#arg_type)).to_string()
                        });
                    }
                    arg_names_idx += 1;
                }
            }
        }

        // For logging - need explicit type when array is empty
        if arg_names_for_logging.is_empty() {
            quote! { [""; 0].join(", ") }
        } else {
            quote! { [#(#arg_names_for_logging),*].join(", ") }
        }
    }

    /// Generate methods for the struct
    fn generate_methods(
        &self,
        wrappers: &BTreeMap<String, CWrapper>,
        closure_handlers: &Vec<CHandler>,
        ref_getters: &mut Vec<TokenStream>,
    ) -> Vec<TokenStream> {
        self.methods
            .iter()
            .filter(|m| !m.arguments.iter().any(|arg| arg.is_double_mut_pointer()))
            .filter(|m| !self.skipped_methods.contains(&m.struct_method_name))
            .filter(|m| !crate::convenience::replaces_method(self, m))
            .filter(|m| m.struct_method_name != "close")
            // These change C's cleanup owner. Returning a borrowed wrapper cannot
            // represent that transfer; expose it only through an audited custom API.
            .filter(|m| !m.fn_name.contains("_get_and_own_"))
            .map(|method| {

                let return_type_helper = ReturnType::new(method.return_type.clone(), wrappers.clone());
                let mut return_type = return_type_helper.get_new_return_type(true, false);
                let returns_text = return_type.to_string() == "& str";
                let string_view = crate::string_views::method(&self.type_name, method);
                let unchecked_text = returns_text && string_view == crate::string_views::StringView::Unchecked;
                if returns_text && string_view == crate::string_views::StringView::Static {
                    return_type = quote! { &'static str };
                }
                let ffi_call = Ident::new(&method.fn_name, proc_macro2::Span::call_site());

                // One classification pass: every argument becomes a ClassifiedArg carrying its
                // signature/call/generic/registration/_once fragments (see arg_classifier.rs). All
                // emitters below consume the plan, so there are no parallel loops to keep in sync.
                let classified = crate::arg_classifier::classify_method_args(method, &self.type_name, wrappers, closure_handlers);
                let exclusive_poll = classified.exclusive_poll(method);
                let fn_name = Ident::new(if method.struct_method_name == "get_buffers" {
                    "get_buffers_into"
                } else if method.struct_method_name == "copy" && method.fn_name.ends_with("_copy") {
                    "copy_from"
                } else { &method.struct_method_name }, proc_macro2::Span::call_site());
                let uses_self = classified.uses_self;
                let owned_retained_handler = classified.owned_retained.clone();

                let mut generic_types: Vec<TokenStream> = classified.generics();
                let mut fn_arguments: Vec<TokenStream> = classified.signatures();
                if unchecked_text && !uses_self {
                    generic_types.insert(0, quote! { 'text });
                    return_type = quote! { &'text str };
                }
                if let crate::string_views::StringView::Argument(name) = string_view {
                    generic_types.insert(0, quote! { 'text });
                    return_type = quote! { &'text str };
                    let argument = fn_arguments.iter_mut().find(|argument| {
                        matches!(syn::parse2::<syn::FnArg>((*argument).clone()),
                            Ok(syn::FnArg::Typed(arg)) if matches!(&*arg.pat,
                                syn::Pat::Ident(ident) if ident.ident == name))
                    }).expect("borrowed text requires its source argument");
                    let syn::FnArg::Typed(mut parsed) = syn::parse2(argument.clone()).unwrap() else { unreachable!() };
                    let Type::Reference(reference) = &mut *parsed.ty else {
                        panic!("borrowed text requires a reference argument: {name}");
                    };
                    reference.lifetime = Some(syn::parse_quote!('text));
                    *argument = quote! { #parsed };
                }
                let where_clause = if generic_types.is_empty() {
                    quote! {}
                } else {
                    quote! { <#(#generic_types),*> }
                };

                let mut arg_names: Vec<TokenStream> = classified.calls();
                let retained_handler_registrations: Vec<TokenStream> = classified.registrations();
                let preludes: Vec<TokenStream> = classified.preludes();
                let resumes: Vec<TokenStream> = classified.resumes();

                let mut converter = return_type_helper.handle_c_to_rs_return(quote! { result }, true, false);
                // The prefix-length check returns an error, so the plain C value is wrapped
                if method.arguments.iter().any(|a| crate::arg_classifier::prefix_limit(&method.fn_name, &a.name).is_some()) {
                    return_type = quote! { Result<#return_type, AeronCError> };
                    converter = quote! { Ok(#converter) };
                }

                // Heap-allocate the owned callback and capture its raw clientd before the
                // FFI call, so the Handler itself stays available for registration/return.
                let handler_prelude = if let Some(arg) = &owned_retained_handler {
                    let name = arg.as_ident();
                    let raw_name = format_ident!("{}_raw", arg.name);
                    let replacement_check = if handle_is_archive_context(&self.type_name) {
                        let key = &method.fn_name;
                        quote! { self.inner.check_callback_replacement(#key)?; }
                    } else { quote! {} };
                    // TODO: Aeron 1.54.2 `aeron_archive_context_set_idle_strategy` after conclude keeps
                    // `owns_idle_strategy`, so close frees the Handler's state. No guard here, the fix
                    // belongs in Aeron (https://github.com/aeron-io/aeron/issues/2250). Un-ignore
                    // `replacing_the_idle_strategy_after_connect_keeps_the_handler` once vendored
                    quote! {
                        if self.inner.as_owned().is_none() {
                            return Err(AeronCError::with_message(-1, "a borrowed wrapper cannot retain a callback"));
                        }
                        #replacement_check
                        let #name = #name.map(SharedHandler::new_shared);
                        let #raw_name = #name
                            .as_ref()
                            .map(|m| m.as_raw())
                            .unwrap_or_else(|| std::ptr::null_mut());
                    }
                } else {
                    quote! {}
                };

                if let Some(arg) = &owned_retained_handler {
                    let name = arg.as_ident();
                    let new_type = parse_str::<Type>(&format!(
                        "{}HandlerImpl",
                        snake_to_pascal_case(&arg.c_type)
                    ))
                    .expect("Invalid class name in wrapper");
                    return_type = quote! { Result<Option<SharedHandler<#new_type>>, AeronCError> };
                    converter = quote! {
                        if result < 0 {
                            return Err(AeronCError::from_code(result));
                        } else {
                            return Ok(#name);
                        }
                    };
                }

                // A C object reached through `self` belongs to `self`'s owner, so the lifetime stops
                // it outliving that owner without a reference count on the hot path
                let borrowed_child = if uses_self && owned_retained_handler.is_none() {
                    return_type_helper.pointee_wrapper().map(|w| format_ident!("{}", w.class_name))
                } else {
                    None
                };
                let nullable_child = crate::arg_classifier::is_c_owned_set_type(&method.return_type.c_type);
                if let Some(child) = &borrowed_child {
                    if nullable_child {
                        return_type = quote! { Option<Ref<'_, #child>> };
                        converter = quote! { if result.is_null() { None } else { Some(Ref::new(#converter)) } };
                    } else {
                        return_type = quote! { Ref<'_, #child> };
                        converter = quote! { Ref::new(#converter) };
                    }
                }

                let mut method_docs: Vec<TokenStream> = get_docs(&method.docs, wrappers, Some(&fn_arguments) );
                let unsafety = MethodUnsafety::new(&self.type_name, method, wrappers, &fn_arguments, &self.unsafe_callback_aliases);
                // The `&str` borrows native storage that a setter, poll or close can free, even through
                // another clone, so the caller must promise to use it straight away. Java and C++ copy instead
                let method_unsafety = if unsafety.any() || unchecked_text { quote! { unsafe } } else { quote! {} };
                if unchecked_text {
                    method_docs.push(quote! { #[doc = "# Safety"] });
                    method_docs.push(quote! { #[doc = "The text points into native storage. A setter, poll or close can rewrite or free it, even through another clone of this handle. Use it straight away, for example to log or copy it, and do not hold it across any call that can change this resource."] });
                    if crate::string_views::has_owned_method(&self.type_name, method) {
                        let owned_doc = format!("Use `{fn_name}_owned()` to keep a copy.");
                        method_docs.push(quote! { #[doc = #owned_doc] });
                    }
                    if method.fn_name == "aeron_realpath" {
                        method_docs.push(quote! { #[doc = "On POSIX, the output buffer must hold at least PATH_MAX bytes. Aeron calls realpath without passing the slice length. Keep the returned text within the output buffer's lifetime and do not mutate the buffer while the text is borrowed."] });
                    }
                }
                method_docs.extend(unsafety.docs());


                if owned_retained_handler.is_some() {
                    method_docs.push(quote! { #[doc = ""] });
                    if handle_is_archive_context(&self.type_name) {
                        method_docs.push(quote! { #[doc = "Replaces this context's handler after a successful registration. Existing connections keep the handler they copied when connecting."] });
                    } else {
                        method_docs.push(quote! { #[doc = "An owned wrapper keeps the callback alive until C cleanup finishes."] });
                    }
                    method_docs.push(quote! { #[doc = "A borrowed wrapper returns an error before changing C, because it cannot keep a callback alive."] });
                    method_docs.push(quote! { #[doc = "Dropping the handler does not unregister the callback."] });
                    method_docs.push(quote! { #[doc = "Returns the [`Handler`] so you can access its state. You can pass a closure with a matching signature."] });
                }

                if let Some(child) = &borrowed_child
                    && fn_arguments.is_empty()
                    && generic_types.is_empty()
                {
                    let panics = !Self::track_caller_if_panics(&[&converter]).is_empty();
                    ref_getters.push(Self::ref_getter(&fn_name, child, &method_docs, nullable_child, panics));
                }

                // Logging expression for arguments
                let args_log_expr = Self::generate_arg_logging(&method.arguments, &arg_names);

                let possible_self = if uses_self {
                    quote! { &self, }
                } else {
                    quote! {}
                };



                let mut register_handlers = if uses_self && !retained_handler_registrations.is_empty() {
                    quote! { #(#retained_handler_registrations)* }
                } else {
                    quote! {}
                };
                // C copies the pair's handler and clientd and may fire them until the client
                // closes (close handlers fire inside that close), so the client keeps the pair
                // and, through it, its Handler. Remove cannot release it: dependencies only grow.
                // TODO: on timeout Aeron 1.54.2 add/remove return -1 while the command stays queued
                // (https://github.com/aeron-io/aeron/issues/2250), so even a failed add may stay
                // registered while we skip retaining it. Once fixed, a successful remove could also
                // release the pair. Retention on add stays: C fires the pair until the client closes
                if matches!(
                    method.fn_name.as_str(),
                    "aeron_add_available_counter_handler"
                        | "aeron_add_unavailable_counter_handler"
                        | "aeron_add_close_handler"
                ) {
                    register_handlers.extend(quote! {
                        if result >= 0 {
                            if let Some(__inner) = self.inner.as_owned() {
                                __inner.add_handle_dependency(pair);
                            }
                        }
                    });
                }

                let mut additional_methods = vec![];
                if unchecked_text && crate::string_views::has_owned_method(&self.type_name, method) {
                    let owned_name = format_ident!("{}_owned", fn_name);
                    let owned_unsafety = if unsafety.any() { quote! { unsafe } } else { quote! {} };
                    let owned_converter = ReturnType::new(method.return_type.clone(), wrappers.clone())
                        .with_owned_strings().handle_c_to_rs_return(quote! { result }, true, false);
                    let owned_self = if uses_self { quote! { &self, } } else { quote! {} };
                    additional_methods.push(quote! {
                        /// Copy the native text into an owned string. This allocates for nonempty text.
                        #[inline]
                        #[must_use]
                        pub #owned_unsafety fn #owned_name(#owned_self #(#fn_arguments),*) -> String {
                            #(#preludes)*
                            unsafe {
                                let result = #ffi_call(#(#arg_names),*);
                                #owned_converter
                            }
                        }
                    });
                }
                if let Some(direct) = &exclusive_poll {
                    let name = format_ident!("{}", method.struct_method_name);
                    let generics = direct.generics();
                    let arguments = direct.signatures();
                    let calls = direct.calls();
                    let preludes = direct.preludes();
                    let resumes = direct.resumes();
                    let direct_log_expr = Self::generate_arg_logging(&method.arguments, &calls);
                    additional_methods.push(quote! {
                        #[inline]
                        #(#method_docs)*
                        ///
                        /// Borrows the callback exclusively until this call returns.
                        /// This method uses no callback allocation, reference count,
                        /// runtime borrow check or mutex. The callback can borrow local state.
                        pub #method_unsafety fn #name<#(#generics),*>(#possible_self #(#arguments),*) -> #return_type {
                            #(#preludes)*
                            #[cfg(feature = "log-c-bindings")]
                            log::info!("{}({})", stringify!(#ffi_call), #direct_log_expr);
                            // SAFETY: the callback is exclusively borrowed and remains
                            // alive until this synchronous native call returns.
                            let result = unsafe { #ffi_call(#(#calls),*) };
                            #(#resumes)*
                            #[cfg(feature = "log-c-bindings")]
                            log::info!("  -> {:?}", result);
                            unsafe { #converter }
                        }
                    });
                }
                let set_closed = if (["aeron_context_t", "aeron_driver_context_t", "aeron_archive_context_t", "aeron_archive_persistent_subscription_context_t"].contains(&self.type_name.as_str()) && method.fn_name.contains("_set_"))
                    || method.fn_name == "aeron_driver_validate_and_apply_affinity_configuration" {
                    if method.return_type.is_c_raw_int() { quote! { self.inner.check_configurable()?; } }
                    else { quote! { self.inner.check_configurable().expect("context cannot be changed after use"); } }
                } else if method.fn_name == "aeron_start" {
                    let started = quote! {
                        if __owner.started.load(std::sync::atomic::Ordering::Acquire) { return Ok(0); }
                    };
                    quote! {
                        let __owner = self.inner.as_owned().ok_or_else(|| AeronCError::with_message(-1, "manual conductor operations require an owning client handle"))?;
                        #started
                        let __operation = __owner.operation_guard()?;
                        #started
                    }
                } else { quote! {} };
                let finish_async_poll = handle_is_consumed_by_poll(&self.type_name) && method.fn_name.ends_with("_poll");
                let mark_poll_complete = if finish_async_poll {
                    quote! { if result != 0 { if let Some(owner) = self.inner.as_owned() { owner.mark_resource_released(); } } }
                } else { quote! {} };
                let mark_output_poll_complete = if finish_async_poll {
                    quote! { if err_code != 0 { if let Some(owner) = self.inner.as_owned() { owner.mark_resource_released(); } } }
                } else { quote! {} };
                let mark_started = if method.fn_name == "aeron_start" {
                    quote! { if result >= 0 { __owner.started.store(true, std::sync::atomic::Ordering::Release); } }
                } else { quote! {} };
                Self::add_mut_string_methods_if_applicable(method, &fn_name, uses_self, &method_docs, &mut additional_methods);

                // getter methods
                Self::add_getter_instead_of_mut_arg_if_applicable(wrappers, method, &fn_name, &where_clause, &possible_self, &method_docs, &mut additional_methods);
                self.add_constants_forwarders(wrappers, method, &mut additional_methods);

                // `_fn` stack-closure variant, emitted straight from the plan (sync-only:
                // a stack closure handed to a retained callback would dangle). Skipped when
                // aeron_custom.rs hand-writes a method of the same `<name>_fn` name, so
                // custom code can override/deprecate a generated `_fn` variant.
                if classified.once_capable
                    && !self
                        .skipped_methods
                        .contains(&format!("{}_fn", method.struct_method_name))
                {
                    let once_fn_name = format_ident!("{}_fn", fn_name);
                    let once_generics = classified.once_generics();
                    let once_where = if once_generics.is_empty() {
                        quote! {}
                    } else {
                        quote! { <#(#once_generics),*> }
                    };
                    let once_args = classified.once_signatures();
                    let once_calls = classified.once_calls();
                    let once_resumes = classified.once_resumes();
                    let once_log_expr = Self::generate_arg_logging(&method.arguments, &once_calls);
                    let panics_doc = if UNWIND_POLL_FNS.contains(&method.fn_name.as_str()) {
                        quote! {
                            /// # Panics
                            ///
                            /// A panic in the closure is passed to the caller. Fragments since the poll
                            /// started (or the last `COMMIT`) are delivered again on the next poll
                        }
                    } else {
                        quote! {
                            /// # Panics
                            ///
                            /// A panic in the closure is caught so the C call can finish, then passed to
                            /// the caller once it returns
                        }
                    };
                    additional_methods.push(quote! {
                        #[inline]
                        #(#method_docs)*
                        ///
                        /// Stack-borrowed closure variant. The `FnMut` closure lives on the caller's
                        /// stack and is borrowed for this call only. The callback fires synchronously,
                        /// so nothing is heap-allocated or stored and the closure may borrow local
                        /// state. Generated only for callbacks C does not retain. Prefer it to the
                        /// [`Handler`] form on the hot path
                        ///
                        #panics_doc
                        pub #method_unsafety fn #once_fn_name #once_where(#possible_self #(#once_args),*) -> #return_type {
                            #set_closed
                            unsafe {
                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #once_log_expr
                                );

                                let result = #ffi_call(#(#once_calls),*);
                                #(#once_resumes)*

                                #[cfg(feature = "log-c-bindings")]
                                log::info!("  -> {:?}", result);

                                #converter
                            }
                        }
                    });
                }

                let mut_primitivies = method.arguments.iter()
                    .filter(|a| a.is_mut_pointer() && a.is_primitive())
                    .collect_vec();
                let single_mut_field = method.return_type.is_c_raw_int() && mut_primitivies.len() == 1;

                // Some Aeron methods return an error code and write a result through `&mut primitive`.
                // Return that primitive instead of making the caller pass it in
                let __method_tokens = if single_mut_field {
                    let mut_field = mut_primitivies.first().unwrap();
                    // keep the qualified path (`c_int` is only valid qualified), not only its last segment
                    let rt: Type = parse_str(mut_field.c_type.trim_start_matches("* mut").trim()).unwrap();
                    let return_type = quote! { Result<#rt, AeronCError> };

                    let fn_arguments= fn_arguments.into_iter().filter(|arg| {!arg.to_string().contains("& mut ")})
                        .collect_vec();

                    let idx = arg_names.iter().enumerate()
                        .filter(|(_, arg)| arg.to_string().ends_with("* mut _"))
                        .map(|(i, _)| i)
                        .next().unwrap();

                    // These counter functions write their output on every nonnegative
                    // return (aeron_counters_manager.c). Other APIs can return success
                    // without writing, for example a missing URI timeout parameter.
                    let writes_output_on_success = matches!(method.fn_name.as_str(),
                        "aeron_counters_reader_counter_registration_id"
                            | "aeron_counters_reader_counter_owner_id"
                            | "aeron_counters_reader_counter_reference_id"
                            | "aeron_counters_reader_counter_state"
                            | "aeron_counters_reader_counter_type_id"
                            | "aeron_counters_reader_free_for_reuse_deadline_ms"
                    );
                    let output_init = if writes_output_on_success {
                        arg_names[idx] = quote! { mut_result.as_mut_ptr() };
                        quote! { let mut mut_result = std::mem::MaybeUninit::<#rt>::uninit(); }
                    } else {
                        arg_names[idx] = quote! { &mut mut_result };
                        quote! { let mut mut_result: #rt = Default::default(); }
                    };
                    let output_read = if writes_output_on_success {
                        quote! {
                            // SAFETY: the audited native function wrote the output.
                            // The error path above neither reads nor logs its value.
                            let mut_result = mut_result.assume_init();
                        }
                    } else {
                        quote! {}
                    };

                    let mut first = true;
                    let mut method_docs = method_docs.iter()
                        .filter(|d| !d.to_string().contains("# Return"))
                        .map(|d| {
                            let mut string = d.to_string();
                            string = string.replace("# Parameters", "");
                            if string.contains("out param") {
                                TokenStream::from_str(&string.replace("- `", "\\n# Return\\n`")).unwrap()
                            } else {
                                if string.contains("- `") && first {
                                    first = false;
                                    string = string.replacen("- `","# Parameters\n- `", 1);
                                }
                                TokenStream::from_str(&string).unwrap()
                            }
                        })
                        .collect_vec();

                    let filter_param_title = !method_docs.iter().any(|d| d.to_string().contains("- `"));

                    if filter_param_title {
                        method_docs = method_docs.into_iter()
                            .map(|s| TokenStream::from_str(s.to_string().replace("# Parameters\n", "").as_str()).unwrap())
                            .collect_vec();
                    }


                    quote! {
                        #[inline]
                        #(#method_docs)*
                        pub #method_unsafety fn #fn_name #where_clause(#possible_self #(#fn_arguments),*) -> #return_type {
                            #set_closed
                            #(#preludes)*
                            unsafe {
                                #output_init

                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #args_log_expr
                                );

                                let err_code = #ffi_call(#(#arg_names),*);
                                #mark_output_poll_complete
                                #register_handlers
                                #(#resumes)*

                                if err_code < 0 {
                                    #[cfg(feature = "log-c-bindings")]
                                    log::info!("  -> err_code = {:?}", err_code);
                                    return Err(AeronCError::from_code(err_code));
                                }
                                #output_read
                                #[cfg(feature = "log-c-bindings")]
                                log::info!("  -> err_code = {:?}, result = {:?}", err_code, mut_result);
                                Ok(mut_result)
                            }
                        }

                        #(#additional_methods)*
                    }
                } else if exclusive_poll.is_some() {
                    // A poll calls its callback before C returns, so the `&mut` form covers every
                    // caller. A `Handler` form would only add a borrow check or mutex per poll
                    quote! { #(#additional_methods)* }
                } else {
                    let must_use = Self::getter_must_use(&fn_name, uses_self && fn_arguments.is_empty(), &return_type);
                    let track_caller = Self::track_caller_if_panics(&[&set_closed, &converter]);
                    quote! {
                        #[inline]
                        #must_use
                        #track_caller
                        #(#method_docs)*
                        pub #method_unsafety fn #fn_name #where_clause(#possible_self #(#fn_arguments),*) -> #return_type {
                            #set_closed
                            #(#preludes)*
                            #handler_prelude
                            unsafe {
                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #args_log_expr
                                );

                                let result = #ffi_call(#(#arg_names),*);
                                #mark_poll_complete
                                #mark_started
                                #register_handlers
                                #(#resumes)*

                                #[cfg(feature = "log-c-bindings")]
                                log::info!("  -> {:?}", result);

                                #converter
                            }
                        }

                        #(#additional_methods)*
                    }
                };

                // Archive control operations return typed errors (AeronArchiveError) so
                // callers can match on the structured code. Apply uniformly at the token
                // level to every method on `aeron_archive_t`, covering the main body plus the
                // `additional_methods` (_fn variants, out-param getters, string getters)
                // which all live in this same closure scope.
                if self.type_name == "aeron_archive_t" {
                    __method_tokens
                        .to_string()
                        .replace("AeronCError", "AeronArchiveError")
                        .parse()
                        .expect("AeronArchiveError token substitution must yield valid tokens")
                } else {
                    __method_tokens
                }
            })
            .collect()
    }

    /// `#[must_use]` for a no-argument `get_*` or `is_*` read. `Result` is already must-use, and
    /// clippy rejects marking it twice
    fn getter_must_use(fn_name: &Ident, no_arguments: bool, return_type: &TokenStream) -> TokenStream {
        let name = fn_name.to_string();
        let return_type = return_type.to_string();
        if no_arguments
            && (name.starts_with("get_") || name.starts_with("is_"))
            && !return_type.is_empty()
            && return_type != "()"
            && !return_type.starts_with("Result <")
        {
            quote! { #[must_use] }
        } else {
            quote! {}
        }
    }

    /// `#[track_caller]` when the generated body can panic through `expect`, so the panic
    /// reports the caller's line instead of the generated file
    fn track_caller_if_panics(bodies: &[&TokenStream]) -> TokenStream {
        if bodies.iter().any(|body| body.to_string().contains(". expect (")) {
            quote! { #[track_caller] }
        } else {
            quote! {}
        }
    }

    /// Registration statements that clone each `Handler` argument into `result`'s
    /// dependencies. Used by constructors: the created C struct stores the callback and
    /// clientd pointers, so the Handler must stay alive until the C resource is closed.
    fn handler_dependency_registrations(arguments: &[Arg]) -> Vec<TokenStream> {
        arguments
            .iter()
            .filter_map(|arg| {
                if let ArgProcessing::Handler(_) = &arg.processing {
                    if !arg.is_mut_pointer() {
                        let name = arg.as_ident();
                        return Some(quote! {
                            if let Some(__handler) = #name {
                                if let Some(__inner) = result.inner.as_owned() {
                                    __inner.add_dependency(__handler.clone());
                                }
                            }
                        });
                    }
                }
                None
            })
            .collect()
    }

    /// The same getter for `impl<'a> Ref<'a, Parent>`, returning `Ref<'a, Child>`. Through `Deref`
    /// the child would borrow the `Ref` itself, so `archive.get_archive_context().get_aeron()`
    /// would be tied to a temporary and fail with E0716
    fn ref_getter(fn_name: &Ident, child: &Ident, docs: &[TokenStream], nullable: bool, panics: bool) -> TokenStream {
        let track_caller = if panics {
            quote! { #[track_caller] }
        } else {
            quote! {}
        };
        if nullable {
            return quote! {
                #[inline]
                #[must_use]
                #track_caller
                #(#docs)*
                pub fn #fn_name(&self) -> Option<Ref<'a, #child>> {
                    self.value.#fn_name().map(|view| Ref::new(view.value))
                }
            };
        }
        quote! {
            #[inline]
            #[must_use]
            #track_caller
            #(#docs)*
            pub fn #fn_name(&self) -> Ref<'a, #child> {
                // `self` is valid for `'a`, so its child is too
                Ref::new(self.value.#fn_name().value)
            }
        }
    }

    /// `publication.stream_id()` for `publication.get_constants()?.stream_id()`, one per scalar
    /// constants field. Text and pointer fields borrow the constants copy, so they are left out.
    /// A name the wrapper already has, from C or `aeron_custom.rs`, wins
    fn add_constants_forwarders(
        &self,
        wrappers: &BTreeMap<String, CWrapper>,
        method: &Method,
        additional_methods: &mut Vec<TokenStream>,
    ) {
        if method.struct_method_name != "constants" || method.arguments.len() != 2 {
            return;
        }
        let constants_arg = ReturnType::new(method.arguments[1].clone(), wrappers.clone());
        let Some(constants) = constants_arg.pointee_wrapper() else {
            return;
        };
        for field in constants.fields.iter().filter(|f| {
            !f.name.starts_with('_')
                && f.is_primitive()
                && !f.is_any_pointer()
                && !self.skipped_methods.contains(&f.name)
                && !self.methods.iter().any(|m| m.struct_method_name == f.name)
                && !self.fields.iter().any(|own| own.name == f.name)
        }) {
            let name = format_ident!("{}", field.name);
            let value_type = ReturnType::new(field.clone(), wrappers.clone()).get_new_return_type(false, false);
            let doc = format!(
                "`get_constants()?.{}()`. Call `get_constants()` once to read several fields",
                field.name
            );
            additional_methods.push(quote! {
                #[inline]
                #[doc = #doc]
                pub fn #name(&self) -> Result<#value_type, AeronCError> {
                    Ok(self.get_constants()?.#name())
                }
            });
        }
    }

    fn add_getter_instead_of_mut_arg_if_applicable(
        wrappers: &BTreeMap<String, CWrapper>,
        method: &Method,
        fn_name: &Ident,
        where_clause: &TokenStream,
        possible_self: &TokenStream,
        method_docs: &Vec<TokenStream>,
        additional_methods: &mut Vec<TokenStream>,
    ) {
        if POINTER_OUTPUT_METHODS.contains(&method.struct_method_name.as_str()) && method.arguments.len() == 2 {
            let rt = ReturnType::new(method.arguments[1].clone(), wrappers.clone());
            let class = format_ident!("{}", rt.pointee_wrapper().expect("native constants output").class_name);
            let native = format_ident!("{}", rt.pointee_wrapper().unwrap().type_name);
            let borrows_parent = rt.pointee_wrapper().unwrap().borrows_native_storage(wrappers);
            let (return_type, output_storage) = if borrows_parent {
                (
                    quote! { #class<ScopedInline<'_, #native>> },
                    quote! { ScopedInline {
                        value: Inline::new(unsafe { value.assume_init() }), _owner: std::marker::PhantomData,
                    } },
                )
            } else {
                (
                    quote! { #class<Inline<#native>> },
                    quote! { Inline::new(unsafe { value.assume_init() }) },
                )
            };
            let getter_method = if method.struct_method_name == "get_buffers" {
                format_ident!("get_buffers")
            } else {
                format_ident!("get_{}", fn_name)
            };
            let c_fn = format_ident!("{}", method.fn_name);
            let method_docs = method_docs
                .iter()
                .cloned()
                .take_while(|t| !t.to_string().contains(" Parameter"))
                .collect_vec();
            additional_methods.push(quote! {
                #[inline]
                #(#method_docs)*
                pub fn #getter_method #where_clause(#possible_self) -> Result<#return_type, AeronCError> {
                    let mut value = std::mem::MaybeUninit::uninit();
                    // SAFETY: Aeron's constants/values functions fill every field when they
                    // return 0 and write nothing otherwise, so no zeroing is needed
                    let code = unsafe { #c_fn(self.get_inner(), value.as_mut_ptr()) };
                    if code < 0 {
                        return Err(AeronCError::from_code(code));
                    }
                    Ok(#class::from_resource(#output_storage))
                }
            });
        }
    }

    fn add_mut_string_methods_if_applicable(
        method: &Method,
        fn_name: &Ident,
        uses_self: bool,
        method_docs: &Vec<TokenStream>,
        additional_methods: &mut Vec<TokenStream>,
    ) {
        if method.arguments.len() == 3 && uses_self {
            let method_docs = method_docs.clone();
            let into_method = format_ident!("{}_into", fn_name);
            if method.arguments[1].is_mut_c_string() && method.arguments[2].is_usize() {
                let string_method = format_ident!("{}_as_string", fn_name);
                additional_methods.push(quote! {
    #[inline]
    #(#method_docs)*
    pub fn #string_method(
        &self,
        max_length: usize,
    ) -> Result<String, AeronCError> {
        let mut result = String::with_capacity(max_length);
        self.#into_method(&mut result)?;
        Ok(result)
    }

    #[inline]
    #(#method_docs)*
    #[doc = "NOTE: allocation friendly method, the string capacity must be set as it will truncate string to capacity it will never grow the string. So if you pass String::new() it will write 0 chars"]
    pub fn #into_method(
        &self,
        dst_truncate_to_capacity: &mut String,
    ) -> Result<i32, AeronCError> {
        let capacity = dst_truncate_to_capacity.capacity();
        // SAFETY: the bytes are zeros (valid UTF-8) until truncated to a checked UTF-8 prefix.
        let vec = unsafe { dst_truncate_to_capacity.as_mut_vec() };
        // Zeroed so C never leaves uninitialised bytes behind, including when it fails without writing.
        vec.clear();
        vec.resize(capacity, 0);
        let result = self.#fn_name(&mut vec[..]);
        let len = match result {
            Ok(_) => {
                let len = vec.iter().position(|&b| b == 0).unwrap_or(capacity);
                // C truncation can split a multi-byte character.
                std::str::from_utf8(&vec[..len]).map_or_else(|e| e.valid_up_to(), |s| s.len())
            }
            Err(_) => 0,
        };
        vec.truncate(len);
        result
    }
                        });
            }
        }
    }

    /// Generate the fields / getters
    fn generate_fields(
        &self,
        cwrappers: &BTreeMap<String, CWrapper>,
        ref_getters: &mut Vec<TokenStream>,
    ) -> Vec<TokenStream> {
        self.fields
            .iter()
            .filter(|arg| {
                !arg.name.starts_with("_")
                    && !self.methods.iter().any(|m| m.struct_method_name.as_str() == arg.name)
                    && !CUSTOM_FIELD_GETTERS.contains(&(self.type_name.as_str(), arg.name.as_str()))
            })
            .map(|arg| {
                let field_name = &arg.name;
                let fn_name = Ident::new(field_name, proc_macro2::Span::call_site());

                let mut arg = arg.clone();
                // Return &str for mutable C strings.
                if arg.is_mut_c_string() {
                    arg.c_type = arg.c_type.replace(" mut ", " const ");
                }
                let mut rt = ReturnType::new(arg.clone(), cwrappers.clone());
                let mut return_type = rt.get_new_return_type(false, false);
                let handler = if let ArgProcessing::Handler(_) = &arg.processing {
                    true
                } else {
                    false
                };
                if return_type.is_empty() || handler {
                    rt = ReturnType::new(
                        Arg {
                            processing: ArgProcessing::Default,
                            ..arg.clone()
                        },
                        cwrappers.clone(),
                    );
                    return_type = rt.get_new_return_type(false, false);
                }
                let mut converter =
                    rt.handle_c_to_rs_return(quote! { unsafe { self.get_inner_ref() }.#fn_name }, false, true);
                if self.type_name == "aeron_agent_runner_t" && field_name == "running" {
                    converter = quote! {
                        // SAFETY: this initialized C bool contains 0 or 1. Aeron
                        // publishes it with release ordering. Load its byte atomically
                        // without borrowing the whole runner or converting the value.
                        unsafe {
                            let running = std::sync::atomic::AtomicU8::from_ptr(
                                std::ptr::addr_of_mut!((*self.get_inner()).running).cast::<u8>()
                            ).load(std::sync::atomic::Ordering::Acquire);
                            std::mem::transmute::<u8, bool>(running)
                        }
                    };
                }
                if let crate::string_views::StringView::BorrowedWithLength(length) =
                    crate::string_views::field(self, field_name)
                {
                    let length = format_ident!("{}", length);
                    converter = quote! {
                        {
                            let value = unsafe { self.get_inner_ref() };
                            if value.#fn_name.is_null() { "" } else {
                                // SAFETY: this field's native length bounds its live bytes.
                                let bytes = unsafe {
                                    std::slice::from_raw_parts(value.#fn_name.cast::<u8>(), value.#length as usize)
                                };
                                std::str::from_utf8(bytes).unwrap_or("")
                            }
                        }
                    };
                }
                if let Some(child) = cwrappers
                    .get(&rt.original.c_type)
                    .filter(|w| w.borrows_native_storage(cwrappers))
                {
                    let class = format_ident!("{}", child.class_name);
                    let native = format_ident!("{}", child.type_name);
                    return_type = quote! { #class<ScopedInline<'_, #native>> };
                    converter = quote! { #class::from_resource(ScopedInline {
                        value: Inline::new(unsafe { self.get_inner_ref() }.#fn_name),
                        _owner: std::marker::PhantomData,
                    }) };
                }
                let (field_unsafety, field_safety_docs) = match return_type.to_string().as_str() {
                    "& str"
                        if crate::string_views::field(self, field_name)
                            == crate::string_views::StringView::Unchecked =>
                    {
                        (
                            quote! { unsafe },
                            quote! {
                                /// # Safety
                                /// The text points into native storage. Use it straight away, and do not
                                /// hold it across a setter, poll or other call that can change it.
                            },
                        )
                    }
                    "& mut [u8]" => (
                        quote! { unsafe },
                        quote! {
                            /// # Safety
                            /// The stored pointer must be valid for the returned slice, and no
                            /// other Rust or native access may overlap its mutable borrow.
                        },
                    ),
                    "& [u8]" => (
                        quote! { unsafe },
                        quote! {
                            /// # Safety
                            /// The stored pointer must be valid for the returned slice, and its
                            /// bytes must not be mutated while the slice is borrowed.
                        },
                    ),
                    _ => (quote! {}, quote! {}),
                };
                let owned_field =
                    if return_type.to_string() == "& str" && crate::string_views::has_owned_field(&self.type_name) {
                        let owned_name = format_ident!("{}_owned", fn_name);
                        let owned_converter = ReturnType::new(arg.clone(), cwrappers.clone())
                            .with_owned_strings()
                            .handle_c_to_rs_return(quote! { unsafe { self.get_inner_ref() }.#fn_name }, false, true);
                        quote! {
                            /// Copy the native text into an owned string. This allocates for nonempty text.
                            #[inline]
                            #[must_use]
                            pub fn #owned_name(&self) -> String { #owned_converter }
                        }
                    } else {
                        quote! {}
                    };
                if let Some(child) = rt.pointee_wrapper().map(|w| format_ident!("{}", w.class_name)) {
                    return_type = quote! { Ref<'_, #child> };
                    converter = quote! { Ref::new(#converter) };
                    let panics = !Self::track_caller_if_panics(&[&converter]).is_empty();
                    ref_getters.push(Self::ref_getter(&fn_name, &child, &[], false, panics));
                }

                let track_caller = Self::track_caller_if_panics(&[&converter]);
                quote! {
                    #[inline]
                    #[must_use]
                    #track_caller
                    #field_safety_docs
                    pub #field_unsafety fn #fn_name(&self) -> #return_type {
                        #converter
                    }
                    #owned_field
                }
            })
            .filter(|t| !t.is_empty())
            .collect()
    }

    /// Generate the constructor for the struct
    fn generate_constructor(&self, wrappers: &BTreeMap<String, CWrapper>) -> Vec<TokenStream> {
        let native_type = format_ident!("{}", self.type_name);
        let constructors = self
            .methods
            .iter()
            .filter(|m| m.arguments.iter().any(|arg| arg.is_double_mut_pointer()))
            .map(|method| {
                let init_fn = format_ident!("{}", method.fn_name);

                // `_add_destination` and `_remove_destination[_by_id]` are constructor-style
                // (double-mut-pointer output) with no C close counterpart. Each is a standalone
                // async op polled to completion via `..._destination_poll` and must not invoke
                // its sibling on drop. Treat them as having a close method (themselves, unused,
                // as `cleanup_tokens` forces `None` for `_destination` methods) so they still
                // get a constructor
                let is_destination_add_or_remove = method.fn_name.contains("_async_add_destination")
                    || method.fn_name.contains("_async_remove_destination");
                let close_method = self.find_close_method(method).or(if is_destination_add_or_remove {
                    Some(method)
                } else {
                    None
                });
                let found_close = is_destination_add_or_remove
                    || (close_method.is_some()
                        && close_method.unwrap().return_type.is_c_raw_int()
                        && close_method.unwrap() != method
                        && close_method
                            .unwrap()
                            .arguments
                            .iter()
                            .skip(1)
                            .all(|a| method.arguments.iter().any(|a2| a.name == a2.name)));
                if found_close {
                    let close_fn = format_ident!("{}", close_method.unwrap().fn_name);
                    let init_args: Vec<TokenStream> = method
                        .arguments
                        .iter()
                        .enumerate()
                        .map(|(idx, arg)| {
                            if idx == 0 {
                                quote! { ctx_field }
                            } else {
                                let arg_name = arg.as_ident();
                                quote! { #arg_name }
                            }
                        })
                        .filter(|t| !t.is_empty())
                        .collect();
                    let close_args: Vec<TokenStream> = close_method
                        .unwrap_or(method)
                        .arguments
                        .iter()
                        .enumerate()
                        .map(|(idx, arg)| {
                            if idx == 0 {
                                if arg.is_double_mut_pointer() {
                                    quote! { ctx_field }
                                } else {
                                    quote! { *ctx_field }
                                }
                            } else {
                                let arg_name = arg.as_ident();
                                quote! { #arg_name.into() }
                            }
                        })
                        .filter(|t| !t.is_empty())
                        .collect();
                    let lets: Vec<TokenStream> = Self::lets_for_copying_arguments(wrappers, &method.arguments, true, !owner_is_shared(&self.type_name));
                    let parent_deps: Vec<TokenStream> = method.arguments.iter()
                        .filter(|arg| arg.is_single_mut_pointer() && wrappers.contains_key(arg.base_type()))
                        .map(|arg| { let copy = format_ident!("{}_copy", arg.name);
                            quote! { result.inner.add_handle_dependency(#copy); }
                        }).collect();
                    let freeze_context = if ["aeron_t", "aeron_driver_t"].contains(&self.type_name.as_str()) {
                        quote! { let __context_use = context.inner.as_owned().ok_or_else(|| AeronCError::with_message(-1, "client requires an owned context"))?.reserve_context()?; }
                    } else { quote! {} };


                    let commit_context = if self.type_name == "aeron_t" { quote! {
                        __context_use.commit();
                    } } else if self.type_name == "aeron_driver_t" { quote! { __context_use.commit(); } } else { quote! {} };
                    // Parent ownership lives in ManagedCResource's dependencies. Keeping
                    // another parent wrapper here duplicates ownership and enlarges views.

                    let new_args: Vec<TokenStream> = method
                        .arguments
                        .iter()
                        .enumerate()
                        .filter_map(|(_idx, arg)| {
                            if arg.is_double_mut_pointer() {
                                None
                            } else {
                                let arg_name = arg.as_ident();
                                let arg_type =
                                    ReturnType::new(arg.clone(), wrappers.clone()).with_local_callbacks(!owner_is_shared(&self.type_name)).get_new_return_type(false, true);
                                if arg_type.clone().into_token_stream().is_empty() {
                                    None
                                } else {
                                    Some(quote! { #arg_name: #arg_type })
                                }
                            }
                        })
                        .filter(|t| !t.is_empty())
                        .collect();
                    let (constructor_unsafety, constructor_safety_docs) = if new_args.iter().any(|arg| {
                        matches!(syn::parse2::<syn::FnArg>(arg.clone()),
                            Ok(syn::FnArg::Typed(arg)) if matches!(*arg.ty, Type::Ptr(_)))
                    }) {
                        (quote! { unsafe }, quote! {
                            /// # Safety
                            /// Keep raw pointer targets valid for every native use,
                            /// including cleanup of the returned resource.
                        })
                    } else {
                        (quote! {}, quote! {})
                    };

                    let fn_name = format_ident!(
                        "{}",
                        method
                            .struct_method_name
                            .replace("init", "new")
                            .replace("create", "new")
                    );
                    // A hand-written constructor of the same name wraps this one, so keep it crate-private
                    let (fn_name, visibility) = if self.skipped_methods.contains(&fn_name.to_string()) {
                        (format_ident!("{}_native", fn_name), quote! { pub(crate) })
                    } else {
                        (fn_name, quote! { pub })
                    };

                    let generic_types: Vec<TokenStream> = method
                        .arguments
                        .iter()
                        .flat_map(|arg| {
                            ReturnType::new(arg.clone(), wrappers.clone()).with_local_callbacks(!owner_is_shared(&self.type_name))
                                .method_generics_for_where(true, owner_is_shared(&self.type_name))
                                .into_iter()
                        })
                        .collect_vec();
                    let where_clause = if generic_types.is_empty() {
                        quote! {}
                    } else {
                        quote! { <#(#generic_types),*> }
                    };

                    let method_docs: Vec<TokenStream> = get_docs(&method.docs, wrappers, Some(&new_args));

                    // The C struct created here stores every callback/clientd pair it is
                    // given, so clone each
                    // Handler into the new resource's dependencies to keep it alive until
                    // the C resource is closed. `lets` shadows each Handler argument with
                    // its C callback pointer, so take the Handler under a fresh name first.
                    let (handler_keeps, handler_deps): (Vec<TokenStream>, Vec<TokenStream>) = method
                        .arguments
                        .iter()
                        .filter(|arg| matches!(arg.processing, ArgProcessing::Handler(_)) && !arg.is_mut_pointer())
                        .map(|arg| {
                            let name = arg.as_ident();
                            let keep = format_ident!("__handler_{}", arg.name);
                            (
                                quote! { let #keep = #name; },
                                quote! {
                                    if let Some(__handler) = #keep {
                                        if let Some(__inner) = result.inner.as_owned() {
                                            __inner.add_dependency(Handler::clone(__handler));
                                        }
                                    }
                                },
                            )
                        })
                        .unzip();

                    // Generate logging expression token stream (will be evaluated in closure)
                    let init_log_expr_tokens = Self::generate_arg_logging(&method.arguments, &init_args);
                    // Generate logging for close method arguments
                    let close_log_expr_tokens = if let Some(close_m) = close_method {
                        Self::generate_arg_logging(&close_m.arguments, &close_args)
                    } else {
                        quote! { "" }
                    };

                    // `async_add_destination` handles must not run `async_remove_destination` on
                    // drop. Removing a destination is a separate, destructive operation, and the C
                    // client frees the async struct itself once its poll completes
                    let cleanup_tokens = if method.fn_name.contains("_destination") {
                        quote! { None }
                    } else if method.fn_name == "aeron_archive_proxy_create" {
                        quote! { Some(Box::new(|ctx_field| unsafe {
                            // Rust owns the publication separately. Native delete also
                            // closes it unless we detach it before deleting the proxy.
                            (**ctx_field).exclusive_publication = std::ptr::null_mut();
                            aeron_archive_proxy_delete(*ctx_field)
                        })) }
                    } else {
                        quote! {
                            Some(Box::new(move |ctx_field| unsafe {
                                #[cfg(feature = "log-c-bindings")]
                                {
                                    let log_args = #close_log_expr_tokens;
                                    log::info!("{}({})", stringify!(#close_fn), log_args);
                                }
                                #close_fn(#(#close_args),*)
                            }))
                        }
                    };

                    quote! {
                        #[inline]
                        #(#method_docs)*
                        #constructor_safety_docs
                        #visibility #constructor_unsafety fn #fn_name #where_clause(#(#new_args),*) -> Result<Self, AeronCError> {
                            #freeze_context
                            #(#handler_keeps)*
                            #(#lets)*
                            let resource_constructor = ManagedCResource::<#native_type>::new(
                                move |ctx_field| unsafe {
                                    #[cfg(feature = "log-c-bindings")]
                                    {
                                        let log_args = #init_log_expr_tokens;
                                        log::info!("{}({})", stringify!(#init_fn), log_args);
                                    }
                                    #init_fn(#(#init_args),*)
                                },
                                #cleanup_tokens,
                                false,
                            )?;

                            #commit_context
                            let result = Self {
                                inner: CResource::new(ResourceRef::new(resource_constructor)).into(),
                            };
                            #(#parent_deps)*
                            #(#handler_deps)*
                            Ok(result)
                        }
                    }
                } else {
                    quote! {}
                }
            })
            .collect_vec();

        let class_name = format_ident!("{}", self.class_name);
        let no_constructor = constructors.iter().map(|x| x.to_string()).join("").trim().is_empty();
        if no_constructor && handle_has_stable_pointer(&self.type_name) {
            return constructors;
        }
        if no_constructor {
            let type_name = format_ident!("{}", self.type_name);
            let (zeroed_unsafety, zeroed_safety_docs) =
                if !self.zeroed_is_valid(wrappers) && self.type_name != "aeron_buffer_claim_t" {
                    (
                        quote! { unsafe },
                        quote! {
                            /// # Safety
                            /// For handles with native methods, initialise the native state
                            /// before calling those methods. Zeroed storage is not a connection.
                        },
                    )
                } else {
                    (quote! {}, quote! {})
                };
            let zeroed_impl = if self.has_invalid_zero() {
                quote! {}
            } else {
                quote! {
                    #[inline]
                    /// Creates zeroed native storage on the heap.
                    #zeroed_safety_docs
                    pub #zeroed_unsafety fn new_zeroed_on_heap() -> Self {
                        let resource = ManagedCResource::<#native_type>::new(
                            move |ctx_field| {
                                #[cfg(feature = "extra-logging")]
                                log::info!("creating zeroed empty resource on heap {}", stringify!(#type_name));
                                let inst: #type_name = unsafe { std::mem::zeroed() };
                                let inner_ptr: *mut #type_name = Box::into_raw(Box::new(inst));
                                unsafe { *ctx_field = inner_ptr };
                                0
                            },
                            None,
                            true,
                        ).unwrap();

                        Self {
                            inner: CResource::new(ResourceRef::new(resource)).into(),
                        }
                    }

                    #[inline]
                    /// Creates zeroed native storage inline.
                    #zeroed_safety_docs
                    pub #zeroed_unsafety fn new_zeroed_on_stack() -> #class_name<Inline<#type_name>> {
                        #[cfg(feature = "extra-logging")]
                        log::debug!("creating zeroed empty resource on stack {}", stringify!(#type_name));

                        #class_name {
                            inner: Inline::new(unsafe { std::mem::zeroed() }),
                        }
                    }
                }
            };
            if self.has_default_method() {
                let type_name = format_ident!("{}", self.type_name);
                let new_args: Vec<TokenStream> = self
                    .fields
                    .iter()
                    .filter_map(|arg| {
                        let arg_name = arg.as_ident();
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                            .with_local_callbacks(!owner_is_shared(&self.type_name))
                            .get_new_return_type(false, true);
                        if arg_type.is_empty() {
                            None
                        } else {
                            Some(quote! { #arg_name: #arg_type })
                        }
                    })
                    .filter(|t| !t.is_empty())
                    .collect();
                let init_args: Vec<TokenStream> = self
                    .fields
                    .iter()
                    .map(|arg| {
                        let arg_name = arg.as_ident();
                        let value = ReturnType::new(arg.clone(), wrappers.clone())
                            .with_local_callbacks(!owner_is_shared(&self.type_name))
                            .handle_rs_to_c_return(quote! { #arg_name }, true);
                        quote! { #value }
                    })
                    .filter(|t| !t.is_empty())
                    .collect();

                let generic_types: Vec<TokenStream> = self
                    .fields
                    .iter()
                    .flat_map(|arg| {
                        ReturnType::new(arg.clone(), wrappers.clone())
                            .with_local_callbacks(!owner_is_shared(&self.type_name))
                            .method_generics_for_where(true, owner_is_shared(&self.type_name))
                            .into_iter()
                    })
                    .collect_vec();
                let where_clause = if generic_types.is_empty() {
                    quote! {}
                } else {
                    quote! { <#(#generic_types),*> }
                };

                let cloned_fields = self
                    .fields
                    .iter()
                    .filter(|a| a.processing == ArgProcessing::Default)
                    .cloned()
                    .collect_vec();
                let lets: Vec<TokenStream> = Self::lets_for_copying_arguments(
                    wrappers,
                    &cloned_fields,
                    false,
                    !owner_is_shared(&self.type_name),
                );

                // The C struct stores these callback/clientd fields directly, so keep the
                // Handler values alive alongside the resource.
                let field_handler_deps: Vec<TokenStream> = Self::handler_dependency_registrations(&self.fields);
                let (constructor_unsafety, constructor_safety_docs) = if self.fields.iter().any(|field| {
                    !matches!(field.processing, ArgProcessing::Handler(_))
                        && self.field_borrows_native_storage(field, wrappers)
                }) {
                    (
                        quote! { unsafe },
                        quote! {
                            /// # Safety
                            /// Pointer fields must refer to valid storage for every use of this struct.
                        },
                    )
                } else {
                    (quote! {}, quote! {})
                };

                vec![quote! {
                    #[inline]
                    #constructor_safety_docs
                    pub #constructor_unsafety fn new #where_clause(#(#new_args),*) -> Result<Self, AeronCError> {
                        #(#lets)*
                        // no constructor in c bindings
                        let r_constructor = ManagedCResource::<#native_type>::new(
                            move |ctx_field| {
                                let inst = #type_name { #(#init_args),* };
                                let inner_ptr: *mut #type_name = Box::into_raw(Box::new(inst));
                                unsafe { *ctx_field = inner_ptr };
                                0
                            },
                            None,
                            true,
                        )?;

                        let result = Self {
                            inner: CResource::new(ResourceRef::new(r_constructor)).into(),
                        };
                        #(#field_handler_deps)*
                        Ok(result)
                    }

                    #zeroed_impl
                }]
            } else {
                vec![zeroed_impl]
            }
        } else {
            constructors
        }
    }

    fn lets_for_copying_arguments(
        wrappers: &BTreeMap<String, CWrapper>,
        arguments: &Vec<Arg>,
        include_let_statements: bool,
        local_callbacks: bool,
    ) -> Vec<TokenStream> {
        arguments
            .iter()
            .enumerate()
            .filter_map(|(_idx, arg)| {
                if arg.is_double_mut_pointer() {
                    None
                } else {
                    let arg_name = arg.as_ident();
                    let rtype = arg.as_type();

                    let fields = if arg.is_single_mut_pointer() && wrappers.contains_key(arg.base_type()) {
                        let arg_copy = format_ident!("{}_copy", arg.name);
                        if handle_is_archive_context(arg.base_type()) {
                            let class = format_ident!("{}", wrappers[arg.base_type()].class_name);
                            quote! {
                                let #arg_copy = &#class::from_resource(#arg_name.resource().clone());
                            }
                        } else {
                            // A borrow is enough to retain the owner, and single-owner handles have no Clone
                            quote! { let #arg_copy = #arg_name; }
                        }
                    } else {
                        quote! {}
                    };

                    let return_type =
                        ReturnType::new(arg.clone(), wrappers.clone()).with_local_callbacks(local_callbacks);

                    if let ArgProcessing::StringWithLength(_args) | ArgProcessing::ByteArrayWithLength(_args) =
                        &return_type.original.processing
                    {
                        return None;
                    }
                    if let ArgProcessing::Handler(args) = &return_type.original.processing {
                        let arg1 = args[0].as_ident();
                        let arg2 = args[1].as_ident();
                        let value = return_type.handle_rs_to_c_return(quote! { #arg_name }, false);

                        if value.is_empty() {
                            return None;
                        }

                        if include_let_statements {
                            return Some(quote! { #fields let (#arg1, #arg2)= (#value); });
                        } else {
                            return Some(fields);
                        }
                    }

                    let value = return_type.handle_rs_to_c_return(quote! { #arg_name }, false);
                    if value.is_empty() {
                        None
                    } else {
                        if include_let_statements {
                            Some(quote! { #fields let #arg_name: #rtype = #value; })
                        } else {
                            return Some(fields);
                        }
                    }
                }
            })
            .filter(|t| !t.is_empty())
            .collect()
    }

    fn find_close_method(&self, method: &Method) -> Option<&Method> {
        let mut close_method = None;

        // must have init, create, add or remove method name. `_remove` is needed so
        // that e.g. `aeron_publication_async_remove_destination`
        if ["_init", "_create", "_add", "_remove"]
            .iter()
            .all(|find| !method.fn_name.contains(find))
        {
            return None;
        }

        for name in ["_destroy", "_delete"] {
            let mut candidate = method.fn_name.replace("_init", "_close").replace("_create", name);
            if method.fn_name.contains("_remove_") {
                // A `_remove_` method (e.g. `..._async_remove_destination`) looks for its paired
                // `_add_` method so `found_close` is satisfied too. `..._remove_destination_by_id`
                // has no `_add_..._by_id` counterpart, so drop the `_by_id` suffix before pairing.
                // This only gates eligibility. `cleanup_tokens` forces `None` for every
                // `_destination` method whatever this resolves to
                candidate = candidate.trim_end_matches("_by_id").replace("_remove_", "_add_");
            } else {
                candidate = candidate.replace("_add_", "_remove_");
            }
            let close_fn = format_ident!("{}", candidate);
            let method = self.methods.iter().find(|m| close_fn.to_string().contains(&m.fn_name));
            if method.is_some() {
                close_method = method;
                break;
            }
        }
        close_method
    }

    fn has_default_method(&self) -> bool {
        // A type with a C constructor has no `Default`. Most constructors return through a double
        // pointer, but `aeron_uri_string_builder_init_*` fill a single pointer instead
        let no_init_method = !self.methods.iter().any(|m| {
            m.arguments
                .iter()
                .any(|arg| arg.is_double_mut_pointer() || (arg.is_single_mut_pointer() && m.fn_name.contains("_init_")))
        });

        no_init_method && !self.fields.iter().any(|arg| arg.name.starts_with("_")) && !self.fields.is_empty()
    }

    /// Only generated Rust allocations qualify. Native constructors, cleanup,
    /// and consuming polls must continue observing the owner's shared pointer.
    fn caches_owned_value_pointer(&self) -> bool {
        !self.opaque
            && self.has_default_method()
            && !self.methods.iter().any(|method| {
                method.fn_name.contains("_init")
                    || ["_close", "_delete", "_destroy", "_free"]
                        .iter()
                        .any(|suffix| method.fn_name.ends_with(suffix))
            })
            && !handle_is_consumed_by_poll(&self.type_name)
            && !handle_has_stable_pointer(&self.type_name)
    }

    fn generate_allocation_test(&self, wrappers: &BTreeMap<String, CWrapper>) -> TokenStream {
        let class_name = format_ident!("{}", self.class_name);

        let has_c_constructor = self
            .methods
            .iter()
            .any(|m| m.arguments.iter().any(|arg| arg.is_double_mut_pointer()));
        let has_empty_new_constructor = self.methods.iter().any(|m| {
            m.fn_name.contains("_init_")
                && m.arguments.iter().any(|arg| arg.is_double_mut_pointer())
                && m.arguments.len() == 1
        });

        let mut tests = vec![];

        if !has_c_constructor && !handle_has_stable_pointer(&self.type_name) && !self.has_invalid_zero() {
            tests.push(quote! {
                #[test]
                #[file_serial(global)]
                fn test_new_on_stack() {
                    crate::test_alloc::assert_no_allocation(|| {
                        for _ in 0..100 {
                            let _ = unsafe { #class_name::new_zeroed_on_stack() };
                        }
                    });
                }
            });
        }

        if has_empty_new_constructor {
            tests.push(quote! {
                #[test]
                #[file_serial(global)]
                fn test_new() {
                    crate::test_alloc::assert_no_allocation(|| {
                        for _ in 0..100 {
                            let _ = #class_name::new();
                        }
                    });
                }
            });
        }

        if self.has_default_method() && (self.zeroed_is_valid(wrappers) || self.type_name == "aeron_buffer_claim_t") {
            tests.push(quote! {
                #[test]
                #[file_serial(global)]
                fn test_default() {
                    crate::test_alloc::assert_no_allocation(|| {
                        for _ in 0..100 {
                            let _ = #class_name::default();
                        }
                    });
                }
            });
        }

        let mod_name = format_ident!("{}_allocation_tests", self.type_name);

        if tests.is_empty() {
            quote! {}
        } else {
            quote! {
                #[cfg(test)]
                mod #mod_name {
                    use super::*;
                    use serial_test::file_serial;
                    #(#tests)*
                }
            }
        }
    }
}

#[cfg(test)]
mod pointer_storage_tests {
    use super::*;

    #[test]
    fn only_rust_owned_values_cache_their_pointer() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let bindings = crate::parse_bindings(&path);
        assert!(bindings.wrappers["aeron_buffer_claim_t"].caches_owned_value_pointer());
        for name in [
            "aeron_context_t",
            "aeron_t",
            "aeron_async_add_publication_t",
            "aeron_publication_t",
        ] {
            assert!(!bindings.wrappers[name].caches_owned_value_pointer(), "{name}");
        }
        // An upstream constructor or destructor must automatically turn caching off.
        for name in [
            "aeron_value_init",
            "aeron_value_close",
            "aeron_value_delete",
            "aeron_value_destroy",
            "aeron_value_free",
        ] {
            let mut value = bindings.wrappers["aeron_buffer_claim_t"].clone();
            value.methods.push(Method {
                fn_name: name.into(),
                struct_method_name: name.into(),
                return_type: Arg {
                    name: String::new(),
                    c_type: "i32".into(),
                    processing: ArgProcessing::Default,
                },
                arguments: vec![],
                docs: BTreeSet::new(),
            });
            assert!(!value.caches_owned_value_pointer(), "{name}");
        }
    }
}

#[cfg(test)]
mod parse_custom_methods_tests {
    use super::parse_custom_methods;
    use std::collections::BTreeSet;

    #[test]
    fn extracts_method_names_from_inherent_impls() {
        let src = r#"
            impl AeronPublication {
                pub fn offer_result(&self) -> i64 { 0 }
                pub fn status(&self) -> u8 { 0 }
            }
            impl AeronSubscription {
                pub fn status(&self) -> u8 { 0 }
            }
        "#;
        let map = parse_custom_methods(src);
        assert_eq!(
            map.get("AeronPublication"),
            Some(
                &["offer_result", "status"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<BTreeSet<_>>()
            )
        );
        assert!(map.get("AeronSubscription").map_or(false, |s| s.contains("status")));
    }

    #[test]
    fn ignores_trait_impls() {
        // `impl Display for X` and `impl<T> Trait for Y` must not be treated as
        // inherent method sources (they'd add bogus skip entries).
        let src = r#"
            impl std::fmt::Display for AeronCError {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { Ok(()) }
            }
        "#;
        let map = parse_custom_methods(src);
        assert!(
            !map.contains_key("AeronCError"),
            "trait impls must not appear in the skip-list"
        );
    }

    #[test]
    #[should_panic(expected = "failed to parse aeron_custom.rs")]
    fn panics_loudly_on_malformed_input() {
        // An empty skip-list would let the generator emit duplicate methods.
        let _ = parse_custom_methods("impl { not valid rust }}}");
    }

    #[test]
    fn empty_input_yields_empty_map() {
        assert!(parse_custom_methods("").is_empty());
    }

    #[test]
    fn self_type_keyed_by_plain_token_stream() {
        let src = r#"
            impl AeronPublication { pub fn offer_result(&self) -> i64 { 0 } }
        "#;
        let map = parse_custom_methods(src);
        // Exact plain-name key. No surrounding whitespace, no path qualifiers.
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("AeronPublication"));
    }
}

#[cfg(test)]
mod arg_classification_tests {
    use super::{Arg, ArgProcessing};

    fn arg(c_type: &str) -> Arg {
        Arg {
            name: "x".to_string(),
            c_type: c_type.to_string(),
            processing: ArgProcessing::Default,
        }
    }

    #[test]
    fn classifies_c_strings() {
        assert!(arg("* const :: std :: os :: raw :: c_char").is_c_string());
        assert!(arg("* const :: std :: os :: raw :: c_char").is_c_string_any());
        assert!(arg("* mut :: std :: os :: raw :: c_char").is_mut_c_string());
        assert!(arg("* mut :: std :: os :: raw :: c_char").is_c_string_any());
        // Not a C string:
        assert!(!arg("* const u8").is_c_string());
        assert!(!arg("i32").is_c_string_any());
    }

    #[test]
    fn classifies_byte_arrays() {
        assert!(arg("* const u8").is_byte_array());
        assert!(arg("* mut u8").is_byte_array());
        assert!(arg("* mut u8").is_mut_byte_array());
        assert!(!arg("* const u8").is_mut_byte_array());
        assert!(!arg("* const :: std :: os :: raw :: c_char").is_byte_array());
    }

    #[test]
    fn classifies_raw_int_return() {
        assert!(arg(":: std :: os :: raw :: c_int").is_c_raw_int());
        assert!(!arg("i32").is_c_raw_int());
    }

    #[test]
    fn classifies_void_pointer() {
        assert!(arg("* mut :: std :: os :: raw :: c_void").is_c_void());
        assert!(!arg("* mut u8").is_c_void());
    }

    #[test]
    fn classifies_mut_pointers_single_vs_double() {
        // Single `*mut`:
        assert!(arg("* mut i32").is_mut_pointer());
        assert!(arg("* mut i32").is_single_mut_pointer());
        assert!(!arg("* mut i32").is_double_mut_pointer());
        // Double `*mut *mut` (output param / handle out):
        assert!(arg("* mut * mut aeron_foo_t").is_mut_pointer());
        assert!(arg("* mut * mut aeron_foo_t").is_double_mut_pointer());
        assert!(!arg("* mut * mut aeron_foo_t").is_single_mut_pointer());
        // `*const` is not a mut pointer:
        assert!(!arg("* const i32").is_mut_pointer());
    }

    #[test]
    fn classifies_primitives() {
        for ty in [
            "i64", "u64", "f32", "f64", "i32", "i16", "u32", "u16", "bool", "usize", "isize",
        ] {
            assert!(arg(ty).is_primitive(), "{ty} should be primitive");
        }
        // `is_primitive` is a suffix match, so a pointer to a primitive such as `* mut i32`
        // also returns true. The generator combines it with `is_single_mut_pointer`
        assert!(arg("* mut i32").is_primitive());
        // But a pointer to a non-primitive (a C struct) is not primitive:
        assert!(!arg("* mut aeron_foo_t").is_primitive());
    }

    #[test]
    fn classifies_any_pointer() {
        assert!(arg("* const i32").is_any_pointer());
        assert!(arg("* mut i32").is_any_pointer());
        assert!(!arg("i32").is_any_pointer());
    }
}

fn get_docs(
    docs: &BTreeSet<String>,
    wrappers: &BTreeMap<String, CWrapper>,
    arguments: Option<&Vec<TokenStream>>,
) -> Vec<TokenStream> {
    let mut first_param = true;
    let param_name = regex::Regex::new(r"@param\s+([A-Za-z0-9_]+)").unwrap();
    // A wrapper can drop C arguments (`clientd`, out pointers), so drop their `@param` lines
    // too. Continuation lines follow the decision for the `@param` above them
    let mut keep_param = true;
    docs.iter()
        .flat_map(|d| d.lines())
        .filter(|s| {
            let Some(arguments) = arguments else { return true };
            if let Some(name) = param_name.captures(s) {
                keep_param = arguments
                    .iter()
                    .any(|a| a.to_string().split_whitespace().find(|token| *token != "mut") == Some(&name[1]));
            } else if s.contains('@') {
                keep_param = true;
            }
            keep_param
        })
        .map(|doc| {
            let mut doc = doc.to_string();

            if first_param && doc.contains("@param") {
                doc = format!("# Parameters\n{}", doc);
                first_param = false;
            }

            if doc.contains("@param") {
                doc = regex::Regex::new("@param\\s+([^ ]+)")
                    .unwrap()
                    .replace(doc.as_str(), "\n - `$1`")
                    .to_string();
            }

            doc = doc
                .replace("@return", "\n# Return\n")
                .replace("<p>", "\n")
                .replace("</p>", "\n");

            // Bracketed IPv6 endpoints are examples, so rustdoc must not resolve them as links.
            if doc.contains('[') && doc.contains(':') {
                doc = regex::Regex::new(r"\[([0-9a-fA-F]*:[0-9a-fA-F:.]+)\](?::[0-9]+)?")
                    .unwrap()
                    .replace_all(&doc, "`$0`")
                    .into_owned();
            }

            doc = wrappers
                .values()
                .fold(doc, |acc, v| acc.replace(&v.type_name, &format!("`{}`", v.class_name)));

            if doc.contains("@deprecated") {
                quote! {
                    #[deprecated]
                    #[doc = #doc]
                }
            } else {
                quote! {
                    #[doc = #doc]
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod documentation_tests {
    use super::*;

    #[test]
    fn param_docs_for_dropped_c_arguments_are_removed_with_their_continuation_lines() {
        let docs = BTreeSet::from([[
            "Poll for messages.",
            "@param handler to which fragments are delivered.",
            "@param clientd to pass to the handler.",
            " continued clientd text.",
            "@param fragment_limit for one poll.",
            "@return the number of fragments.",
        ]
        .join("\n")]);
        let arguments = vec![
            quote! { handler: Option<&Handler<T>> },
            quote! { fragment_limit: usize },
        ];
        let rendered = get_docs(&docs, &BTreeMap::new(), Some(&arguments))
            .iter()
            .map(|doc| doc.to_string())
            .collect::<String>();
        assert!(rendered.contains("handler"));
        assert!(rendered.contains("fragment_limit"));
        assert!(rendered.contains("Return"));
        assert!(!rendered.contains("clientd"), "{rendered}");
    }

    #[test]
    fn ipv6_examples_render_as_code_and_rust_links_remain_links() {
        for (input, expected) in [
            (
                "E.g. [fe80::7552:c06e:6bf4:4160]:12345.",
                "E.g. `[fe80::7552:c06e:6bf4:4160]:12345`.",
            ),
            (
                "Connect to [::1]:9000 or [::ffff:127.0.0.1]:8000.",
                "Connect to `[::1]:9000` or `[::ffff:127.0.0.1]:8000`.",
            ),
            (
                "See [`Self::new`] and [Aeron::new].",
                "See [`Self::new`] and [Aeron::new].",
            ),
        ] {
            let docs = BTreeSet::from([input.to_string()]);
            let rendered = get_docs(&docs, &BTreeMap::new(), None);
            assert_eq!(rendered.len(), 1);
            assert_eq!(rendered[0].to_string(), quote!(#[doc = #expected]).to_string());
        }
    }
}

fn generate_stateless_handler_code(handler: &CHandler) -> TokenStream {
    let type_name_ident = format_ident!("{}", handler.type_name);
    let closure_type_name = format_ident!("{}Callback", snake_to_pascal_case(&handler.type_name));
    let logger_type_name = format_ident!("{}Logger", snake_to_pascal_case(&handler.type_name));
    let handle_method_name = format_ident!("handle_{}", &handler.type_name[..handler.type_name.len() - 2]);
    let closure_return_type = handler.return_type.as_type();

    let (fn_mut_args, trait_args): (Vec<TokenStream>, Vec<TokenStream>) = handler
        .args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            let return_type = ReturnType::new(arg.clone(), BTreeMap::new());
            let type_name = return_type.get_new_return_type(false, false);
            let arg_name = format_ident!("arg_{}", i);

            let fn_mut_arg = if arg.is_single_mut_pointer() && arg.is_primitive() {
                let owned_type: Type = parse_str(arg.base_type()).unwrap();
                quote! { #owned_type }
            } else {
                quote! { #type_name }
            };

            let trait_arg = if type_name.is_empty() {
                quote! {}
            } else if arg.is_single_mut_pointer() && arg.is_primitive() {
                let owned_type: Type = parse_str(arg.base_type()).unwrap();
                quote! { #arg_name: #owned_type }
            } else {
                quote! { #arg_name: #type_name }
            };

            (fn_mut_arg, trait_arg)
        })
        .filter(|(fn_mut, trait_)| !fn_mut.is_empty() && !trait_.is_empty())
        .unzip();

    let fn_mut_sig = quote! {
       FnMut(#(#fn_mut_args),*) -> #closure_return_type
    };

    quote! {
        pub trait #closure_type_name {
            fn #handle_method_name(&mut self, #(#trait_args),*) -> #closure_return_type;
        }

        pub struct #logger_type_name;
        impl #closure_type_name for #logger_type_name {
            fn #handle_method_name(&mut self, #(#trait_args),*) -> #closure_return_type {
                log::info!(
                    "{}({}\n)",
                    stringify!(#handle_method_name),
                    ""
                );
                unimplemented!()
            }
        }
        // No trampoline: without `clientd`, C has nowhere to pass the closure
    }
}

pub fn generate_handlers(handler: &mut CHandler, bindings: &CBinding) -> TokenStream {
    if handler
        .args
        .iter()
        .any(|arg| arg.is_primitive() && arg.is_mut_pointer())
    {
        return quote! {};
    }

    // Stateless callbacks have no c_void parameter, so no clientd can carry a closure.
    // Generate only the trait and logger
    let has_c_void_param = handler.args.iter().any(|a| a.is_c_void());
    let is_stateless = !has_c_void_param;

    if is_stateless {
        let closure_type_name = format_ident!("{}Callback", snake_to_pascal_case(&handler.type_name));
        let fn_mut_args: Vec<TokenStream> = handler
            .args
            .iter()
            .map(|arg| {
                let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
                let type_name = return_type.get_callback_arg_type();
                if arg.is_single_mut_pointer() && arg.is_primitive() {
                    let owned_type: Type = parse_str(arg.base_type()).unwrap();
                    quote! { #owned_type }
                } else {
                    quote! { #type_name }
                }
            })
            .filter(|t| !t.is_empty())
            .collect();
        let closure_return_type = handler.return_type.as_type();
        handler.fn_mut_signature = quote! {
           FnMut(#(#fn_mut_args),*) -> #closure_return_type
        };
        handler.closure_type_name = quote! {
           #closure_type_name
        };
        return generate_stateless_handler_code(handler);
    }

    let fn_name = format_ident!("{}_callback", handler.type_name);
    let closure_fn_name = format_ident!("{}_callback_for_once_closure", handler.type_name);
    let doc_comments: Vec<TokenStream> = handler
        .docs
        .iter()
        .flat_map(|doc| doc.lines())
        .map(|line| quote! { #[doc = #line] })
        .collect();

    let closure = handler
        .args
        .iter()
        .find(|a| a.is_c_void())
        .map(|a| a.name.clone())
        .unwrap_or_else(|| "state".to_string());
    let closure_name = format_ident!("{}", closure);
    let closure_type_name = format_ident!("{}Callback", snake_to_pascal_case(&handler.type_name));
    let closure_return_type = handler.return_type.as_type();

    let logger_type_name = format_ident!("{}Logger", snake_to_pascal_case(&handler.type_name));

    let handle_method_name = format_ident!("handle_{}", &handler.type_name[..handler.type_name.len() - 2]);

    let no_method_name = format_ident!(
        "no_{}_handler",
        &handler.type_name[..handler.type_name.len() - 2]
            .replace("_on_", "_")
            .replace("aeron_", "")
    );

    let args: Vec<TokenStream> = handler
        .args
        .iter()
        .map(|arg| {
            let arg_name = arg.as_ident();
            // Raw C types, as this signature belongs to the trampoline C calls
            let arg_type: Type = arg.as_type();
            quote! { #arg_name: #arg_type }
        })
        .filter(|t| !t.is_empty())
        .collect();

    let arg_names_for_logging: Vec<TokenStream> = handler
        .args
        .iter()
        .map(|arg| {
            let arg_name = arg.as_ident();
            quote! { format!("{} = {:?}", stringify!(#arg_name), #arg_name) }
        })
        .collect();

    let converted_args: Vec<TokenStream> = handler
        .args
        .iter()
        .filter_map(|arg| {
            let name = &arg.name;
            let arg_name = arg.as_ident();
            // Skip closure argument - it's only used to get the closure reference
            // All other args (including other c_void args) are passed to the handler
            if name == &closure {
                None
            } else {
                let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
                Some(return_type.handle_c_to_rs_callback_arg(quote! {#arg_name}, &bindings.handlers))
            }
        })
        .filter(|t| !t.is_empty())
        .collect();

    let closure_args: Vec<TokenStream> = handler
        .args
        .iter()
        .filter_map(|arg| {
            let name = &arg.name;
            if name == &closure {
                return None;
            }

            let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
            let type_name = return_type.get_callback_arg_type();
            let field_name = format_ident!("{}", name);
            if type_name.is_empty() {
                None
            } else {
                Some(quote! {
                    #field_name: #type_name
                })
            }
        })
        .filter(|t| !t.is_empty())
        .collect();

    let mut log_field_names = vec![];
    let closure_args_in_logger: Vec<TokenStream> = handler
        .args
        .iter()
        .filter_map(|arg| {
            let name = &arg.name;
            if name == &closure {
                return None;
            }

            let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
            let type_name = return_type.get_callback_arg_type();
            let field_name = format_ident!("{}", name);
            if type_name.is_empty() {
                None
            } else {
                log_field_names.push(Some(
                    quote! { format!("{} : {:?}", stringify!(#field_name), #field_name) },
                ));

                Some(quote! {
                    #field_name: #type_name
                })
            }
        })
        .filter(|t| !t.is_empty())
        .collect();

    if log_field_names.is_empty() {
        log_field_names.push(Some(quote! { "" }));
    }

    let fn_mut_args: Vec<TokenStream> = handler
        .args
        .iter()
        .filter_map(|arg| {
            let name = &arg.name;
            if name == &closure {
                return None;
            }

            let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
            let type_name = return_type.get_callback_arg_type();
            if arg.is_single_mut_pointer() && arg.is_primitive() {
                let owned_type: Type = parse_str(arg.base_type()).unwrap();
                return Some(quote! { #owned_type });
            } else {
                return Some(quote! {
                    #type_name
                });
            }
        })
        .filter(|t| !t.is_empty())
        .collect();

    handler.fn_mut_signature = quote! {
       FnMut(#(#fn_mut_args),*) -> #closure_return_type
    };
    handler.closure_type_name = quote! {
       #closure_type_name
    };

    let logger_return_type = if closure_return_type.to_token_stream().to_string().eq("()") {
        closure_return_type.clone().to_token_stream()
    } else {
        quote! {
            unimplemented!()
        }
    };

    let wrapper_closure_args: Vec<TokenStream> = handler
        .args
        .iter()
        .filter_map(|arg| {
            let name = &arg.name;
            if name == &closure {
                return None;
            }

            let field_name = format_ident!("{}", name);
            let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone()).get_new_return_type(false, false);
            if return_type.is_empty() {
                None
            } else {
                Some(quote! { #field_name })
            }
        })
        .filter(|t| !t.is_empty())
        .collect();

    // Trampolines per handler, picked by arg_classifier:
    // - plain name: logs a panic and carries on (callbacks retained by C, e.g. on the conductor thread)
    // - `_catch`: stores the panic for the Rust wrapper to raise once C returns
    // - `_unwind` (UNWIND_HANDLER_TYPES only): a plain call that lets the panic through at no cost
    // Both catching kinds return `fallback` to C after a panic
    let unwind = UNWIND_HANDLER_TYPES.contains(&handler.type_name.as_str());
    let abi = if unwind {
        quote! { "C-unwind" }
    } else {
        quote! { "C" }
    };
    let return_type_str = closure_return_type.to_token_stream().to_string();
    let fallback = match return_type_str.as_str() {
        "()" => quote! { () },
        "aeron_controlled_fragment_handler_action_t" => {
            quote! { aeron_controlled_fragment_handler_action_t::AERON_ACTION_ABORT }
        }
        "aeron_rb_read_action_t" => quote! { aeron_rb_read_action_t::AERON_RB_ABORT },
        s if s.starts_with("* mut") => quote! { std::ptr::null_mut() },
        s if s.starts_with("* const") => quote! { std::ptr::null() },
        // ints and bools; a new enum return type fails to compile here and needs its own arm
        _ => quote! { Default::default() },
    };
    #[derive(Clone, Copy)]
    enum OnPanic {
        Log,
        Catch,
        Unwind,
    }
    let trampoline = |name: &Ident, bound: TokenStream, call: TokenStream, on_panic: OnPanic| {
        let once = name.to_string().contains("for_once_closure") || name.to_string().contains("for_borrowed_handler");
        let policy_generic = if once {
            quote! {}
        } else {
            quote! { , P: CallbackPolicy }
        };
        let (callback_access, access_doc) = if once {
            (
                quote! { let closure: &mut F = unsafe { &mut *(#closure_name as *mut F) }; },
                quote! {
                    /// Borrowed callback: `clientd` is the caller's `&mut F` for this call only.
                    /// No `lock()`, `RefCell`, mutex or reference count. `poll`, `poll_fn` and
                    /// other `_fn` methods use this form
                },
            )
        } else {
            (
                quote! { let mut state = unsafe { Handler::<F, P>::lock_raw(#closure_name) }; let closure: &mut F = &mut *state; },
                quote! {
                    /// `Handler` callback: every call takes `Handler::lock_raw`, a `RefCell` borrow
                    /// check or a `SharedHandler` mutex. Used by retained setters, close
                    /// notifications and non-poll methods that take a `Handler`. Polls never use it
                },
            )
        };
        // Inside the catch body, so a null `clientd` is logged instead of aborting the process
        let callback_access = quote! {
            #[cfg(debug_assertions)]
            if #closure_name.is_null() {
                panic!("closure should not be null")
            }
            #callback_access
        };
        let body = match on_panic {
            OnPanic::Log => quote! { log_panic(stringify!(#name), #fallback, || { #callback_access #call }) },
            OnPanic::Catch => quote! { catch_panic(#fallback, || { #callback_access #call }) },
            OnPanic::Unwind => quote! { #callback_access #call },
        };
        quote! {
            #[allow(dead_code)]
            #(#doc_comments)*
            ///
            #access_doc
            unsafe extern #abi fn #name<F: #bound #policy_generic>(
                #(#args),*
            ) -> #closure_return_type
            {
                #[cfg(feature = "extra-logging")]
                {
                    log::debug!("calling {}", stringify!(#name));
                }
                #[cfg(feature = "log-c-bindings")]
                log::debug!(
                    "{}({}\n)",
                    stringify!(#name),
                    [#(#arg_names_for_logging),*].join(", ")
                );
                #body
            }
        }
    };
    let handle_call = quote! { closure.#handle_method_name(#(#converted_args),*) };
    let once_call = quote! { closure(#(#converted_args),*) };
    let once_bound = quote! { FnMut(#(#fn_mut_args),*) -> #closure_return_type };
    let handle_bound = quote! { #closure_type_name };
    let mut trampolines = vec![trampoline(
        &fn_name,
        handle_bound.clone(),
        handle_call.clone(),
        OnPanic::Log,
    )];
    let mut kinds = vec![("_catch", OnPanic::Catch)];
    if unwind {
        kinds.push(("_unwind", OnPanic::Unwind));
    }
    for (suffix, on_panic) in kinds {
        let name = format_ident!("{}{}", fn_name, suffix);
        let once_name = format_ident!("{}{}", closure_fn_name, suffix);
        trampolines.push(trampoline(&name, handle_bound.clone(), handle_call.clone(), on_panic));
        trampolines.push(trampoline(&once_name, once_bound.clone(), once_call.clone(), on_panic));
        if unwind {
            let direct_name = format_ident!("{}_callback_for_borrowed_handler{}", handler.type_name, suffix);
            trampolines.push(trampoline(
                &direct_name,
                handle_bound.clone(),
                handle_call.clone(),
                on_panic,
            ));
        }
    }
    let trampolines = quote! { #(#trampolines)* };

    // Lets a poll wrapper raise a panic its handler caught one level down (the fragment
    // assemblers catch their delegate's panic), at no cost to handlers that leave it false
    let native_poll_callback = if matches!(
        handler.type_name.as_str(),
        "aeron_fragment_handler_t" | "aeron_controlled_fragment_handler_t"
    ) {
        let native_type = format_ident!("{}", handler.type_name);
        quote! {
            #[doc(hidden)]
            fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, #native_type>> { None }
        }
    } else {
        quote! {}
    };
    let resume_const = if unwind {
        quote! {
            #[doc(hidden)]
            const RESUMES_CAUGHT_PANIC: bool = false;
        }
    } else {
        quote! {}
    };
    let resume_const_forward = if unwind {
        quote! { const RESUMES_CAUGHT_PANIC: bool = T::RESUMES_CAUGHT_PANIC; }
    } else {
        quote! {}
    };

    let slot_type_name = format_ident!("{}Slot", snake_to_pascal_case(&handler.type_name));
    // Fragment polls take an exclusive callback directly. Handler does not implement the
    // poll callback traits, so no RefCell or Mutex can reach the poll path
    let shared_handler_impl = if unwind {
        quote! {}
    } else {
        quote! {
            /// Pass a [`Handler`] clone to share a callback across registrations.
            impl<T: #closure_type_name, P: CallbackPolicy> #closure_type_name for Handler<T, P> {
                #resume_const_forward
                #[inline]
                fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type {
                    let mut inner = self.lock();
                    inner.#handle_method_name(#(#wrapper_closure_args),*)
                }
            }
        }
    };
    let slot_constructor = if return_type_str == "()" {
        quote! {
            /// Creates an empty slot. Calls do nothing until you use `set`.
            pub fn new() -> Self {
                Self { callback: CallbackSlot::new(), on_empty: || () }
            }
        }
    } else {
        quote! {
            /// Creates an empty slot. Calls return `on_empty()` until you use `set`.
            pub fn new(on_empty: fn() -> #closure_return_type) -> Self {
                Self { callback: CallbackSlot::new(), on_empty }
            }
        }
    };
    let slot_default = if return_type_str == "()" {
        quote! {
            impl Default for #slot_type_name {
                fn default() -> Self { Self::new() }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #(#doc_comments)*
        ///
        ///
        /// _(note wrapper arguments are borrowed for the call only; copy out any fields, including strings, you use afterwards)_
        pub trait #closure_type_name {
            #resume_const
            #native_poll_callback
            fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type;
        }

        pub struct #logger_type_name;
        impl #closure_type_name for #logger_type_name {
            fn #handle_method_name(&mut self, #(#closure_args_in_logger),*) -> #closure_return_type {
                log::info!("{}({}\n)",
                    stringify!(#handle_method_name),
                    [#(#log_field_names),*].join(", "),
                );
                #logger_return_type
            }
        }


        /// Any closure with the matching signature can be a callback.
        /// Retained-callback methods allocate it in a [`SharedHandler`].
        /// The registering resource keeps that handler alive.
        impl<F: FnMut(#(#fn_mut_args),*) -> #closure_return_type> #closure_type_name for F {
            #[inline]
            fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type {
                self(#(#wrapper_closure_args),*)
            }
        }

        #shared_handler_impl

        /// A callback you can replace without registering another handler.
        ///
        /// Register a clone once, then use `set` or `clear`. Clones share the callback.
        /// Old callbacks are freed after their last call. The handler stays registered.
        ///
        /// Calls to the same callback run one at a time. It can replace or clear itself,
        /// but calling itself again will deadlock.
        /// A non-empty slot takes two internal mutex locks and clones an `Arc` per call.
        /// A retained handler adds its own lock, for three lock acquisitions in total.
        /// For updates that can wait for an active callback, use the returned handler's
        /// `lock()` instead. This avoids the slot's locks and per-call `Arc` clone.
        #[derive(Clone)]
        pub struct #slot_type_name {
            callback: CallbackSlot<std::sync::Mutex<Box<dyn FnMut(#(#fn_mut_args),*) -> #closure_return_type + Send>>>,
            on_empty: fn() -> #closure_return_type,
        }

        impl #slot_type_name {
            #slot_constructor

            /// Replaces the callback. Calls already using it can finish.
            pub fn set<F>(&self, callback: F)
            where F: FnMut(#(#fn_mut_args),*) -> #closure_return_type + Send + 'static {
                self.callback.set(std::sync::Arc::new(std::sync::Mutex::new(Box::new(callback))));
            }

            /// Removes the callback. Calls already using it can finish.
            pub fn clear(&self) {
                self.callback.clear();
            }
        }

        #slot_default

        impl #closure_type_name for #slot_type_name {
            fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type {
                self.callback.with(|callback| {
                    // Keep using the callback after a panic.
                    let mut callback = callback.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    callback(#(#wrapper_closure_args),*)
                }).unwrap_or_else(self.on_empty)
            }
        }

        // `NoHandler` implements every generated callback trait so that
        // `Handlers::NONE` (= `None::<&Handler<NoHandler>>`) can pin the callback
        // generic at any call site. Its body is unreachable: the C side receives a
        // null callback + null clientd, so this method can never be invoked.
        impl #closure_type_name for NoHandler {
            fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type {
                unreachable!("NoHandler is a None sentinel; its callback must never be invoked")
            }
        }

        #trampolines
    }
}

/// True if the only way to get this struct is as a callback argument. Such wrappers get no
/// `Clone` and no safe `From` from a pointer or struct value
///
/// Why: a callback argument points at C memory that is gone once the callback returns. `Clone`
/// on it copies the pointer, not the data, so this would compile and then read freed memory:
///
/// ```ignore
/// let mut kept = None;
/// archive.list_recordings_fn(&mut count, 0, 10, |d| kept = Some(d.clone()))?;
/// kept.unwrap().original_channel(); // still points at the freed C memory
/// ```
///
/// Without `Clone`, `d.clone()` only clones the `&` reference, which cannot leave the callback,
/// so the compiler rejects this. `From(d.get_inner())` and `From(**d)` would be the same escape
///
/// A struct counts as callback only when all of these hold:
/// - some callback takes a pointer to it, e.g. `aeron_archive_recording_descriptor_t *`
/// - no C function hands one out, by return value or `**` out-param. Copy functions that need an
///   existing one as input, e.g. `aeron_publication_error_values_copy(dst, src)`, do not count
/// - no other struct keeps a pointer to it. Constructors such as
///   `AeronArchiveReplicationParams::new` call `.clone()` on those to keep them alive, and that
///   must stay a real `Clone`
fn is_callback_only(type_name: &str, wrappers: &BTreeMap<String, CWrapper>, handlers: &[CHandler]) -> bool {
    let points_to = |a: &Arg| a.is_any_pointer() && a.base_type() == type_name;
    let takes_existing = |a: &Arg| points_to(a) && !a.is_double_mut_pointer();
    let in_callback = handlers.iter().flat_map(|h| &h.args).any(takes_existing);
    let produced = wrappers.values().flat_map(|w| &w.methods).any(|m| {
        let outputs =
            points_to(&m.return_type) || m.arguments.iter().any(|a| points_to(a) && a.is_double_mut_pointer());
        outputs && !m.arguments.iter().any(takes_existing)
    });
    let held = wrappers.values().flat_map(|w| &w.fields).any(points_to);
    in_callback && !produced && !held
}

#[cfg(test)]
mod is_callback_only_tests {
    use super::*;

    fn arg(c_type: &str) -> Arg {
        Arg {
            name: "a".into(),
            c_type: c_type.into(),
            processing: ArgProcessing::Default,
        }
    }

    fn method(return_type: &str, arguments: &[&str]) -> Method {
        Method {
            fn_name: String::new(),
            struct_method_name: String::new(),
            return_type: arg(return_type),
            arguments: arguments.iter().map(|a| arg(a)).collect(),
            docs: BTreeSet::new(),
        }
    }

    fn handler(args: &[&str]) -> CHandler {
        CHandler {
            type_name: String::new(),
            args: args.iter().map(|a| arg(a)).collect(),
            return_type: arg("()"),
            docs: BTreeSet::new(),
            fn_mut_signature: quote! {},
            closure_type_name: quote! {},
        }
    }

    // one case per rule, modelled on the real Aeron structs
    #[test]
    fn applies_each_rule() {
        let wrapper = |fields: &[&str], methods: Vec<Method>| CWrapper {
            fields: fields.iter().map(|f| arg(f)).collect(),
            methods,
            ..Default::default()
        };
        let wrappers = BTreeMap::from([
            ("aeron_header_t".into(), wrapper(&[], vec![])),
            // a copy needs an existing value, so it does not hand out a new one
            (
                "aeron_publication_error_values_t".into(),
                wrapper(
                    &[],
                    vec![method(
                        "i32",
                        &[
                            "* mut * mut aeron_publication_error_values_t",
                            "* mut aeron_publication_error_values_t",
                        ],
                    )],
                ),
            ),
            (
                "aeron_image_t".into(),
                wrapper(&[], vec![method("* mut aeron_image_t", &["* mut aeron_t"])]),
            ),
            (
                "aeron_counter_t".into(),
                wrapper(&[], vec![method("i32", &["* mut * mut aeron_counter_t"])]),
            ),
            ("aeron_archive_encoded_credentials_t".into(), wrapper(&[], vec![])),
            (
                "aeron_archive_replication_params_t".into(),
                wrapper(&["* mut aeron_archive_encoded_credentials_t"], vec![]),
            ),
        ]);
        let handlers = [
            handler(&[
                "* mut aeron_header_t",
                "* mut aeron_publication_error_values_t",
                "* mut aeron_image_t",
            ]),
            handler(&["* mut aeron_counter_t", "* mut aeron_archive_encoded_credentials_t"]),
        ];
        let callback_only = |t| is_callback_only(t, &wrappers, &handlers);

        assert!(callback_only("aeron_header_t"));
        assert!(callback_only("aeron_publication_error_values_t"));
        assert!(!callback_only("aeron_image_t"), "returned by a function");
        assert!(!callback_only("aeron_counter_t"), "written to a `**` out-param");
        assert!(
            !callback_only("aeron_archive_encoded_credentials_t"),
            "held by another struct"
        );
        assert!(
            !callback_only("aeron_archive_replication_params_t"),
            "not a callback argument"
        );
    }
}

fn generate_debug_fields(
    wrapper: &CWrapper,
    wrappers: &BTreeMap<String, CWrapper>,
    fields: &[TokenStream],
    methods: &[ImplItem],
) -> Vec<TokenStream> {
    // A terminal native poll can free the allocation before Rust drops its wrapper.
    if handle_is_consumed_by_poll(&wrapper.type_name) {
        return vec![];
    }

    // Unchecked text getters are unsafe because the returned `&str` can outlive a later
    // setter or poll. Formatting finishes before any such call can run on a local handle,
    // so Debug may read them. Shared handles skip them, as another thread may write
    let local = !handle_is_shared(&wrapper.type_name);
    let field_impl: syn::ItemImpl = syn::parse2(quote! { impl Placeholder { #(#fields)* } }).expect("generated fields");
    let getters: BTreeMap<_, _> = field_impl
        .items
        .iter()
        .chain(methods)
        .filter_map(|item| match item {
            ImplItem::Fn(method) if method.sig.inputs.len() == 1 && method.sig.receiver().is_some() => {
                let ident = &method.sig.ident;
                if matches!(method.sig.safety, syn::Safety::Default) {
                    Some((ident.to_string(), quote! { self.#ident() }))
                } else if local && method.sig.output.to_token_stream().to_string() == "-> & str" {
                    Some((ident.to_string(), quote! { unsafe { self.#ident() } }))
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    let mut result = vec![];
    for field in &wrapper.fields {
        let text = field.is_c_string_any();
        let value = !field.is_any_pointer()
            && (field.is_primitive() || field.is_byte_array() || wrappers.contains_key(&field.c_type));
        if (text || value)
            && let Some(getter) = getters.get(&field.name)
        {
            let name = &field.name;
            result.push(quote! { .field(#name, &#getter) });
        }
    }
    for method in &wrapper.methods {
        let name = &method.struct_method_name;
        let getter = if method.arguments.len() == 1 && method.return_type.is_c_string_any() {
            name.clone()
        } else if matches!(
            method.fn_name.as_str(),
            "aeron_publication_constants"
                | "aeron_exclusive_publication_constants"
                | "aeron_subscription_constants"
                | "aeron_image_constants"
                | "aeron_counter_constants"
                | "aeron_cnc_constants"
                | "aeron_counters_reader_get_buffers"
        ) {
            format!("get_{name}")
        } else {
            continue;
        };
        if let Some(getter) = getters.get(&getter) {
            result.push(quote! { .field(#name, &#getter) });
        }
    }
    result
}

pub fn generate_rust_code(
    wrapper: &CWrapper,
    wrappers: &BTreeMap<String, CWrapper>,
    include_common_code: bool,
    include_clippy: bool,
    include_aeron_client_registering_resource_t: bool,
    closure_handlers: &Vec<CHandler>,
    constants: &[Arg],
) -> TokenStream {
    let class_name = Ident::new(&wrapper.class_name, proc_macro2::Span::call_site());
    let type_name = Ident::new(&wrapper.type_name, proc_macro2::Span::call_site());

    let mut ref_getters = vec![];

    let generated_methods = wrapper.generate_methods(wrappers, closure_handlers, &mut ref_getters);
    // Associated functions stay on the default handle so `Aeron::nano_clock()`
    // does not need a storage type argument. Receiver methods work on all storage.
    let method_impl: syn::ItemImpl =
        syn::parse2(quote! { impl Placeholder { #(#generated_methods)* } }).expect("generated methods");
    let (static_methods, methods): (Vec<_>, Vec<_>) = method_impl
        .items
        .into_iter()
        .partition(|item| matches!(item, syn::ImplItem::Fn(method) if method.sig.receiver().is_none()));
    let tests = wrapper.generate_allocation_test(wrappers);
    let constructor = if crate::convenience::is_descriptor_poller(wrapper) {
        vec![]
    } else {
        wrapper.generate_constructor(wrappers)
    };
    let convenience_methods = crate::convenience::generate(wrapper, wrappers, constants);

    let async_impls = if wrapper.type_name.starts_with("aeron_async_")
        || wrapper.type_name.starts_with("aeron_archive_async_")
    {
        let new_method = wrapper.methods.iter().find(|m| m.fn_name == wrapper.without_name);

        if let Some(new_method) = new_method {
            let main_type = &wrapper.type_name.replace("_async_", "_").replace("_add_", "_");
            let main = get_possible_wrappers(main_type)
                .iter()
                .filter_map(|f| wrappers.get(f))
                .next()
                .expect(&format!("failed to find main type {}", main_type));

            let poll_method = main
                .methods
                .iter()
                .find(|m| m.fn_name == format!("{}_poll", wrapper.without_name))
                .unwrap();

            let main_class_name = format_ident!("{}", main.class_name);
            let main_native_type = format_ident!("{}", main.type_name);
            let async_class_name = format_ident!("{}", wrapper.class_name);
            let poll_method_name = format_ident!("{}_poll", wrapper.without_name);
            let new_method_name = format_ident!("{}", new_method.fn_name);

            let client_class = wrappers
                .get(
                    new_method
                        .arguments
                        .iter()
                        .skip(1)
                        .next()
                        .unwrap()
                        .c_type
                        .split_whitespace()
                        .last()
                        .unwrap(),
                )
                .unwrap();
            let client_type = format_ident!("{}", client_class.class_name);
            let client_type_method_name = format_ident!(
                "{}",
                new_method
                    .fn_name
                    .replace(&format!("{}_", client_class.without_name), "")
            );
            let client_type_method_name_without_async = format_ident!(
                "{}",
                new_method
                    .fn_name
                    .replace(&format!("{}_", client_class.without_name), "")
                    .replace("async_", "")
            );
            // A context owner (archive connect) holds the client in C, set through
            // `set_aeron`, rather than as a Rust dependency of the poller.
            let owner_client_fallback = if client_class
                .methods
                .iter()
                .any(|m| m.fn_name == format!("{}_get_aeron", client_class.without_name))
            {
                quote! {
                    .or_else(|| {
                        self.inner
                            .get_handle_dependency::<#client_type>()
                            .and_then(|owner| owner.inner.get_handle_dependency_matching::<Aeron>(owner.get_aeron().get_inner()))
                            .filter(|aeron| !aeron.get_inner().is_null())
                    })
                }
            } else {
                quote! {}
            };

            let init_args: Vec<TokenStream> = poll_method
                .arguments
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| {
                    if idx == 0 {
                        Some(quote! { ctx_field })
                    } else {
                        let arg_name = arg.as_ident();
                        let arg_name = ReturnType::new(arg.clone(), wrappers.clone())
                            .handle_rs_to_c_return(quote! { #arg_name }, false);
                        Some(quote! { #arg_name })
                    }
                })
                .filter(|t| !t.is_empty())
                .collect();

            let new_args: Vec<TokenStream> = poll_method
                .arguments
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| {
                    if idx == 0 {
                        None
                    } else {
                        let arg_name = arg.as_ident();
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, true);
                        if arg_type.clone().into_token_stream().is_empty() {
                            None
                        } else {
                            Some(quote! { #arg_name: #arg_type })
                        }
                    }
                })
                .filter(|t| !t.is_empty())
                .collect();

            let async_init_args: Vec<TokenStream> = new_method
                .arguments
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| {
                    if idx == 0 {
                        Some(quote! { ctx_field })
                    } else {
                        let arg_name = arg.as_ident();
                        let arg_name = ReturnType::new(arg.clone(), wrappers.clone())
                            .handle_rs_to_c_return(quote! { #arg_name }, false);
                        Some(quote! { #arg_name })
                    }
                })
                .filter(|t| !t.is_empty())
                .collect();

            // Generate logging for async new method arguments (as token stream)
            let async_log_expr_tokens = CWrapper::generate_arg_logging(&new_method.arguments, &async_init_args);

            let generic_types: Vec<TokenStream> = new_method
                .arguments
                .iter()
                .flat_map(|arg| {
                    ReturnType::new(arg.clone(), wrappers.clone())
                        .method_generics_for_where(true, owner_is_shared(&wrapper.type_name))
                        .into_iter()
                })
                .collect_vec();
            let where_clause_async = if generic_types.is_empty() {
                quote! {}
            } else {
                quote! { <#(#generic_types),*> }
            };
            let generic_types: Vec<TokenStream> = poll_method
                .arguments
                .iter()
                .flat_map(|arg| {
                    ReturnType::new(arg.clone(), wrappers.clone())
                        .method_generics_for_where(true, owner_is_shared(&wrapper.type_name))
                        .into_iter()
                })
                .collect_vec();
            let where_clause_main = if generic_types.is_empty() {
                quote! {}
            } else {
                quote! { <#(#generic_types),*> }
            };
            let async_new_args: Vec<TokenStream> = new_method
                .arguments
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| {
                    if idx == 0 {
                        None
                    } else {
                        let arg_name = arg.as_ident();
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, true);
                        if arg_type.clone().into_token_stream().is_empty() {
                            None
                        } else {
                            Some(quote! { #arg_name: #arg_type })
                        }
                    }
                })
                .filter(|t| !t.is_empty())
                .collect();

            // `new` signatures are already tokens here, so match the rendered `name : Aeron`
            let client_arg_name = |a: &TokenStream| {
                let a = a.to_string();
                (a.contains(" : Aeron") || a.contains(" : & Aeron"))
                    .then(|| format_ident!("{}", a.split_whitespace().next().unwrap()))
            };
            let async_dependancies = async_new_args
                .iter()
                .filter_map(client_arg_name)
                .map(|var_name| {
                    let callback_snapshot = if handle_is_archive_context(&client_class.type_name) {
                        quote! { result.inner.copy_dependencies_from(&#var_name.inner); }
                    } else {
                        quote! {}
                    };
                    quote! {
                        result.inner.add_handle_dependency(#var_name);
                        #callback_snapshot
                    }
                })
                .collect_vec();

            let archive_context_check = if handle_is_archive_context(&client_class.type_name) {
                quote! {
                    let _client = ctx.retained_aeron()?;
                }
            } else {
                quote! {}
            };
            // A missing client is a configuration error, so the blocking helper must not
            // retry it until the timeout
            let blocking_archive_context_check = if handle_is_archive_context(&client_class.type_name) {
                quote! {
                    self.retained_aeron()?;
                }
            } else {
                quote! {}
            };

            // The `Aeron` client argument, if any, keeps retained handlers alive and is passed to
            // `..._cancel(client, async)` on drop and `.cancel()`
            let async_client_var: Option<Ident> = async_new_args.iter().find_map(client_arg_name);

            let mut async_handler_deps: Vec<TokenStream> =
                CWrapper::handler_dependency_registrations(&new_method.arguments);

            // The conductor thread can invoke retained callbacks (e.g. image lifecycle
            // handlers) for as long as the *client* lives, even if this async poller is
            // dropped without ever being polled. Anchor each handler to the client too, so
            // dropping an unpolled poller cannot free a callback C still points at.
            if let Some(client_var) = &async_client_var {
                let client_anchored: Vec<TokenStream> = new_method
                    .arguments
                    .iter()
                    .filter_map(|arg| {
                        if let ArgProcessing::Handler(_) = &arg.processing {
                            if !arg.is_mut_pointer() {
                                let name = arg.as_ident();
                                return Some(quote! {
                                    if let Some(__handler) = #name {
                                        if let Some(__client_inner) = #client_var.inner.as_owned() {
                                            __client_inner.add_dependency(__handler.clone());
                                        }
                                    }
                                });
                            }
                        }
                        None
                    })
                    .collect();
                async_handler_deps.extend(client_anchored);
            }

            let async_new_args_for_client = async_new_args.iter().skip(1).cloned().collect_vec();

            let async_new_args_name_only: Vec<TokenStream> = new_method
                .arguments
                .iter()
                .enumerate()
                .filter_map(|(idx, arg)| {
                    if idx < 2 {
                        None
                    } else {
                        let arg_name = arg.as_ident();
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, false);
                        if arg_type.clone().into_token_stream().is_empty() {
                            None
                        } else {
                            Some(quote! { #arg_name })
                        }
                    }
                })
                .filter(|t| !t.is_empty())
                .collect();

            // Generate logging for poll method arguments (as token stream)
            let poll_log_expr_tokens = CWrapper::generate_arg_logging(&poll_method.arguments, &init_args);

            let close_cleanup = if let Some(close_method) = main.get_close_method() {
                let close_fn = format_ident!("{}", close_method.fn_name);
                // Aeron C close functions take optional notification callbacks and a clientd
                // pointer. `Default::default()` fills every arg beyond the resource pointer and
                // infers to `None` or `null_mut()`
                let extra_args: Vec<TokenStream> = close_method
                    .arguments
                    .iter()
                    .skip(1)
                    .map(|_| quote! { Default::default() })
                    .collect();
                quote! {
                    Some(Box::new(move |ptr| unsafe {
                        #close_fn(*ptr, #(#extra_args),*)
                    }))
                }
            } else {
                quote! { None }
            };

            // Safety: `poll()` calls `ManagedCResource::mark_resource_released()` on every terminal
            // outcome, which nulls the stored pointer, and `close_shared()` runs the cleanup closure
            // only for a non-null pointer. After `poll()` returns `Ok(Some(_))` or `Err(_)`, neither
            // `Drop` nor `.cancel()` calls cancel on the consumed pointer
            let cancel_method_name = format!("{}_cancel", new_method.fn_name);
            let async_cancel_cleanup = if let (Some(cancel_method), Some(client_var)) = (
                client_class.methods.iter().find(|m| m.fn_name == cancel_method_name),
                async_client_var.clone(),
            ) {
                let cancel_fn = format_ident!("{}", cancel_method.fn_name);
                let client_owned = format_ident!("{}_for_cancel", client_var);
                Some(quote! {
                    {
                        let #client_owned = #client_var.clone();
                        Some(Box::new(move |ptr| unsafe {
                            log::info!(
                                "auto-cancelling {} (poll() was never resolved before drop/cancel) to avoid leaking the pending Aeron registration",
                                stringify!(#async_class_name)
                            );
                            #cancel_fn(#client_owned.get_inner(), *ptr)
                        }))
                    }
                })
            } else {
                None
            };
            // Aeron 1.54.2 only frees an unpolled `aeron_archive_async_connect_t` through the private
            // `aeron_archive_async_connect_delete`, so declare it here (main does the same)
            // TODO: switch to the public API once https://github.com/aeron-io/aeron/issues/2250 adds
            // one. A new `_cancel` is not picked up automatically: the lookup above needs an `Aeron`
            // client argument and calls `cancel(client, async)`, while archive connect takes a context
            let async_new_cleanup_tokens = if type_name.to_string() == "aeron_archive_async_connect_t" {
                quote! { Some(Box::new(|ptr| {
                    unsafe extern "C" {
                        fn aeron_archive_async_connect_delete(connect: *mut aeron_archive_async_connect_t) -> i32;
                    }
                    unsafe { aeron_archive_async_connect_delete(*ptr) }
                })) }
            } else {
                async_cancel_cleanup.clone().unwrap_or(quote! { None })
            };
            // Only poll() may consume a registration. It also transfers parents
            // and invalidates every alias of the native async allocation.
            let main_constructor_visibility = quote! { pub(crate) };
            let has_cancel_method = async_cancel_cleanup.is_some();
            let transfer_dependencies = if main.type_name == "aeron_archive_t" {
                quote! { result.inner.take_dependencies_from(&self.inner); }
            } else {
                quote! { result.inner.copy_dependencies_from(&self.inner); }
            };

            let cancel_method_impl = if has_cancel_method {
                quote! {
                    #[doc = r"Cancels this operation and releases the pending Aeron registration."]
                    #[doc = r"Dropping an unresolved poller also cancels it."]
                    #[doc = r""]
                    #[doc = r"Does nothing after `poll()` returns `Ok(Some(_))` or `Err(_)`."]
                    #[doc = r"At that point Aeron has consumed the C struct and no registration"]
                    #[doc = r"remains pending."]
                    #[inline]
                    pub fn cancel(&self) -> Result<(), AeronCError> {
                        if let Some(inner) = self.inner.as_owned() {
                            inner.close_shared()
                        } else {
                            Ok(())
                        }
                    }
                }
            } else {
                quote! {}
            };

            quote! {
                    impl #main_class_name {
                        #[inline]
                        #main_constructor_visibility fn new #where_clause_main (#(#new_args),*) -> Result<Self, AeronCError> {
                            let resource = ManagedCResource::<#main_native_type>::new(
                                move |ctx_field| unsafe {
                                    #[cfg(feature = "log-c-bindings")]
                                    {
                                        let log_args = #poll_log_expr_tokens;
                                        log::info!("{}({})", stringify!(#poll_method_name), log_args);
                                    }
                                    #poll_method_name(#(#init_args),*)
                                },
                                #close_cleanup,
                                false,
                            )?;
                            Ok(Self {
                                inner: CResource::new(ResourceRef::new(resource)).into(),
                            })
                        }
                    }

                    impl #client_type {
                        #[doc = r"# Handler lifetime and async close"]
                        #[doc = r""]
                        #[doc = r"If this call takes a [`Handler`], the conductor may invoke its callbacks"]
                        #[doc = r"after the resource's `close()` or `drop` returns. The C close is asynchronous."]
                        #[doc = r""]
                        #[doc = r"The new resource keeps a clone of its [`Aeron`] client. The client keeps"]
                        #[doc = r"registered handlers alive until its last owning reference drops and C cleanup"]
                        #[doc = r"finishes. Closing the resource earlier does not free its handlers."]
                        #[doc = r"You do not need to call `release()`."]
                        #[doc = r""]
                        #[doc = r"Each registration adds a handler clone to the client. The closures and any"]
                        #[doc = r"values they capture stay in memory until client cleanup, even after the"]
                        #[doc = r"conductor finishes closing the resource."]
                        #[inline]
                        pub fn #client_type_method_name #where_clause_async(&self, #(#async_new_args_for_client),*) -> Result<#async_class_name, AeronCError> {
                            let mut result =  #async_class_name::new(self, #(#async_new_args_name_only),*);
                            if let Ok(result) = &mut result {
                                result.inner.add_handle_dependency(self);
                            }

                            result
                        }
                    }

                    impl #client_type {
                        #[doc = r"Blocking convenience wrapper around the async add operation."]
                        #[doc = r""]
                        #[doc = r"For examples and tests. Blocks the calling thread in a busy-poll loop until"]
                        #[doc = r"the resource is available or `timeout` elapses. In production, call the"]
                        #[doc = r"`async_add_*` variant and drive its `poll()` from your event loop."]
                        #[doc = r""]
                        #[doc = r"# Event loop example"]
                        #[doc = r"```text"]
                        #[doc = r"// Don't block. Drive the async poller from your loop"]
                        #[doc = r"let poller = client.async_add_*(...)?;"]
                        #[doc = r"loop {"]
                        #[doc = r"    if let Some(resource) = poller.poll()? {"]
                        #[doc = r"        break; // ready"]
                        #[doc = r"    }"]
                        #[doc = r"    do_other_work(); // service other subscriptions, timers, etc."]
                        #[doc = r"}"]
                        #[doc = r"```"]
                        #[doc = r""]
                        #[doc = r"`poll_blocking` on the async poller also blocks the calling thread."]
                        #[inline]
                        pub fn #client_type_method_name_without_async #where_clause_async(&self #(
                    , #async_new_args_for_client)*,  timeout: std::time::Duration) -> Result<#main_class_name, AeronCError> {
                            #blocking_archive_context_check
                            let start = std::time::Instant::now();
                            let mut last_error = None;
                            loop {
                                match #async_class_name::new(self, #(#async_new_args_name_only),*) {
                                Err(error) => last_error = Some(error),
                                Ok(poller) => {
                                    let invoker = poller.agent_invoker_client();
                                    while start.elapsed() <= timeout  {
                                      if let Some(aeron) = &invoker {
                                          aeron.main_do_work()?;
                                      }
                                      if let Some(result) = poller.poll()? {
                                          return Ok(result);
                                      }
                                    #[cfg(debug_assertions)]
                                    std::thread::sleep(std::time::Duration::from_millis(10));
                                  }
                                }
                                }
                            if start.elapsed() > timeout {
                                log::error!("failed async poll for {:?}", self);
                                // Report why the request could not start rather than a bare timeout
                                return Err(last_error.unwrap_or_else(|| AeronErrorType::TimedOut.into()));
                            }
                            #[cfg(debug_assertions)]
                            std::thread::sleep(std::time::Duration::from_millis(10));
                          }
            }
                    }

                    impl #async_class_name {
                        #[inline]
                        pub fn new #where_clause_async (#(#async_new_args),*) -> Result<Self, AeronCError> {
                            #archive_context_check
                            let resource_async = ManagedCResource::<#type_name>::new(
                                move |ctx_field| unsafe {
                                    #[cfg(feature = "log-c-bindings")]
                                    {
                                        let log_args = #async_log_expr_tokens;
                                        log::info!("{}({})", stringify!(#new_method_name), log_args);
                                    }
                                    #new_method_name(#(#async_init_args),*)
                                },
                                #async_new_cleanup_tokens,
                                false,
                            )?;
                            let result = Self {
                                inner: CResource::new(ResourceRef::new(resource_async)).into(),
                            };
                            #(#async_dependancies)*
                            // Retained callbacks (e.g. image lifecycle handlers) are stored by
                            // the C client; the dependency clones below are propagated to the
                            // final resource when poll() succeeds, keeping them alive until it
                            // closes.
                            #(#async_handler_deps)*
                            Ok(result)
                        }

                        #[inline]
                        pub fn poll(&self) -> Result<Option<#main_class_name>, AeronCError> {

                            if let Some(inner) = self.inner.as_owned() {
                                if inner.is_resource_released() || inner.get().is_null() {
                                    return Ok(None);
                                }
                            }

                            let mut result = #main_class_name::new(self);
                            if let Ok(result) = &mut result {
                                #transfer_dependencies
                            }

                            match result {
                                Ok(result) => {
                                    if let Some(inner) = self.inner.as_owned() {
                                        inner.mark_resource_released();
                                    }
                                    Ok(Some(result))
                                }
                                Err(e) if e.code == 0 => {
                                  Ok(None) // try again
                                }
                                Err(e) => {
                                    if let Some(inner) = self.inner.as_owned() {
                                        inner.mark_resource_released();
                                    }
                                    Err(e)
                                }
                            }
                        }

                        #cancel_method_impl

                        #[doc = r"Polls synchronously until the async operation completes or `timeout` elapses."]
                        #[doc = r""]
                        #[doc = r"For examples and tests. Blocks the calling thread"]
                        #[doc = r"in a busy-poll loop. In production, drive `poll()` from your own event loop"]
                        #[doc = r"(between other work, on a timer, or in a dedicated duty cycle) rather than"]
                        #[doc = r"blocking on a single operation."]
                        #[doc = r""]
                        #[doc = r"# Event loop example"]
                        #[doc = r"```text"]
                        #[doc = r"// Don't block. Integrate poll() into your existing loop"]
                        #[doc = r"loop {"]
                        #[doc = r"    if let Some(resource) = poller.poll()? {"]
                        #[doc = r"        break; // ready"]
                        #[doc = r"    }"]
                        #[doc = r"    do_other_work(); // service other subscriptions, timers, etc."]
                        #[doc = r"}"]
                        #[doc = r"```"]
                        #[doc = r""]
                        #[doc = r"See the blocking `add_*(.., timeout)` helpers for the same caveat."]
                        #[inline]
                        pub fn poll_blocking(&self, timeout: std::time::Duration) -> Result<#main_class_name, AeronCError> {
                            if let Some(result) = self.poll()? {
                                return Ok(result);
                            }

                            let invoker = self.agent_invoker_client();
                            let time = std::time::Instant::now();
                            while time.elapsed() < timeout {
                                if let Some(aeron) = &invoker {
                                    aeron.main_do_work()?;
                                }
                                if let Some(result) = self.poll()? {
                                    return Ok(result);
                                }
                                #[cfg(debug_assertions)]
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }
                            log::error!("failed async poll for {:?}", self);
                            Err(AeronErrorType::TimedOut.into())
                        }

                        #[doc = r"The client a blocking poll must drive, as nothing else runs its agent-invoker conductor."]
                        #[inline]
                        fn agent_invoker_client(&self) -> Option<Aeron> {
                            self.inner
                                .get_handle_dependency::<Aeron>()
                                #owner_client_fallback
                                .filter(Aeron::uses_agent_invoker)
                        }
                    }
                                }
        } else {
            quote! {}
        }
    } else {
        quote! {}
    };

    let mut additional_impls = vec![];

    if let Some(close_method) = wrapper.get_close_method() {
        let close_deferred_if_shared = wrapper.type_name == "aeron_t" || wrapper.type_name == "aeron_archive_t";
        let skip_generated_close =
            wrapper.methods.iter().any(|m| m.fn_name.contains("_init")) && !close_deferred_if_shared;
        if !skip_generated_close {
            let close_fn = format_ident!("{}", close_method.fn_name);
            let close_resource_call = quote! { self.inner.close_resource_deferred_if_shared() };

            // Consuming close(self) runs the FFI close through the shared ManagedCResource state,
            // then drops this handle. The cleanup closure is taken once across all clones
            if close_deferred_if_shared {
                additional_impls.push(quote! {
                    impl #class_name {
                        /// Releases this handle and closes the C client **once the last reference drops**.
                        ///
                        /// The C client owns and frees every child resource (publications,
                        /// subscriptions, counters) when it closes, so while children or clones
                        /// are alive this is deferred, equivalent to `drop`, and the surviving
                        /// handles remain usable. The C close runs when the final
                        /// reference (child or clone) is released.
                        pub fn close(self) -> Result<(), AeronCError> {
                            let result = #close_resource_call;
                            // Drop this handle (decrements Rc, releases deps).
                            drop(self);
                            result
                        }

                        /// Closes the C client **immediately**, even if children or clones are alive.
                        ///
                        /// All surviving clones and child handles become invalid:
                        ///
                        /// # Safety
                        /// The C client frees every child resource (publications, subscriptions,
                        /// counters) during this call, so surviving child handles dangle. Any use
                        /// is use-after-free, **including their `Drop`**, which calls the C close
                        /// on the freed pointer (double free). You must `std::mem::forget` every
                        /// surviving child, or never return (e.g. `std::process::exit`). Prefer
                        /// [`Self::close`], which defers until the last reference drops.
                        pub unsafe fn close_now(self) -> Result<(), AeronCError> {
                            let result = self.inner.close_resource();
                            drop(self);
                            result
                        }
                    }
                });
            } else {
                additional_impls.push(quote! {
                    impl #class_name {
                        /// Releases this handle. Native cleanup waits for the last owning clone
                        /// when the resource can be retained by another handle.
                        pub fn close(self) -> Result<(), AeronCError> {
                            let result = #close_resource_call;
                            // Drop this handle (decrements Rc, releases deps).
                            drop(self);
                            result
                        }
                    }
                });
            }

            let close_has_notification_handler = !close_deferred_if_shared
                && close_method
                    .arguments
                    .iter()
                    .any(|arg| matches!(arg.processing, ArgProcessing::Handler(_)))
                && close_method.arguments.iter().any(|arg| arg.is_c_void());

            if close_has_notification_handler {
                additional_impls.push(quote! {
                    impl #class_name {
                        /// Like [`Self::close`], but notifies `on_close_complete` when the close
                        /// finishes. The handler must outlive the notification (keep your
                        /// `Handler` alive until it has fired). C calls it later from the conductor,
                        /// so it needs a retained `SharedHandler`; it runs once at shutdown, off the
                        /// hot path
                        pub fn close_with_handler<AeronNotificationHandlerImpl: AeronNotificationCallback + Send + 'static>(
                            self,
                            on_close_complete: Option<&SharedHandler<AeronNotificationHandlerImpl>>,
                        ) -> Result<(), AeronCError> {
                            if let Some(owner) = self.inner.as_owned() {
                                if ResourceRef::strong_count(owner) > 1 {
                                    return Err(AeronCError::with_message(-1, "close_with_handler requires the last owning handle"));
                                }
                                if let Some(handler) = on_close_complete {
                                    if let Some(client) = self.inner.get_handle_dependency::<Aeron>() {
                                        if let Some(client_owner) = client.inner.as_owned() {
                                            client_owner.add_dependency(handler.clone());
                                        }
                                    }
                                }
                            }
                            let result = self.inner.close_resource_with(move |ptr| unsafe {
                                #close_fn(
                                    *ptr,
                                    {
                                        let callback: aeron_notification_t = if on_close_complete.is_none() {
                                            None
                                        } else {
                                            Some(aeron_notification_t_callback::<AeronNotificationHandlerImpl, SharedCallback>)
                                        };
                                        callback
                                    },
                                    on_close_complete
                                        .map(|m| m.as_raw())
                                        .unwrap_or_else(|| std::ptr::null_mut()),
                                )
                            });
                            drop(self);
                            result
                        }
                    }
                });
            }

            // Generate additional methods for specific types
            if wrapper.type_name == "aeron_counter_t" {
                additional_impls.push(quote! {
                    impl #class_name {
                        #[inline]
                        pub fn addr_atomic(&self) -> &std::sync::atomic::AtomicI64 {
                            unsafe { std::sync::atomic::AtomicI64::from_ptr(self.addr()) }
                        }
                    }
                });
            } else if wrapper.type_name == "aeron_counters_reader_t" {
                additional_impls.push(quote! {
                    impl #class_name {
                        #[inline]
                        pub fn addr_atomic(&self, counter_id: i32) -> &std::sync::atomic::AtomicI64 {
                            assert!(counter_id >= 0 && counter_id <= self.max_counter_id(), "invalid counter id");
                            unsafe { std::sync::atomic::AtomicI64::from_ptr(self.addr(counter_id)) }
                        }
                    }
                });
            } else if wrapper.type_name == "aeron_publication_t" || wrapper.type_name == "aeron_exclusive_publication_t"
            {
                additional_impls.push(quote! {
                    impl #class_name {
                        #[inline]
                        pub fn is_ready(&self) -> bool {
                            self.is_connected() && self.position_limit() != 0
                        }
                    }
                });
            }
        }
    }

    let common_code = if !include_common_code {
        quote! {}
    } else {
        TokenStream::from_str(COMMON_CODE).unwrap()
    };
    let warning_code = if !include_common_code {
        quote! {}
    } else {
        let mut code = String::new();

        if include_clippy {
            code.push_str(
                "        #![allow(non_upper_case_globals)]
        #![allow(non_camel_case_types)]
        #![allow(non_snake_case)]
        #![allow(clippy::all)]
        #![allow(unused_variables)]
        #![allow(unused_unsafe)]
",
            );
        }

        if include_aeron_client_registering_resource_t {
            code.push_str(
                "
                type aeron_client_registering_resource_t = aeron_client_registering_resource_stct;
",
            );
        }

        TokenStream::from_str(code.as_str()).unwrap()
    };
    let class_docs: Vec<TokenStream> = wrapper
        .docs
        .iter()
        .map(|doc| {
            let doc = doc.replace("<p>", "\n").replace("</p>", "\n");
            quote! {
                #[doc = #doc]
            }
        })
        .collect();

    let fields = wrapper.generate_fields(&wrappers, &mut ref_getters);
    let debug_fields = generate_debug_fields(wrapper, wrappers, &fields, &methods);
    let ref_impl = if ref_getters.is_empty() {
        quote! {}
    } else {
        quote! {
            impl<'a, S: ResourceStorage<#type_name>> Ref<'a, #class_name<S>> {
                #(#ref_getters)*
            }
        }
    };

    let default_impl =
        if wrapper.has_default_method() && !constructor.iter().map(|x| x.to_string()).join("").trim().is_empty() {
            // A shallow copy is independent of its source only when the struct has no pointers.
            let (unsafety, copy_docs) = if wrapper.borrows_native_storage(wrappers) {
                (
                    quote! { unsafe },
                    quote! {
                        ///
                        /// # Safety
                        /// This is a shallow copy, not a new native resource. Pointers inside
                        /// unions, arrays and nested structs are copied too. Keep their targets
                        /// alive for every use of the copy. Coordinate writes through aliases,
                        /// and do not use the copy to release or reinitialize shared native state.
                        /// No Rust or native write may overlap copying the source struct.
                        /// Callback storage expires when its callback returns. Copy individual
                        /// fields into Rust values when you need an independent snapshot.
                    },
                )
            } else {
                (
                    quote! {},
                    quote! {
                        ///
                        /// The struct holds no pointers, so the copy does not depend on the source
                    },
                )
            };
            let clone_struct = quote! {
                /// Copies the underlying struct into a new heap allocation
                ///
                /// Only for plain data structs with no init or clean-up functions, so not for
                /// Aeron, AeronContext, AeronPublication or AeronSubscription
                #copy_docs
                pub #unsafety fn clone_struct(&self) -> #class_name<CResource<#type_name>> {
                    // SAFETY: the source is initialised. Pointer fields keep the
                    // same lifetime requirements documented on this method.
                    let value = unsafe { *self.get_inner_ref() };
                    #class_name::from_resource(CResource::owned_value(value))
                }
            };

            let default_value = if wrapper.zeroed_is_valid(wrappers) || wrapper.type_name == "aeron_buffer_claim_t" {
                quote! {
                    impl Default for #class_name<CResource<#type_name>> {
                        fn default() -> Self {
                            #class_name::new_zeroed_on_heap()
                        }
                    }

                }
            } else {
                quote! {}
            };
            quote! {
                #default_value
                impl<S: ResourceStorage<#type_name>> #class_name<S> { #clone_struct }
            }
        } else {
            quote! {}
        };

    let derive_clone = if handle_is_archive_context(&wrapper.type_name)
        || handle_is_single_owner(&wrapper.type_name)
        || crate::convenience::is_descriptor_poller(wrapper)
    {
        quote! {}
    } else {
        quote! { #[derive(Clone)] }
    };
    let from_native = if !wrapper.opaque
        && !handle_has_stable_pointer(&wrapper.type_name)
        && !handle_is_archive_context(&wrapper.type_name)
        && !wrapper.skipped_methods.contains("from_native")
    {
        let (unsafety, safety_docs) = if wrapper.borrows_native_storage(wrappers) {
            (
                quote! { unsafe },
                quote! {
                    /// # Safety
                    /// Pointer fields must stay valid for every use of the returned handle.
                },
            )
        } else {
            (quote! {}, quote! {})
        };
        quote! {
            /// Copy a native struct into inline storage.
            #safety_docs
            #[inline]
            pub #unsafety fn from_native(value: #type_name) -> #class_name<Inline<#type_name>> {
                #class_name { inner: Inline::new(value),  }
            }
        }
    } else {
        quote! {}
    };
    let from_ptr = if wrapper.skipped_methods.contains("from_ptr") {
        quote! {}
    } else {
        quote! {
            /// Borrow a native pointer without taking ownership.
            /// # Safety
            /// The pointer must be non-null. It and its parents must remain live for every use. Calls
            /// must follow Aeron's threading and aliasing rules.
            #[inline]
            pub unsafe fn from_ptr(ptr: *mut #type_name) -> #class_name<Borrowed<#type_name>> {
                #class_name { inner: unsafe { Borrowed::new_unchecked(ptr) },  }
            }
        }
    };
    let wrap_impls = quote! {
        impl #class_name {
            #from_ptr
            #from_native
        }
    };

    let copy_out_impl = if wrapper.opaque {
        quote! {}
    } else {
        quote! {
            impl<S: ResourceStorage<#type_name>> From<#class_name<S>> for #type_name {
                #[inline]
                fn from(value: #class_name<S>) -> Self {
                    unsafe { *value.get_inner().clone() }
                }
            }
        }
    };

    let aliases: Vec<_> = wrappers
        .values()
        .filter(|w| !wrapper.native_type.is_empty() && w.native_type == wrapper.native_type)
        .collect();
    let policy = if owner_is_shared(&wrapper.type_name) || aliases.iter().any(|w| owner_is_shared(&w.type_name)) {
        quote! { SharedResource }
    } else {
        quote! { LocalResource }
    };
    let emits_policy = aliases.iter().all(|w| w.type_name >= wrapper.type_name);
    let pointer_storage =
        if wrapper.caches_owned_value_pointer() && aliases.iter().all(|alias| alias.caches_owned_value_pointer()) {
            quote! { CachedPointer<Self> }
        } else {
            quote! { ReloadPointer }
        };
    let native_policy = if emits_policy {
        quote! {
            unsafe impl NativeResource for #type_name {
                type Policy = #policy;
                type Pointer = #pointer_storage;
            }
        }
    } else {
        quote! {}
    };
    let owner_storage = if handle_has_stable_pointer(&wrapper.type_name) {
        quote! { LiveCResource<#type_name> }
    } else {
        quote! { CResource<#type_name> }
    };
    let resource_storage = if is_callback_only(&wrapper.type_name, wrappers, closure_handlers) {
        quote! { Borrowed<#type_name> }
    } else {
        owner_storage.clone()
    };
    let ref_name = format_ident!("{}Ref", wrapper.class_name);

    let lifecycle_helpers = if handle_is_archive_context(&wrapper.type_name) {
        quote! {
        /// Require a Rust-owned client before C can copy this context. Otherwise C
        /// can create and later free a client still referenced by the original context.
        pub(crate) fn retained_aeron(&self) -> Result<Aeron, AeronCError> {
            let pointer = unsafe { aeron_archive_context_get_aeron(self.get_inner()) };
            let client = self
                .inner
                .get_handle_dependency_matching::<Aeron>(pointer)
                .ok_or_else(|| {
                    AeronCError::with_message(
                        -1,
                        "set a Rust-owned Aeron client on the archive context before connecting",
                    )
                })?;
            if self.get_owns_aeron_client() {
                return Err(AeronCError::with_message(
                    -1,
                    "archive context must not own the native client",
                ));
            }
            Ok(client)
        }

            }
    } else {
        quote! {}
    };

    let borrow_method = if handle_is_consumed_by_poll(&wrapper.type_name) {
        quote! {}
    } else {
        quote! {
            /// Borrow this handle without cloning its owner.
            #[inline(always)]
            pub fn as_ref(&self) -> Ref<'_, Self> {
                // SAFETY: an owning handle keeps this allocation alive while borrowed.
                Ref::new(Self::from_borrowed(unsafe { Borrowed::new(self.inner.get()) }))
            }
        }
    };
    let check_live_pointer = if handle_is_consumed_by_poll(&wrapper.type_name) {
        quote! { assert!(!pointer.is_null(), "native async resource has already completed or been cancelled"); }
    } else {
        quote! {}
    };

    let thread_traits = if handle_is_shared(&wrapper.type_name) {
        quote! {
            // SAFETY: this type is in the audited shared-handle list. Shared owner
            // storage checks dependencies and cleanup; close waits for the last owner.
            unsafe impl<S: ResourceStorage<#type_name> + SharedStorage<#type_name>> Send for #class_name<S> {}
            unsafe impl<S: ResourceStorage<#type_name> + SharedStorage<#type_name>> Sync for #class_name<S> {}
        }
    } else if handle_is_send_only(&wrapper.type_name) {
        quote! {
            // SAFETY: the owner retains only shared dependencies and has no Clone, so moving it
            // moves the only safe handle. Without Sync, one thread at a time uses it, as C requires.
            // Retained images keep their own reference; see the type's threading docs
            unsafe impl Send for #class_name<LiveCResource<#type_name>> {}
        }
    } else {
        quote! {}
    };

    quote! {
        #warning_code

        // SAFETY: generated C structs contain no Rust-owned values. The policy
        // checks what their owners retain; it grants no native method concurrency.
        #native_policy

        #thread_traits
        #(#class_docs)*
        #derive_clone
        pub struct #class_name<S: ResourceStorage<#type_name> = #resource_storage> {
            inner: S,
        }

        /// Non-owning form used in callbacks and lifetime-bound getters.
        pub type #ref_name = #class_name<Borrowed<#type_name>>;
        impl<S: ResourceStorage<#type_name>> resource_sealed::Handle for #class_name<S> {}
        impl<S: ResourceStorage<#type_name>> ResourceHandle for #class_name<S> {
            type Native = #type_name;
            type Storage = S;
            type Borrowed = #class_name<Borrowed<#type_name>>;
        }
        impl<S: ResourceStorage<#type_name>> ResourceHandleExt for #class_name<S> {
            #[inline]
            fn resource(&self) -> &S { &self.inner }
            #[inline]
            fn from_resource(inner: S) -> Self { Self { inner,  } }
            #[inline]
            fn from_borrowed(inner: Borrowed<#type_name>) -> Self::Borrowed {
                #class_name { inner,  }
            }
        }
        impl OwnedResourceHandle for #class_name<#owner_storage> {
            #[inline]
            fn from_owned(inner: CResource<#type_name>) -> Self { Self { inner: inner.into(),  } }
        }

        impl<S: ResourceStorage<#type_name>> core::fmt::Debug for #class_name<S> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // Check storage directly so formatting a completed async handle cannot panic.
                if self.inner.get().is_null() {
                    return f.debug_struct(stringify!(#class_name)).field("inner", &"null").finish();
                }
                f.debug_struct(stringify!(#class_name))
                    .field("inner", &self.inner)
                    #(#debug_fields)*
                    .finish()
            }
        }

        impl #class_name<#owner_storage> {
            #(#constructor)*
        }
        impl #class_name {
            #(#static_methods)*
        }
        impl<S: ResourceStorage<#type_name>> #class_name<S> {
            #(#fields)*
            #(#methods)*
            #lifecycle_helpers

            #borrow_method

            #[inline(always)]
            pub fn get_inner(&self) -> *mut #type_name {
                let pointer = self.inner.get();
                #check_live_pointer
                pointer
            }

            /// Mutable access to the underlying C struct, minted from `&self`: nothing
            /// prevents two live `&mut` at once, so the caller must ensure exclusive
            /// access for the lifetime of the returned reference.
            ///
            /// # Safety
            /// No other reference (`&` or `&mut`) to the underlying struct may be
            /// alive while the returned `&mut` is in use.
            #[inline(always)]
            pub unsafe fn get_inner_mut(&self) -> &mut #type_name {
                &mut *self.inner.get()
            }

            #[inline(always)]
            /// Borrow the native struct.
            /// # Safety
            /// No Rust or native call may mutate the struct while this reference
            /// is alive. Shared wrappers also require coordination with other threads.
            pub unsafe fn get_inner_ref(&self) -> & #type_name {
                // Async resources can be released by a successful native poll.
                unsafe { self.inner.get().as_ref() }.expect("native resource was released")
            }
        }

        #ref_impl

        #wrap_impls

        impl<S: ResourceStorage<#type_name>> From<#class_name<S>> for *mut #type_name {
            #[inline]
            fn from(value: #class_name<S>) -> Self {
                value.get_inner()
            }
        }

        impl<S: ResourceStorage<#type_name>> From<&#class_name<S>> for *mut #type_name {
            #[inline]
            fn from(value: &#class_name<S>) -> Self {
                value.get_inner()
            }
        }

        #copy_out_impl

        #(#additional_impls)*

        #async_impls
        #default_impl
        #tests
        #convenience_methods
        #common_code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api_methods(bindings: &CBinding, native: &str) -> BTreeMap<String, syn::ImplItemFn> {
        let generated = generate_rust_code(
            &bindings.wrappers[native],
            &bindings.wrappers,
            false,
            false,
            true,
            &bindings.handlers,
            &bindings.constants,
        );
        syn::parse2::<syn::File>(generated)
            .unwrap()
            .items
            .into_iter()
            .filter_map(|item| match item {
                Item::Impl(item) => Some(item.items),
                _ => None,
            })
            .flatten()
            .filter_map(|item| match item {
                ImplItem::Fn(item) => Some((item.sig.ident.to_string(), item)),
                _ => None,
            })
            .collect()
    }

    fn assert_safe(methods: &BTreeMap<String, syn::ImplItemFn>, name: &str, safe: bool) {
        let method = methods
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}; generated {:?}", methods.keys().collect_vec()));
        assert_eq!(matches!(method.sig.safety, syn::Safety::Default), safe, "{name}");
    }

    #[test]
    fn native_value_apis_resolve_scalar_aliases_and_enums() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/media-driver.rs");
        let bindings = crate::parse_bindings(&path);
        for native in [
            "in_addr",
            "sockaddr",
            "timespec",
            "aeron_frame_header_t",
            "aeron_status_message_header_t",
            "aeron_driver_uri_subscription_params_t",
            "aeron_retransmit_action_t",
        ] {
            let methods = api_methods(&bindings, native);
            for name in [
                "new",
                "clone_struct",
                "from_native",
                "new_zeroed_on_heap",
                "new_zeroed_on_stack",
            ] {
                assert_safe(&methods, name, true);
            }
            assert_safe(&methods, "from_ptr", false);
        }
        for native in ["aeron_clock_cache_t", "aeron_cnc_metadata_t"] {
            let methods = api_methods(&bindings, native);
            assert_safe(&methods, "new_zeroed_on_stack", true);
            assert_safe(&methods, "from_native", true);
        }
        for native in ["aeron_uri_t", "aeron_agent_runner_t"] {
            let methods = api_methods(&bindings, native);
            assert_safe(&methods, "from_native", false);
            assert_safe(&methods, "new_zeroed_on_stack", false);
        }
    }

    #[test]
    fn invalid_enum_zero_has_no_zeroed_constructor() {
        let source = syn::parse_file("enum E { A = 1 } type Alias = E; struct value_t { tag: Alias }").unwrap();
        let wrapper = CWrapper {
            class_name: "Value".into(),
            type_name: "value_t".into(),
            native_type: "value_t".into(),
            fields: vec![arg("tag", "Alias")],
            native_types: std::sync::Arc::new(crate::NativeTypeMetadata::from_items(&source.items)),
            ..Default::default()
        };
        let bindings = CBinding {
            wrappers: BTreeMap::from([("value_t".into(), wrapper)]),
            ..Default::default()
        };
        let methods = api_methods(&bindings, "value_t");
        assert_safe(&methods, "new", true);
        assert_safe(&methods, "from_native", true);
        assert!(!methods.contains_key("new_zeroed_on_heap"));
        assert!(!methods.contains_key("new_zeroed_on_stack"));
    }

    #[test]
    fn native_pointer_contracts_distinguish_ignored_inputs_and_live_resources() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/media-driver.rs");
        let bindings = crate::parse_bindings(&path);
        let idle = api_methods(&bindings, "aeron_idle_strategy_t");
        for name in ["yielding_idle", "busy_spinning_idle", "noop_idle"] {
            assert_safe(&idle, name, true);
        }
        for name in ["sleeping_idle", "backoff_idle"] {
            assert_safe(&idle, name, false);
        }
        let driver = api_methods(&bindings, "aeron_driver_t");
        for name in [
            "termination_validator_default_allow",
            "termination_validator_default_deny",
        ] {
            assert_safe(&driver, name, true);
        }
        let manager = api_methods(&bindings, "aeron_counters_manager_t");
        assert_safe(&manager, "free", true);
        assert_safe(&manager, "init", false);
        assert_safe(&manager, "aeron_network_publication_close", false);
        let conductor = api_methods(&bindings, "aeron_driver_conductor_t");
        assert_safe(&conductor, "on_client_close", true);
        let context = api_methods(
            &crate::parse_bindings(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/archive.rs")),
            "aeron_archive_persistent_subscription_context_t",
        );
        assert_safe(&context, "set_state_counter", false);
    }

    #[test]
    fn scalar_pointer_results_preserve_raw_pointers() {
        for native in ["* mut i64", "* mut u32", "* mut :: std :: os :: raw :: c_int"] {
            let result = ReturnType::new(arg("value", native), BTreeMap::new());
            assert_eq!(result.get_new_return_type(false, false).to_string(), native);
            assert!(result.get_new_return_type(false, true).to_string().starts_with("& mut"));
            assert_eq!(
                result
                    .handle_c_to_rs_return(quote! { result }, false, false)
                    .to_string(),
                "result"
            );
        }
    }

    fn arg(name: &str, c_type: &str) -> Arg {
        Arg {
            name: name.to_string(),
            c_type: c_type.to_string(),
            processing: ArgProcessing::Default,
        }
    }

    #[test]
    fn unknown_embedded_types_are_not_assumed_pointer_free() {
        for c_type in [
            "aeron_uri_stct__bindgen_ty_1",
            "[ * mut u8 ; 4usize ]",
            "[ aeron_uri_param_t ; 2usize ]",
            "Option < unsafe extern \"C\" fn () >",
            "future_native_type_t",
        ] {
            let wrapper = CWrapper {
                fields: vec![arg("value", c_type)],
                ..Default::default()
            };
            assert!(wrapper.borrows_native_storage(&BTreeMap::new()), "{c_type}");
        }
    }

    #[test]
    fn integer_arrays_and_nested_plain_structs_are_pointer_free() {
        let nested = CWrapper {
            fields: vec![arg("bytes", "[u8; 1024usize]"), arg("id", "i64")],
            ..Default::default()
        };
        let wrappers = BTreeMap::from([("plain_t".into(), nested)]);
        let wrapper = CWrapper {
            fields: vec![arg("values", "[plain_t; 2usize]")],
            ..Default::default()
        };
        assert!(!wrapper.borrows_native_storage(&wrappers));
    }

    #[test]
    fn uri_union_constructor_and_copy_require_unsafe() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let bindings = crate::parse_bindings(&path);
        let wrapper = &bindings.wrappers["aeron_uri_t"];
        let generated = generate_rust_code(
            wrapper,
            &bindings.wrappers,
            false,
            false,
            true,
            &bindings.handlers,
            &bindings.constants,
        )
        .to_string();
        assert!(generated.contains("pub unsafe fn new ("));
        assert!(generated.contains("pub unsafe fn clone_struct ("));
    }

    #[test]
    fn debug_avoids_expired_storage_and_reads_text_within_the_call() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let bindings = crate::parse_bindings(&path);
        for (name, present, absent) in [
            ("aeron_async_add_publication_t", None, "self . get_"),
            ("aeron_uri_param_t", Some("self . key ()"), "unsafe"),
            ("aeron_context_t", Some("self . get_dir ()"), "get_dir_owned"),
        ] {
            let generated = generate_rust_code(
                &bindings.wrappers[name],
                &bindings.wrappers,
                false,
                false,
                true,
                &bindings.handlers,
                &bindings.constants,
            );
            let file = syn::parse2::<syn::File>(generated).unwrap();
            let debug = file
                .items
                .iter()
                .find_map(|item| match item {
                    syn::Item::Impl(i)
                        if i.trait_
                            .as_ref()
                            .is_some_and(|(path, _)| path.segments.last().unwrap().ident == "Debug") =>
                    {
                        Some(i)
                    }
                    _ => None,
                })
                .unwrap()
                .to_token_stream()
                .to_string();
            if let Some(present) = present {
                assert!(debug.contains(present), "{name}: {debug}");
            }
            assert!(!debug.contains(absent), "{name}: {debug}");
        }
    }

    #[test]
    fn debug_preserves_safe_fields_and_immutable_queries() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let bindings = crate::parse_bindings(&path);
        for (native, expected) in [
            ("aeron_agent_runner_t", vec!["role_name", "running"]),
            ("aeron_frame_header_t", vec!["frame_length", "type_"]),
            ("aeron_publication_t", vec!["get_constants", "channel"]),
            ("aeron_subscription_t", vec!["get_constants"]),
        ] {
            let generated = generate_rust_code(
                &bindings.wrappers[native],
                &bindings.wrappers,
                false,
                false,
                true,
                &bindings.handlers,
                &bindings.constants,
            );
            let file = syn::parse2::<syn::File>(generated).unwrap();
            let debug = file
                .items
                .iter()
                .find_map(|item| match item {
                    syn::Item::Impl(item)
                        if item
                            .trait_
                            .as_ref()
                            .is_some_and(|(path, _)| path.segments.last().unwrap().ident == "Debug") =>
                    {
                        Some(item.to_token_stream().to_string())
                    }
                    _ => None,
                })
                .unwrap();
            for name in expected {
                assert!(debug.contains(&format!("self . {name} ()")), "{native}: {debug}");
            }
            assert!(!debug.contains("_owned"), "{native}: {debug}");
        }
    }

    #[test]
    fn driver_suppliers_require_unsafe_and_manual_methods_are_not_generated() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/media-driver.rs");
        let mut bindings = crate::parse_bindings(&path);
        bindings.apply_method_overrides(crate::MEDIA_DRIVER_METHOD_OVERRIDES);
        let generate = |native: &str| {
            generate_rust_code(
                &bindings.wrappers[native],
                &bindings.wrappers,
                false,
                false,
                true,
                &bindings.handlers,
                &bindings.constants,
            )
            .to_string()
        };
        let context = generate("aeron_driver_context_t");
        for name in [
            "set_unicast_flowcontrol_supplier",
            "set_multicast_flowcontrol_supplier",
            "set_congestioncontrol_supplier",
            "set_name_resolver_supplier",
        ] {
            assert!(context.contains(&format!("pub unsafe fn {name}")), "{name}");
        }
        let driver = generate("aeron_driver_t");
        for name in ["start", "main_do_work", "main_idle_strategy"] {
            assert!(!driver.contains(&format!("pub fn {name} (")), "{name}");
        }
        assert!(!driver.contains("# [derive (Clone)] pub struct AeronDriver"));
    }

    #[test]
    fn driver_supplier_safety_preserves_client_and_archive_callback_signatures() {
        let cases: &[(&str, &str, &[&str])] = &[
            ("client.rs", "aeron_t", &["set_fprintf_handler", "is_driver_active"]),
            ("archive.rs", "aeron_t", &["set_fprintf_handler", "is_driver_active"]),
            ("archive.rs", "aeron_archive_context_t", &["set_credentials_supplier"]),
        ];
        for (file, native, methods) in cases {
            let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("bindings")
                .join(file);
            let bindings = crate::parse_bindings(&path);
            let generated = generate_rust_code(
                &bindings.wrappers[*native],
                &bindings.wrappers,
                false,
                false,
                true,
                &bindings.handlers,
                &bindings.constants,
            )
            .to_string();
            for name in *methods {
                assert!(generated.contains(&format!("pub fn {name}")), "{file}: {name}");
            }
        }
    }

    #[test]
    fn raw_pointer_methods_and_closure_variants_require_unsafe() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/client.rs");
        let bindings = crate::parse_bindings(&path);
        let generated = generate_rust_code(
            &bindings.wrappers["aeron_uri_t"],
            &bindings.wrappers,
            false,
            false,
            true,
            &bindings.handlers,
            &bindings.constants,
        )
        .to_string();
        assert!(generated.contains("pub unsafe fn parse_params <"));
        assert!(generated.contains("pub unsafe fn parse_params_fn <"));
    }

    #[test]
    fn archive_ownership_transfers_are_not_generated_as_borrowed_getters() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/archive.rs");
        let bindings = crate::parse_bindings(&path);
        let generated = generate_rust_code(
            &bindings.wrappers["aeron_archive_t"],
            &bindings.wrappers,
            false,
            false,
            true,
            &bindings.handlers,
            &bindings.constants,
        )
        .to_string();
        assert!(!generated.contains("fn get_and_own_"));
    }

    /// Every (buffer, length) slice pair passes `len() as _`, so the call type-checks whether
    /// the C length is `usize` or `int32_t`
    #[test]
    fn slice_length_is_cast_to_the_c_length_type() {
        let cases: [(fn(Vec<Arg>) -> ArgProcessing, &str); 4] = [
            (ArgProcessing::StringWithLength, Arg::C_CHAR_STR),
            (ArgProcessing::StringWithLength, Arg::C_MUT_CHAR_STR),
            (ArgProcessing::ByteArrayWithLength, Arg::C_BYTE_ARRAY),
            (ArgProcessing::ByteArrayWithLength, Arg::C_BYTE_MUT_ARRAY),
        ];
        for (processing, buffer_type) in cases {
            for length_type in ["usize", "i32"] {
                let pair = vec![arg("buf", buffer_type), arg("buf_length", length_type)];
                let length = Arg {
                    processing: processing(pair),
                    ..arg("buf_length", length_type)
                };
                let return_type = ReturnType::new(length, BTreeMap::new());

                let positional = return_type.handle_rs_to_c_return(quote! {}, false).to_string();
                assert!(
                    positional.ends_with(", buf . len () as _"),
                    "{buffer_type} / {length_type}: {positional}"
                );
                let named = return_type.handle_rs_to_c_return(quote! {}, true).to_string();
                assert!(
                    named.ends_with(", buf_length : buf . len () as _"),
                    "{buffer_type} / {length_type}: {named}"
                );
            }
        }
    }
}

#[cfg(test)]
mod usability_attribute_tests {
    use super::{Arg, ArgProcessing, CWrapper, Method};
    use proc_macro2::{Span, TokenStream};
    use quote::quote;
    use std::collections::BTreeMap;

    fn ident(name: &str) -> proc_macro2::Ident {
        proc_macro2::Ident::new(name, Span::call_site())
    }

    fn arg(name: &str, c_type: &str) -> Arg {
        Arg {
            name: name.to_string(),
            c_type: c_type.to_string(),
            processing: ArgProcessing::Default,
        }
    }

    #[test]
    fn must_use_only_marks_plain_reads() {
        let is_must_use = |name: &str, no_arguments: bool, ret: TokenStream| {
            !CWrapper::getter_must_use(&ident(name), no_arguments, &ret).is_empty()
        };
        assert!(is_must_use("get_stream_id", true, quote! { i32 }));
        assert!(is_must_use("is_closed", true, quote! { bool }));
        // `Result` is already must-use and clippy rejects a second marker
        assert!(!is_must_use("get_constants", true, quote! { Result<i32, AeronCError> }));
        assert!(!is_must_use("get_stream_id", false, quote! { i32 }));
        assert!(!is_must_use("get_nothing", true, quote! { () }));
        assert!(!is_must_use("offer", true, quote! { i64 }));
    }

    #[test]
    fn track_caller_follows_expect_in_any_body() {
        let plain = quote! { self.inner.get() };
        let panics = quote! { value.expect("invariant") };
        assert!(CWrapper::track_caller_if_panics(&[&plain]).is_empty());
        assert!(!CWrapper::track_caller_if_panics(&[&plain, &panics]).is_empty());
    }

    #[test]
    fn constants_forwarders_skip_text_private_and_existing_names() {
        let constants = CWrapper {
            type_name: "aeron_x_constants_t".into(),
            class_name: "AeronXConstants".into(),
            fields: vec![
                arg("stream_id", "i32"),
                arg("session_id", "i32"),
                arg("channel", "* const :: std :: os :: raw :: c_char"),
                arg("_reserved", "i64"),
            ],
            ..Default::default()
        };
        let get_constants = Method {
            fn_name: "aeron_x_constants".into(),
            struct_method_name: "constants".into(),
            return_type: arg("", ":: std :: os :: raw :: c_int"),
            arguments: vec![
                arg("x", "* mut aeron_x_t"),
                arg("constants", "* mut aeron_x_constants_t"),
            ],
            docs: Default::default(),
        };
        let session_id = Method {
            struct_method_name: "session_id".into(),
            ..get_constants.clone()
        };
        let parent = CWrapper {
            type_name: "aeron_x_t".into(),
            class_name: "AeronX".into(),
            methods: vec![get_constants.clone(), session_id],
            ..Default::default()
        };
        let wrappers = BTreeMap::from([("aeron_x_constants_t".to_string(), constants)]);

        let mut generated = vec![];
        parent.add_constants_forwarders(&wrappers, &get_constants, &mut generated);

        let generated: String = generated.iter().map(ToString::to_string).collect();
        assert!(generated.contains("pub fn stream_id"), "{generated}");
        assert!(
            generated.contains("self . get_constants () ? . stream_id ()"),
            "{generated}"
        );
        assert!(!generated.contains("fn session_id"), "a direct getter already exists");
        assert!(!generated.contains("fn channel"), "text borrows the constants copy");
        assert!(!generated.contains("fn _reserved"));
    }
}
