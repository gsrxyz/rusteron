use crate::get_possible_wrappers;
#[allow(unused_imports)]
use crate::snake_to_pascal_case;
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote, ToTokens};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use std::str::FromStr;
use syn::{parse_str, ImplItem, Item, Type};

pub const COMMON_CODE: &str = include_str!("common.rs");
pub const CLIENT_BINDINGS: &str = include_str!("../bindings/client.rs");
pub const ARCHIVE_BINDINGS: &str = include_str!("../bindings/archive.rs");
pub const MEDIA_DRIVER_BINDINGS: &str = include_str!("../bindings/media-driver.rs");

#[derive(Debug, Clone, Default)]
pub struct CBinding {
    pub wrappers: BTreeMap<String, CWrapper>,
    pub methods: Vec<Method>,
    pub handlers: Vec<CHandler>,
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

/// C callback typedefs that are only ever invoked synchronously, during the FFI call
/// they are passed to (e.g. fragment handlers during `poll`). Everything not listed here
/// is treated as *retained*: the C client stores the `clientd` pointer and may invoke the
/// callback later (conductor thread), so the wrapper must keep the `Handler` alive by
/// cloning it into the registering resource's dependencies, and it is unsound to offer a
/// stack-closure `_once` variant for it.
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
            "i64", "u64", "f32", "f64", "i32", "i16", "u32", "u16", "bool", "usize", "isize",
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
    wrappers: BTreeMap<String, CWrapper>,
}

impl ReturnType {
    pub fn new(original_c_type: Arg, wrappers: BTreeMap<String, CWrapper>) -> Self {
        ReturnType {
            original: original_c_type,
            wrappers,
        }
    }

    pub fn get_new_return_type(&self, convert_errors: bool, use_ref_for_cwrapper: bool) -> TokenStream {
        if let ArgProcessing::Handler(_) = self.original.processing {
            if self.original.name.len() > 0 {
                if !self.original.is_mut_pointer() {
                    let new_type =
                        parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&self.original.c_type)))
                            .expect("Invalid class name in wrapper");
                    return quote! { Option<&Handler<#new_type>> };
                } else {
                    return quote! {};
                }
            }
        } else if let ArgProcessing::StringWithLength(_) = self.original.processing {
            if self.original.name.len() > 0 {
                if self.original.is_c_string() {
                    return quote! { &str };
                } else if self.original.is_mut_c_string() {
                    // return quote! { &mut str };
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

        if self.original.is_single_mut_pointer() {
            let type_name = self.original.split(" ").last().unwrap();
            if let Some(wrapper) = self.wrappers.get(type_name) {
                let new_type = parse_str::<Type>(&wrapper.class_name).expect("Invalid class name in wrapper");
                if use_ref_for_cwrapper {
                    return quote! { &#new_type };
                } else {
                    return quote! { #new_type };
                }
            }
        }
        if let Some(wrapper) = self.wrappers.get(&self.original.c_type) {
            let new_type = parse_str::<Type>(&wrapper.class_name).expect("Invalid class name in wrapper");
            return quote! { #new_type };
        }
        if convert_errors && self.original.is_c_raw_int() {
            return quote! { Result<i32, AeronCError> };
        }
        if self.original.is_c_string() {
            // if incoming argument use &CString
            if !convert_errors && use_ref_for_cwrapper {
                return quote! { &std::ffi::CStr };
            } else {
                return quote! { &str };
            }
        }
        let return_type: Type = parse_str(&self.original).expect("Invalid return type");
        if self.original.is_single_mut_pointer() && self.original.is_primitive() {
            let mut_type: Type =
                parse_str(&return_type.to_token_stream().to_string().replace("* mut ", "&mut ")).unwrap();
            return quote! { #mut_type };
        }
        quote! { #return_type }
    }

    pub fn handle_c_to_rs_return(&self, result: TokenStream, convert_errors: bool, use_self: bool) -> TokenStream {
        if let ArgProcessing::StringWithLength(_) = &self.original.processing {
            if !self.original.is_c_string_any() {
                return quote! {};
            }
        }
        if let ArgProcessing::ByteArrayWithLength(args) = &self.original.processing {
            if !self.original.is_byte_array() {
                return quote! {};
            } else {
                let star_const = &args[0].as_ident();
                let length = &args[1].as_ident();
                let me = if use_self {
                    quote! {self.}
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
                    return Err(AeronCError::from_c_code(result));
                } else {
                    return Ok(result)
                }
            }
        } else if self.original.is_c_string() {
            if let ArgProcessing::StringWithLength(args) = &self.original.processing {
                let length = &args[1].as_ident();
                return quote! { if #result.is_null() { ""} else { std::str::from_utf8_unchecked(std::slice::from_raw_parts(#result as *const u8, #length.try_into().unwrap()))}};
            } else {
                return quote! { if #result.is_null() { ""} else { unsafe { std::ffi::CStr::from_ptr(#result).to_str().unwrap_or("") } } };
            }
        } else if self.original.is_single_mut_pointer() && self.original.is_primitive() {
            return quote! {
                unsafe { &mut *#result }
            };
        } else {
            quote! { #result.into() }
        }
    }

    /// Generic bounds for a handler argument. `force_static` is used by constructors,
    /// where the handler is always cloned into the created resource's dependencies
    /// (requiring `'static` for the `dyn Any` storage). For plain methods, only
    /// *retained* handlers (see [`SYNC_HANDLER_TYPES`]) get the `'static` bound —
    /// synchronous handlers may borrow local state, and their `_once` closure variants
    /// rely on that.
    pub fn method_generics_for_where(&self, force_static: bool) -> Option<TokenStream> {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let new_type = parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&handler.c_type)))
                    .expect("Invalid class name in wrapper");
                let new_handler = parse_str::<Type>(&format!("{}Callback", snake_to_pascal_case(&handler.c_type)))
                    .expect("Invalid class name in wrapper");
                let needs_static = force_static || !is_sync_handler_type(&handler.c_type);
                return if needs_static {
                    Some(quote! {
                        #new_type: #new_handler + 'static
                    })
                } else {
                    Some(quote! {
                        #new_type: #new_handler
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
                let method_name = format_ident!("{}_callback", handler.c_type);
                let new_type =
                    parse_str::<Type>(&format!("{}HandlerImpl", snake_to_pascal_case(&self.original.c_type)))
                        .expect("Invalid class name in wrapper");
                if include_field_name {
                    return quote! {
                        #handler_name: { let callback: #handler_type = if #handler_name.is_none() { None } else { Some(#method_name::<#new_type>) }; callback },
                        #clientd_name: #handler_name.map(|m|m.as_raw()).unwrap_or_else(|| std::ptr::null_mut())
                    };
                } else {
                    return quote! {
                        { let callback: #handler_type = if #handler_name.is_none() { None } else { Some(#method_name::<#new_type>) }; callback },
                        #handler_name.map(|m|m.as_raw()).unwrap_or_else(|| std::ptr::null_mut())
                    };
                }
            } else {
                return quote! {};
            }
        }
        if let ArgProcessing::StringWithLength(handler_client) = &self.original.processing {
            if !self.original.is_c_string() {
                let array = handler_client.get(0).unwrap();
                let array_name = array.as_ident();
                let length_name = handler_client.get(1).unwrap().as_ident();
                if include_field_name {
                    return quote! {
                        #array_name: #array_name.as_ptr() as *const _,
                        #length_name: #array_name.len()
                    };
                } else {
                    return quote! {
                        #array_name.as_ptr() as *const _,
                        #array_name.len()
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
                        #length_name: #array_name.len()
                    };
                } else {
                    return quote! {
                        #array_name.as_ptr() as *mut _,
                        #array_name.len()
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
    pub class_name: String,
    pub type_name: String,
    pub without_name: String,
    pub fields: Vec<Arg>,
    pub methods: Vec<Method>,
    pub docs: BTreeSet<String>,
    /// Method names manually implemented in aeron_custom.rs — the generator skips these
    /// to avoid duplicate definitions.
    pub skipped_methods: BTreeSet<String>,
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
            // Only plain `impl TypeName { ... }` — no trait impls
            if impl_block.trait_.is_some() {
                continue;
            }
            let type_name = impl_block.self_ty.to_token_stream().to_string();
            let type_name = type_name.trim().to_string();
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
                    // Handlers - just show type
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
                    // For primitive types, show value. For pointers/structs, just show name:type
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
        additional_outer_impls: &mut Vec<TokenStream>,
        debug_fields: &mut Vec<TokenStream>,
    ) -> Vec<TokenStream> {
        self.methods
            .iter()
            .filter(|m| !m.arguments.iter().any(|arg| arg.is_double_mut_pointer()))
            .filter(|m| !self.skipped_methods.contains(&m.struct_method_name))
            .filter(|m| m.struct_method_name != "close")
            .map(|method| {

                let fn_name =
                    Ident::new(&method.struct_method_name, proc_macro2::Span::call_site());
                let return_type_helper =
                    ReturnType::new(method.return_type.clone(), wrappers.clone());
                let mut return_type = return_type_helper.get_new_return_type(true, false);
                let ffi_call = Ident::new(&method.fn_name, proc_macro2::Span::call_site());

                // One classification pass: every argument becomes a PlannedArg carrying its
                // signature/call/generic/registration/_once fragments (see plan.rs). All
                // emitters below consume the plan — no parallel loops to keep in sync.
                let plan = crate::plan::plan_method(method, &self.type_name, wrappers, closure_handlers);
                let uses_self = plan.uses_self;
                let owned_retained_handler = plan.owned_retained.clone();

                let generic_types: Vec<TokenStream> = plan.generics();
                let where_clause = if generic_types.is_empty() {
                    quote! {}
                } else {
                    quote! { <#(#generic_types),*> }
                };

                let fn_arguments: Vec<TokenStream> = plan.signatures();
                let mut arg_names: Vec<TokenStream> = plan.calls();
                let retained_handler_registrations: Vec<TokenStream> = plan.registrations();

                let mut converter = return_type_helper.handle_c_to_rs_return(quote! { result }, true, false);

                // Heap-allocate the owned callback and capture its raw clientd before the
                // FFI call, so the Handler itself stays available for registration/return.
                let handler_prelude = if let Some(arg) = &owned_retained_handler {
                    let name = arg.as_ident();
                    let raw_name = format_ident!("{}_raw", arg.name);
                    quote! {
                        let #name = #name.map(Handler::new);
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
                    return_type = quote! { Result<Option<Handler<#new_type>>, AeronCError> };
                    converter = quote! {
                        if result < 0 {
                            return Err(AeronCError::from_c_code(result));
                        } else {
                            return Ok(#name);
                        }
                    };
                }

                let mut method_docs: Vec<TokenStream> = get_docs(&method.docs, wrappers, Some(&fn_arguments) );

                if owned_retained_handler.is_some() {
                    method_docs.push(quote! { #[doc = ""] });
                    method_docs.push(quote! { #[doc = " The callback is retained by the C client; this resource keeps it alive automatically."] });
                    method_docs.push(quote! { #[doc = " Returns the created [`Handler`] for optional state access — safe to ignore. Closures with a matching signature are accepted directly."] });
                }

                // Generate logging expression for arguments
                let args_log_expr = Self::generate_arg_logging(&method.arguments, &arg_names);

                if uses_self && method.return_type.is_c_string_any() && method.arguments.len() == 1 {
                    let name = format_ident!("{}", method.struct_method_name);
                    debug_fields.push( quote! {
                            .field(stringify!(#name), &self.#name() )
                        } );
                }

                let possible_self = if uses_self  {
                    quote! { &self, }
                } else {
                    if return_type.to_string().eq("& str") {
                        return_type = quote! { &'static str  };
                        method_docs.push(quote! {#[doc = "SAFETY: this is static for performance reasons, so you should not store this without copying it!!"]});
                    }
                    quote! {}
                };



                let register_handlers = if uses_self && !retained_handler_registrations.is_empty() {
                    quote! { #(#retained_handler_registrations)* }
                } else {
                    quote! {}
                };

                let mut additional_methods = vec![];
                let set_closed = quote! {};

                Self::add_mut_string_methods_if_applicable(method, &fn_name, uses_self, &method_docs, &mut additional_methods);

                // getter methods
                Self::add_getter_instead_of_mut_arg_if_applicable(wrappers, method, &fn_name, &where_clause, &possible_self, &method_docs, &mut additional_methods, debug_fields);

                // `_once` stack-closure variant, emitted straight from the plan (sync-only:
                // a stack closure handed to a retained callback would dangle). Skipped when
                // aeron_custom.rs hand-writes a method of the same `<name>_once` name, so
                // custom code can override/deprecate a generated `_once` variant.
                if plan.once_capable
                    && !self
                        .skipped_methods
                        .contains(&format!("{}_once", method.struct_method_name))
                {
                    let once_fn_name = format_ident!("{}_once", fn_name);
                    let once_generics = plan.once_generics();
                    let once_where = if once_generics.is_empty() {
                        quote! {}
                    } else {
                        quote! { <#(#once_generics),*> }
                    };
                    let once_args = plan.once_signatures();
                    let once_calls = plan.once_calls();
                    let once_log_expr = Self::generate_arg_logging(&method.arguments, &once_calls);
                    additional_methods.push(quote! {
                        #[inline]
                        #(#method_docs)*
                        ///
                        ///
                        /// **Stack-borrowed closure** (`_once` variant): the `FnMut` closure lives on the
                        /// caller's stack and is borrowed for this call only — the callback fires
                        /// synchronously inside the call, so nothing is heap-allocated, nothing is stored,
                        /// and the closure may borrow local state. Prefer this over the retained
                        /// [`Handler`]-based form on the hot path; only generated for callbacks the C
                        /// client does not retain (i.e. not stored for later firing).
                        ///
                        /// # Panics
                        ///
                        /// A panic inside the closure cannot unwind across the `extern "C"` callback
                        /// boundary and **aborts the process** (since Rust 1.81). Return early instead
                        /// of panicking in production fragment handlers.
                        pub fn #once_fn_name #once_where(#possible_self #(#once_args),*) -> #return_type {
                            #set_closed
                            unsafe {
                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #once_log_expr
                                );

                                let result = #ffi_call(#(#once_calls),*);

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

                // in aeron some methods return error code but have &mut primitive
                // ideally we should return that primitive instead of forcing user to pass it in
                if single_mut_field {
                    let mut_field = mut_primitivies.first().unwrap();
                    let rt: Type = parse_str(mut_field.c_type.split_whitespace().last().unwrap()).unwrap();
                    let return_type = quote! { Result<#rt, AeronCError> };

                    let fn_arguments= fn_arguments.into_iter().filter(|arg| {!arg.to_string().contains("& mut ")})
                        .collect_vec();

                    let idx = arg_names.iter().enumerate()
                        .filter(|(_, arg)| arg.to_string().ends_with("* mut _"))
                        .map(|(i, _)| i)
                        .next().unwrap();

                    arg_names[idx] = quote! { &mut mut_result };

                    let mut first = true;
                    let mut method_docs = method_docs.iter()
                        .filter(|d| !d.to_string().contains("# Return"))
                        .map(|d| {
                            let mut string = d.to_string();
                            string = string.replace("# Parameters", "");
                            if string.contains("out param") {
                                TokenStream::from_str(&string.replace("- `", "\n# Return\n`")).unwrap()
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
                        pub fn #fn_name #where_clause(#possible_self #(#fn_arguments),*) -> #return_type {
                            #set_closed
                            unsafe {
                                let mut mut_result: #rt = Default::default();

                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #args_log_expr
                                );

                                let err_code = #ffi_call(#(#arg_names),*);
                                #register_handlers

                                #[cfg(feature = "log-c-bindings")]
                                log::info!("  -> err_code = {:?}, result = {:?}", err_code, mut_result);

                                if err_code < 0 {
                                    return Err(AeronCError::from_c_code(err_code));
                                } else {
                                    return Ok(mut_result);
                                }
                            }
                        }

                        #(#additional_methods)*
                    }
                } else {
                    quote! {
                        #[inline]
                        #(#method_docs)*
                        pub fn #fn_name #where_clause(#possible_self #(#fn_arguments),*) -> #return_type {
                            #set_closed
                            #handler_prelude
                            unsafe {
                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #args_log_expr
                                );

                                let result = #ffi_call(#(#arg_names),*);
                                #register_handlers

                                #[cfg(feature = "log-c-bindings")]
                                log::info!("  -> {:?}", result);

                                #converter
                            }
                        }

                        #(#additional_methods)*
                    }
                }
            })
            .collect()
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

    fn add_getter_instead_of_mut_arg_if_applicable(
        wrappers: &BTreeMap<String, CWrapper>,
        method: &Method,
        fn_name: &Ident,
        where_clause: &TokenStream,
        possible_self: &TokenStream,
        method_docs: &Vec<TokenStream>,
        additional_methods: &mut Vec<TokenStream>,
        debug_fields: &mut Vec<TokenStream>,
    ) {
        if ["constants", "buffers", "values"]
            .iter()
            .any(|name| method.struct_method_name == *name)
            && method.arguments.len() == 2
        {
            let rt = ReturnType::new(method.arguments[1].clone(), wrappers.clone());
            let return_type = rt.get_new_return_type(false, false);
            let getter_method = format_ident!("get_{}", fn_name);
            let method_docs = method_docs
                .iter()
                .cloned()
                .take_while(|t| !t.to_string().contains(" Parameter"))
                .collect_vec();
            additional_methods.push(quote! {
                #[inline]
                #(#method_docs)*
                pub fn #getter_method #where_clause(#possible_self) -> Result<#return_type, AeronCError> {
                    let result = #return_type::new_zeroed_on_stack();
                    self.#fn_name(&result)?;
                    Ok(result)
                }
            });
            debug_fields.push(quote! {
                .field(stringify!(#fn_name), &self.#getter_method() )
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
        unsafe {
            let capacity = dst_truncate_to_capacity.capacity();
            let vec = dst_truncate_to_capacity.as_mut_vec();
            vec.set_len(capacity);
            let result = self.#fn_name(vec.as_mut_ptr() as *mut _, capacity)?;
            let mut len = 0;
            loop {
                if len == capacity {
                    break;
                }
                let val = vec[len];
                if val == 0 {
                    break;
                }
                len += 1;
            }
            vec.set_len(len);
            Ok(result)
        }
    }
                        });
            }
        }
    }

    /// Generate the fields / getters
    fn generate_fields(
        &self,
        cwrappers: &BTreeMap<String, CWrapper>,
        debug_fields: &mut Vec<TokenStream>,
    ) -> Vec<TokenStream> {
        self.fields
            .iter()
            .filter(|arg| {
                !arg.name.starts_with("_") && !self.methods.iter().any(|m| m.struct_method_name.as_str() == arg.name)
            })
            .map(|arg| {
                let field_name = &arg.name;
                let fn_name = Ident::new(field_name, proc_macro2::Span::call_site());

                let mut arg = arg.clone();
                // for mut strings return just &str not &mut str
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
                let converter = rt.handle_c_to_rs_return(quote! { self.#fn_name }, false, true);

                if rt.original.is_primitive()
                    || rt.original.is_c_string_any()
                    || rt.original.is_byte_array()
                    || cwrappers.contains_key(&rt.original.c_type)
                {
                    if !rt.original.is_any_pointer() || rt.original.is_c_string_any() {
                        debug_fields.push(quote! { .field(stringify!(#fn_name), &self.#fn_name()) });
                    }
                }

                quote! {
                    #[inline]
                    pub fn #fn_name(&self) -> #return_type {
                        #converter
                    }
                }
            })
            .filter(|t| !t.is_empty())
            .collect()
    }

    /// Generate the constructor for the struct
    fn generate_constructor(
        &self,
        wrappers: &BTreeMap<String, CWrapper>,
        constructor_fields: &mut Vec<TokenStream>,
        new_ref_set_none: &mut Vec<TokenStream>,
    ) -> Vec<TokenStream> {
        let constructors = self
            .methods
            .iter()
            .filter(|m| m.arguments.iter().any(|arg| arg.is_double_mut_pointer()))
            .map(|method| {
                let init_fn = format_ident!("{}", method.fn_name);
                let close_method = self.find_close_method(method);
                let found_close = close_method.is_some()
                    && close_method.unwrap().return_type.is_c_raw_int()
                    && close_method.unwrap() != method
                    && close_method
                        .unwrap()
                        .arguments
                        .iter()
                        .skip(1)
                        .all(|a| method.arguments.iter().any(|a2| a.name == a2.name));
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
                    let lets: Vec<TokenStream> = Self::lets_for_copying_arguments(wrappers, &method.arguments, true);

                    constructor_fields.clear();
                    constructor_fields.extend(Self::constructor_fields(wrappers, &method.arguments, &self.class_name));

                    let new_ref_args = Self::new_args(wrappers, &method.arguments, &self.class_name, false);

                    new_ref_set_none.clear();
                    new_ref_set_none.extend(Self::new_args(wrappers, &method.arguments, &self.class_name, true));

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
                                    ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, true);
                                if arg_type.clone().into_token_stream().is_empty() {
                                    None
                                } else {
                                    Some(quote! { #arg_name: #arg_type })
                                }
                            }
                        })
                        .filter(|t| !t.is_empty())
                        .collect();

                    let fn_name = format_ident!(
                        "{}",
                        method
                            .struct_method_name
                            .replace("init", "new")
                            .replace("create", "new")
                    );

                    let generic_types: Vec<TokenStream> = method
                        .arguments
                        .iter()
                        .flat_map(|arg| {
                            ReturnType::new(arg.clone(), wrappers.clone())
                                .method_generics_for_where(true)
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
                    // given (e.g. fragment assemblers keep their delegate), so clone each
                    // Handler into the new resource's dependencies to keep it alive until
                    // the C resource is closed.
                    let handler_deps: Vec<TokenStream> = Self::handler_dependency_registrations(&method.arguments);

                    // Generate logging expression token stream (will be evaluated in closure)
                    let init_log_expr_tokens = Self::generate_arg_logging(&method.arguments, &init_args);
                    // Generate logging for close method arguments
                    let close_log_expr_tokens = if let Some(close_m) = close_method {
                        Self::generate_arg_logging(&close_m.arguments, &close_args)
                    } else {
                        quote! { "" }
                    };

                    // `async_add_destination` handles must NOT run `async_remove_destination`
                    // on drop: removing the destination is a separate, destructive operation —
                    // not the destructor of the async-add handle (the C client frees the async
                    // struct itself once its poll completes). Pairing them made dropping the
                    // poll handle silently tear the destination down again, with a dangling
                    // uri pointer to boot.
                    let cleanup_tokens = if method.fn_name.contains("_destination") {
                        quote! { None }
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
                        pub fn #fn_name #where_clause(#(#new_args),*) -> Result<Self, AeronCError> {
                            #(#lets)*
                            // new by using constructor
                            let resource_constructor = ManagedCResource::new(
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

                            let result = Self {
                                inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource_constructor)),
                                #(#new_ref_args)*
                            };
                            #(#handler_deps)*
                            Ok(result)
                        }
                    }
                } else {
                    quote! {}
                }
            })
            .collect_vec();

        let no_constructor = constructors.iter().map(|x| x.to_string()).join("").trim().is_empty();
        if no_constructor {
            let type_name = format_ident!("{}", self.type_name);
            let zeroed_impl = quote! {
                #[inline]
                /// creates zeroed struct where the underlying c struct is on the heap
                pub fn new_zeroed_on_heap() -> Self {
                    let resource = ManagedCResource::new(
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
                        inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
                    }
                }

                #[inline]
                /// creates zeroed struct where the underlying c struct is on the stack
                /// _(Use with care)_
                pub fn new_zeroed_on_stack() -> Self {
                    #[cfg(feature = "extra-logging")]
                    log::debug!("creating zeroed empty resource on stack {}", stringify!(#type_name));

                    Self {
                        inner: CResource::OwnedOnStack(std::mem::MaybeUninit::zeroed()),
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
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone()).get_new_return_type(false, true);
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
                            .method_generics_for_where(true)
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
                let lets: Vec<TokenStream> = Self::lets_for_copying_arguments(wrappers, &cloned_fields, false);

                // The C struct stores these callback/clientd fields directly, so keep the
                // Handler values alive alongside the resource.
                let field_handler_deps: Vec<TokenStream> = Self::handler_dependency_registrations(&self.fields);

                vec![quote! {
                    #[inline]
                    pub fn new #where_clause(#(#new_args),*) -> Result<Self, AeronCError> {
                        #(#lets)*
                        // no constructor in c bindings
                        let r_constructor = ManagedCResource::new(
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
                            inner: CResource::OwnedOnHeap(std::rc::Rc::new(r_constructor)),
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

                    // check if I need to make copy of object for reference counting
                    let fields = if arg.is_single_mut_pointer()
                        && wrappers.contains_key(arg.c_type.split_whitespace().last().unwrap())
                    {
                        let arg_copy = format_ident!("{}_copy", arg.name);
                        quote! {
                            let #arg_copy = #arg_name.clone();
                        }
                    } else {
                        quote! {}
                    };

                    let return_type = ReturnType::new(arg.clone(), wrappers.clone());

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

    fn constructor_fields(
        wrappers: &BTreeMap<String, CWrapper>,
        arguments: &Vec<Arg>,
        class_name: &String,
    ) -> Vec<TokenStream> {
        if class_name == "AeronAsyncDestination" {
            return vec![];
        }

        arguments
            .iter()
            .enumerate()
            .filter_map(|(_idx, arg)| {
                if arg.is_double_mut_pointer() {
                    None
                } else {
                    let arg_name = arg.as_ident();
                    let rtype = arg.as_type();
                    if arg.is_single_mut_pointer()
                        && wrappers.contains_key(arg.c_type.split_whitespace().last().unwrap())
                    {
                        let return_type = ReturnType::new(arg.clone(), wrappers.clone());
                        let return_type = return_type.get_new_return_type(false, false);

                        let arg_copy = format_ident!("_{}", arg.name);
                        Some(quote! {
                            #arg_copy: Option<#return_type>,
                        })
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    fn new_args(
        wrappers: &BTreeMap<String, CWrapper>,
        arguments: &Vec<Arg>,
        class_name: &String,
        set_none: bool,
    ) -> Vec<TokenStream> {
        if class_name == "AeronAsyncDestination" {
            return vec![];
        }

        arguments
            .iter()
            .enumerate()
            .filter_map(|(_idx, arg)| {
                if arg.is_double_mut_pointer() {
                    None
                } else {
                    let arg_name = arg.as_ident();
                    let rtype = arg.as_type();
                    if arg.is_single_mut_pointer()
                        && wrappers.contains_key(arg.c_type.split_whitespace().last().unwrap())
                    {
                        let arg_f = format_ident!("_{}", &arg.name);
                        let arg_copy = format_ident!("{}_copy", &arg.name);
                        if set_none {
                            Some(quote! {
                                #arg_f: None,
                            })
                        } else {
                            Some(quote! {
                                #arg_f: Some(#arg_copy),
                            })
                        }
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    fn find_close_method(&self, method: &Method) -> Option<&Method> {
        let mut close_method = None;

        // must have init, create or add method name
        if ["_init", "_create", "_add"]
            .iter()
            .all(|find| !method.fn_name.contains(find))
        {
            return None;
        }

        for name in ["_destroy", "_delete"] {
            let close_fn = format_ident!(
                "{}",
                method
                    .fn_name
                    .replace("_init", "_close")
                    .replace("_create", name)
                    .replace("_add_", "_remove_")
            );
            let method = self.methods.iter().find(|m| close_fn.to_string().contains(&m.fn_name));
            if method.is_some() {
                close_method = method;
                break;
            }
        }
        close_method
    }

    fn has_default_method(&self) -> bool {
        // AeronUriStringBuilder does not follow the normal convention so have additional check arg.is_single_mut_pointer() && m.fn_name.contains("_init_")
        let no_init_method = !self.methods.iter().any(|m| {
            m.arguments
                .iter()
                .any(|arg| arg.is_double_mut_pointer() || (arg.is_single_mut_pointer() && m.fn_name.contains("_init_")))
        });

        no_init_method && !self.fields.iter().any(|arg| arg.name.starts_with("_")) && !self.fields.is_empty()
    }

    fn generate_allocation_test(&self) -> TokenStream {
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

        if !has_c_constructor {
            tests.push(quote! {
                #[test]
                #[file_serial(global)]
                fn test_new_on_stack() {
                    crate::test_alloc::assert_no_allocation(|| {
                        for _ in 0..100 {
                            let _ = #class_name::new_zeroed_on_stack();
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

        if self.has_default_method() {
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
        // Previously this returned an empty map → duplicate-method emission.
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
        // Exact plain-name key — no surrounding whitespace, no path qualifiers.
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
        // NOTE: `is_primitive` is a SUFFIX match (`c_type.ends_with(primitive)`),
        // so a pointer-to-primitive like `* mut i32` also returns true. This is
        // the contract the generator relies on (it composes this with the
        // `is_single_mut_pointer` check elsewhere), so pin it here.
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
    docs.iter()
        .flat_map(|d| d.lines())
        .filter(|s| {
            arguments.is_none()
                || !s.contains("@param")
                || (s.contains("@param")
                    && arguments
                        .unwrap()
                        .iter()
                        .any(|a| s.contains(format!(" {}", a.to_string().split_whitespace().next().unwrap()).as_str())))
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

fn generate_stateless_handler_code(handler: &CHandler) -> TokenStream {
    let type_name_ident = format_ident!("{}", handler.type_name);
    let callback_fn_name = format_ident!("{}_callback", handler.type_name);
    let closure_type_name = format_ident!("{}Callback", snake_to_pascal_case(&handler.type_name));
    let logger_type_name = format_ident!("{}Logger", snake_to_pascal_case(&handler.type_name));
    let handle_method_name = format_ident!("handle_{}", &handler.type_name[..handler.type_name.len() - 2]);
    let closure_return_type = handler.return_type.as_type();

    // For stateless callbacks, the FnMut signature takes no args (other than &mut self)
    let (fn_mut_args, trait_args): (Vec<TokenStream>, Vec<TokenStream>) = handler
        .args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            let return_type = ReturnType::new(arg.clone(), BTreeMap::new());
            let type_name = return_type.get_new_return_type(false, false);
            let arg_name = format_ident!("arg_{}", i);

            let fn_mut_arg = if arg.is_single_mut_pointer() && arg.is_primitive() {
                let owned_type: Type = parse_str(arg.c_type.split_whitespace().last().unwrap()).unwrap();
                quote! { #owned_type }
            } else {
                quote! { #type_name }
            };

            let trait_arg = if type_name.is_empty() {
                quote! {}
            } else if arg.is_single_mut_pointer() && arg.is_primitive() {
                let owned_type: Type = parse_str(arg.c_type.split_whitespace().last().unwrap()).unwrap();
                quote! { #arg_name: #owned_type }
            } else {
                quote! { #arg_name: #type_name }
            };

            (fn_mut_arg, trait_arg)
        })
        .filter(|(fn_mut, trait_)| !fn_mut.is_empty() && !trait_.is_empty())
        .unzip();

    // Store the FnMut signature for use in wrapper code
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

        // Stub callback function for stateless handlers - panics when called
        #[allow(dead_code)]
        unsafe extern "C" fn #callback_fn_name<F: #closure_type_name>(#(#trait_args),*) -> #closure_return_type {
            unimplemented!("Stateless handler callback called - not supported")
        }
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

    // Check for stateless callbacks (no c_void parameter)
    // These cannot use closure pattern as there's no clientd to pass the closure through
    // Generate only the trait and logger, not the closure infrastructure
    let has_c_void_param = handler.args.iter().any(|a| a.is_c_void());
    let is_stateless = !has_c_void_param;

    if is_stateless {
        let closure_type_name = format_ident!("{}Callback", snake_to_pascal_case(&handler.type_name));
        // Set the FnMut signature for stateless handlers (needed by wrapper code generation)
        let fn_mut_args: Vec<TokenStream> = handler
            .args
            .iter()
            .map(|arg| {
                let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
                let type_name = return_type.get_new_return_type(false, false);
                if arg.is_single_mut_pointer() && arg.is_primitive() {
                    let owned_type: Type = parse_str(arg.c_type.split_whitespace().last().unwrap()).unwrap();
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
            // do not need to convert as its calling hour handler
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
                Some(return_type.handle_c_to_rs_return(quote! {#arg_name}, false, false))
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
            let type_name = return_type.get_new_return_type(false, false);
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
            let type_name = return_type.get_new_return_type(false, false);
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
            let type_name = return_type.get_new_return_type(false, false);
            if arg.is_single_mut_pointer() && arg.is_primitive() {
                let owned_type: Type = parse_str(arg.c_type.split_whitespace().last().unwrap()).unwrap();
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

    quote! {
        #(#doc_comments)*
        ///
        ///
        /// _(note you must copy any arguments that you use afterwards even those with static lifetimes)_
        pub trait #closure_type_name {
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

        unsafe impl Send for #logger_type_name {}
        unsafe impl Sync for #logger_type_name {}

        /// Any closure with the matching signature is a callback: methods that retain it
        /// heap-allocate it into a [`Handler`] owned by the registering resource.
        impl<F: FnMut(#(#fn_mut_args),*) -> #closure_return_type> #closure_type_name for F {
            #[inline]
            fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type {
                self(#(#wrapper_closure_args),*)
            }
        }

        /// Pass an existing [`Handler`] clone anywhere a callback value is expected, e.g.
        /// to share one callback instance across several registrations.
        impl<T: #closure_type_name> #closure_type_name for Handler<T> {
            #[inline]
            fn #handle_method_name(&mut self, #(#closure_args),*) -> #closure_return_type {
                use std::ops::DerefMut;
                self.deref_mut().#handle_method_name(#(#wrapper_closure_args),*)
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

        // #[no_mangle]
        #[allow(dead_code)]
        #(#doc_comments)*
        unsafe extern "C" fn #fn_name<F: #closure_type_name>(
            #(#args),*
        ) -> #closure_return_type
        {
            #[cfg(debug_assertions)]
            if #closure_name.is_null() {
                unimplemented!("closure should not be null")
            }
            #[cfg(feature = "extra-logging")]
            {
                log::debug!("calling {}", stringify!(#handle_method_name));
            }
            #[cfg(feature = "log-c-bindings")]
            log::debug!(
                "{}({}\n)",
                stringify!(#fn_name),
                [#(#arg_names_for_logging),*].join(", ")
            );
            let closure: &mut F = &mut *(#closure_name as *mut F);
            closure.#handle_method_name(#(#converted_args),*)
        }

        // #[no_mangle]
        #[allow(dead_code)]
        #(#doc_comments)*
        unsafe extern "C" fn #closure_fn_name<F: FnMut(#(#fn_mut_args),*) -> #closure_return_type>(
            #(#args),*
        ) -> #closure_return_type
        {
            #[cfg(debug_assertions)]
            if #closure_name.is_null() {
                unimplemented!("closure should not be null")
            }
            #[cfg(feature = "extra-logging")]
            {
                log::debug!("calling {}", stringify!(#closure_fn_name));
            }
            #[cfg(feature = "log-c-bindings")]
            log::debug!(
                "{}({}\n)",
                stringify!(#closure_fn_name),
                [#(#arg_names_for_logging),*].join(", ")
            );
            let closure: &mut F = &mut *(#closure_name as *mut F);
            closure(#(#converted_args),*)
        }

    }
}

pub fn generate_rust_code(
    wrapper: &CWrapper,
    wrappers: &BTreeMap<String, CWrapper>,
    include_common_code: bool,
    include_clippy: bool,
    include_aeron_client_registering_resource_t: bool,
    closure_handlers: &Vec<CHandler>,
) -> TokenStream {
    let class_name = Ident::new(&wrapper.class_name, proc_macro2::Span::call_site());
    let type_name = Ident::new(&wrapper.type_name, proc_macro2::Span::call_site());

    let mut additional_outer_impls = vec![];
    let mut debug_fields = vec![];

    let methods = wrapper.generate_methods(
        wrappers,
        closure_handlers,
        &mut additional_outer_impls,
        &mut debug_fields,
    );
    let tests = wrapper.generate_allocation_test();
    let mut constructor_fields = vec![];
    let mut new_ref_set_none = vec![];
    let constructor = wrapper.generate_constructor(wrappers, &mut constructor_fields, &mut new_ref_set_none);

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
                        .method_generics_for_where(true)
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
                        .method_generics_for_where(true)
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

            let async_dependancies = async_new_args
                .iter()
                .filter(|a| a.to_string().contains(" : Aeron") || a.to_string().contains(" : & Aeron"))
                .map(|e| {
                    let var_name = format_ident!("{}", e.to_string().split_whitespace().next().unwrap());
                    quote! {
                        result.inner.add_dependency(#var_name.clone());
                    }
                })
                .collect_vec();

            let mut async_handler_deps: Vec<TokenStream> =
                CWrapper::handler_dependency_registrations(&new_method.arguments);

            // The conductor thread can invoke retained callbacks (e.g. image lifecycle
            // handlers) for as long as the *client* lives — even if this async poller is
            // dropped without ever being polled. Anchor each handler to the client too, so
            // dropping an unpolled poller cannot free a callback C still points at.
            if let Some(client_arg) = async_new_args
                .iter()
                .find(|a| a.to_string().contains(" : Aeron") || a.to_string().contains(" : & Aeron"))
            {
                let client_var = format_ident!("{}", client_arg.to_string().split_whitespace().next().unwrap());
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
                // Aeron C close functions now take optional notification
                // callbacks and a clientd pointer.  Use Default::default()
                // for any arg beyond the resource pointer — it resolves to
                // `None` / `null_mut()` via type inference at the call site.
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

            quote! {
                    impl #main_class_name {
                        #[inline]
                        pub fn new #where_clause_main (#(#new_args),*) -> Result<Self, AeronCError> {
                            let resource = ManagedCResource::new(
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
                                inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
                            })
                        }
                    }

                    impl #client_type {
                        #[inline]
                        pub fn #client_type_method_name #where_clause_async(&self, #(#async_new_args_for_client),*) -> Result<#async_class_name, AeronCError> {
                            let mut result =  #async_class_name::new(self, #(#async_new_args_name_only),*);
                            if let Ok(result) = &mut result {
                                result.inner.add_dependency(self.clone());
                            }

                            result
                        }
                    }

                    impl #client_type {
                        #[inline]
                        pub fn #client_type_method_name_without_async #where_clause_async(&self #(
                    , #async_new_args_for_client)*,  timeout: std::time::Duration) -> Result<#main_class_name, AeronCError> {
                            let start = std::time::Instant::now();
                            loop {
                                if let Ok(poller) = #async_class_name::new(self, #(#async_new_args_name_only),*) {
                                    while start.elapsed() <= timeout  {
                                      if let Some(result) = poller.poll()? {
                                          return Ok(result);
                                      }
                                    #[cfg(debug_assertions)]
                                    std::thread::sleep(std::time::Duration::from_millis(10));
                                  }
                                }
                            if start.elapsed() > timeout {
                                log::error!("failed async poll for {:?}", self);
                                return Err(AeronErrorType::TimedOut.into());
                            }
                            #[cfg(debug_assertions)]
                            std::thread::sleep(std::time::Duration::from_millis(10));
                          }
            }
                    }

                    impl #async_class_name {
                        #[inline]
                        pub fn new #where_clause_async (#(#async_new_args),*) -> Result<Self, AeronCError> {
                            let resource_async = ManagedCResource::new(
                                move |ctx_field| unsafe {
                                    #[cfg(feature = "log-c-bindings")]
                                    {
                                        let log_args = #async_log_expr_tokens;
                                        log::info!("{}({})", stringify!(#new_method_name), log_args);
                                    }
                                    #new_method_name(#(#async_init_args),*)
                                },
                                None,
                                false,
                            )?;
                            let result = Self {
                                inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource_async)),
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
                                if inner.is_resource_released() {
                                    return Ok(None);
                                }
                            }

                            let mut result = #main_class_name::new(self);
                            if let Ok(result) = &mut result {
                                unsafe {
                                    for d in (&mut *self.inner.as_owned().unwrap().dependencies.get()).iter_mut() {
                                      result.inner.add_dependency(d.clone());
                                    }
                                }
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

                        #[inline]
                        pub fn poll_blocking(&self, timeout: std::time::Duration) -> Result<#main_class_name, AeronCError> {
                            if let Some(result) = self.poll()? {
                                return Ok(result);
                            }

                            let time = std::time::Instant::now();
                            while time.elapsed() < timeout {
                                if let Some(result) = self.poll()? {
                                    return Ok(result);
                                }
                                #[cfg(debug_assertions)]
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }
                            log::error!("failed async poll for {:?}", self);
                            Err(AeronErrorType::TimedOut.into())
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
            let close_resource_call = if close_deferred_if_shared {
                quote! { self.inner.close_resource_deferred_if_shared() }
            } else {
                quote! { self.inner.close_resource() }
            };
            // Consuming close(self): run the FFI close through the shared
            // ManagedCResource state, then drop this handle.  The cleanup
            // closure is taken exactly once across clones, and after
            // close(self) this binding is gone.
            if close_deferred_if_shared {
                additional_impls.push(quote! {
                    impl #class_name {
                        /// Releases this handle and closes the C client **once the last reference drops**.
                        ///
                        /// The C client owns and frees every child resource (publications,
                        /// subscriptions, counters) when it closes, so while children or clones
                        /// are alive this is deferred — equivalent to `drop` — and the surviving
                        /// handles remain fully usable. The C close runs when the final
                        /// reference (child or clone) is released.
                        pub fn close(self) -> Result<(), AeronCError> {
                            let result = #close_resource_call;
                            // Drop this handle (decrements Rc, releases deps).
                            drop(self);
                            result
                        }

                        /// Closes the C client **immediately**, even if children or clones are alive.
                        ///
                        /// Clones of *this* handle are safe afterwards (their shared pointer is
                        /// nulled). Child handles are not:
                        ///
                        /// # Safety
                        /// The C client frees every child resource (publications, subscriptions,
                        /// counters) during this call, so surviving child handles dangle. Any use
                        /// is use-after-free — **including their `Drop`**, which calls the C close
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
                        /// Closes this resource in the C client immediately; all clones of this
                        /// handle become closed (their pointer is nulled) and must not be used.
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
                        /// `Handler` alive until it has fired).
                        pub fn close_with_handler<AeronNotificationHandlerImpl: AeronNotificationCallback>(
                            self,
                            on_close_complete: Option<&Handler<AeronNotificationHandlerImpl>>,
                        ) -> Result<(), AeronCError> {
                            let result = self.inner.close_resource_with(move |ptr| unsafe {
                                #close_fn(
                                    *ptr,
                                    {
                                        let callback: aeron_notification_t = if on_close_complete.is_none() {
                                            None
                                        } else {
                                            Some(aeron_notification_t_callback::<AeronNotificationHandlerImpl>)
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
            quote! {
                #[doc = #doc]
            }
        })
        .collect();

    let fields = wrapper.generate_fields(&wrappers, &mut debug_fields);

    let default_impl =
        if wrapper.has_default_method() && !constructor.iter().map(|x| x.to_string()).join("").trim().is_empty() {
            // let default_method_call = if wrapper.has_any_methods() {
            //     quote! {
            //         #class_name::new_zeroed_on_heap()
            //     }
            //  } else {
            //     quote! {
            //         #class_name::new_zeroed_on_stack()
            //     }
            // };

            quote! {
                /// This will create an instance where the struct is zeroed, use with care
                impl Default for #class_name {
                    fn default() -> Self {
                        #class_name::new_zeroed_on_heap()
                    }
                }

                impl #class_name {
                    /// Regular clone just increases the reference count of underlying count.
                    /// `clone_struct` shallow copies the content of the underlying struct on heap.
                    ///
                    /// NOTE: if the struct has references to other structs these will not be copied
                    ///
                    /// Must be only used on structs which has no init/clean up methods.
                    /// So its dangerous to use with Aeron/AeronContext/AeronPublication/AeronSubscription
                    /// More intended for AeronArchiveRecordingDescriptor (note strings will not work as its a shallow copy)
                    pub fn clone_struct(&self) -> Self {
                        let copy = Self::default();
                        copy.get_inner_mut().clone_from(self.deref());
                        copy
                    }
                }
            }
        } else {
            quote! {}
        };

    quote! {
        #warning_code

        #(#class_docs)*
        #[derive(Clone)]
        pub struct #class_name {
            inner: CResource<#type_name>,
            #(#constructor_fields)*
        }

        impl core::fmt::Debug for  #class_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if self.inner.get().is_null() {
                    f.debug_struct(stringify!(#class_name))
                    .field("inner", &"null")
                    .finish()
                } else {
                    f.debug_struct(stringify!(#class_name))
                      .field("inner", &self.inner)
                      #(#debug_fields)*
                      .finish()
                }
            }
        }

        impl #class_name {
            #(#constructor)*
            #(#fields)*
            #(#methods)*

            #[inline(always)]
            pub fn get_inner(&self) -> *mut #type_name {
                self.inner.get()
            }

            #[inline(always)]
            pub fn get_inner_mut(&self) -> &mut #type_name {
                unsafe { &mut *self.inner.get() }
            }

            #[inline(always)]
            pub fn get_inner_ref(&self) -> & #type_name {
                unsafe { &*self.inner.get() }
            }
        }

        impl std::ops::Deref for #class_name {
            type Target = #type_name;

            fn deref(&self) -> &Self::Target {
                self.get_inner_ref()
            }
        }

        impl From<*mut #type_name> for #class_name {
            #[inline]
            fn from(value: *mut #type_name) -> Self {
                #class_name {
                    inner: CResource::Borrowed(value),
                    #(#new_ref_set_none)*
                }
            }
        }

        impl From<#class_name> for *mut #type_name {
            #[inline]
            fn from(value: #class_name) -> Self {
                value.get_inner()
            }
        }

        impl From<&#class_name> for *mut #type_name {
            #[inline]
            fn from(value: &#class_name) -> Self {
                value.get_inner()
            }
        }

        impl From<#class_name> for #type_name {
            #[inline]
            fn from(value: #class_name) -> Self {
                unsafe { *value.get_inner().clone() }
            }
        }

        impl From<*const #type_name> for #class_name {
            #[inline]
            fn from(value: *const #type_name) -> Self {
                #class_name {
                    inner: CResource::Borrowed(value as *mut #type_name),
                    #(#new_ref_set_none)*
                }
            }
        }

        impl From<#type_name> for #class_name {
            #[inline]
            fn from(value: #type_name) -> Self {
                #class_name {
                    inner: CResource::OwnedOnStack(MaybeUninit::new(value)),
                    #(#new_ref_set_none)*
                }
            }
        }

        #(#additional_impls)*

        #async_impls
        #default_impl
        #tests
        #common_code
    }
}
