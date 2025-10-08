use crate::get_possible_wrappers;
#[allow(unused_imports)]
use crate::snake_to_pascal_case;
use itertools::Itertools;
use log::info;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote, ToTokens};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::str::FromStr;
use syn::{parse_str, Type};

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

    pub fn get_new_return_type(
        &self,
        convert_errors: bool,
        use_ref_for_cwrapper: bool,
    ) -> TokenStream {
        if let ArgProcessing::Handler(_) = self.original.processing {
            if self.original.name.len() > 0 {
                if !self.original.is_mut_pointer() {
                    let new_type = parse_str::<Type>(&format!(
                        "{}HandlerImpl",
                        snake_to_pascal_case(&self.original.c_type)
                    ))
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
                let new_type =
                    parse_str::<Type>(&wrapper.class_name).expect("Invalid class name in wrapper");
                if use_ref_for_cwrapper {
                    return quote! { &#new_type };
                } else {
                    return quote! { #new_type };
                }
            }
        }
        if let Some(wrapper) = self.wrappers.get(&self.original.c_type) {
            let new_type =
                parse_str::<Type>(&wrapper.class_name).expect("Invalid class name in wrapper");
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
            let mut_type: Type = parse_str(
                &return_type
                    .to_token_stream()
                    .to_string()
                    .replace("* mut ", "&mut "),
            )
            .unwrap();
            return quote! { #mut_type };
        }
        quote! { #return_type }
    }

    pub fn handle_c_to_rs_return(
        &self,
        result: TokenStream,
        convert_errors: bool,
        use_self: bool,
    ) -> TokenStream {
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
                        if #me #star_const.is_null() { &[] as &[_]  } else { std::slice::from_raw_parts(#me #star_const, #me #length) }
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
            if let ArgProcessing::StringWithLength(args) = &self.original.processing {
                let length = &args[1].as_ident();
                return quote! { if #result.is_null() { ""} else { std::str::from_utf8_unchecked(std::slice::from_raw_parts(#result as *const u8, #length.try_into().unwrap()))}};
            } else {
                return quote! { if #result.is_null() { ""} else { unsafe { std::ffi::CStr::from_ptr(#result).to_str().unwrap() } } };
            }
        } else if self.original.is_single_mut_pointer() && self.original.is_primitive() {
            return quote! {
                unsafe { &mut *#result }
            };
        } else {
            quote! { #result.into() }
        }
    }

    pub fn method_generics_for_where(&self) -> Option<TokenStream> {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let new_type = parse_str::<Type>(&format!(
                    "{}HandlerImpl",
                    snake_to_pascal_case(&handler.c_type)
                ))
                .expect("Invalid class name in wrapper");
                let new_handler = parse_str::<Type>(&format!(
                    "{}Callback",
                    snake_to_pascal_case(&handler.c_type)
                ))
                .expect("Invalid class name in wrapper");
                return Some(quote! {
                    #new_type: #new_handler
                });
            }
        }
        None
    }

    pub fn method_generics_for_method(&self) -> Option<TokenStream> {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let new_type = parse_str::<Type>(&format!(
                    "{}HandlerImpl",
                    snake_to_pascal_case(&handler.c_type)
                ))
                .expect("Invalid class name in wrapper");
                return Some(quote! {
                    #new_type
                });
            }
        }
        None
    }

    pub fn handle_rs_to_c_return(
        &self,
        result: TokenStream,
        include_field_name: bool,
    ) -> TokenStream {
        if let ArgProcessing::Handler(handler_client) = &self.original.processing {
            if !self.original.is_mut_pointer() {
                let handler = handler_client.get(0).unwrap();
                let handler_name = handler.as_ident();
                let handler_type = handler.as_type();
                let clientd_name = handler_client.get(1).unwrap().as_ident();
                let method_name = format_ident!("{}_callback", handler.c_type);
                let new_type = parse_str::<Type>(&format!(
                    "{}HandlerImpl",
                    snake_to_pascal_case(&self.original.c_type)
                ))
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

    fn get_is_closed_method(&self) -> Option<Method> {
        self.find_unique_method("is_closed")
    }

    fn get_is_closed_method_quote(&self) -> TokenStream {
        if let Some(method) = self.get_is_closed_method() {
            let fn_name = format_ident!("{}", method.fn_name);
            quote! {
                Some(|c| unsafe{#fn_name(c)})
            }
        } else {
            quote! {
                None
            }
        }
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
            .map(|method| {
                if method.struct_method_name.contains("errmsg") {
                    info!("{}", method.fn_name);
                }
                let set_closed = if method.struct_method_name == "close" {
                    quote! {
                        if let Some(inner) = self.inner.as_owned() {
                            inner.close_already_called.set(true);
                        }
                    }
                } else {
                    quote! {}
                };

                let fn_name =
                    Ident::new(&method.struct_method_name, proc_macro2::Span::call_site());
                let return_type_helper =
                    ReturnType::new(method.return_type.clone(), wrappers.clone());
                let mut return_type = return_type_helper.get_new_return_type(true, false);
                let ffi_call = Ident::new(&method.fn_name, proc_macro2::Span::call_site());

                // Filter out arguments that are `*mut` of the struct's type
                let generic_types: Vec<TokenStream> = method
                    .arguments
                    .iter()
                    .flat_map(|arg| {
                        ReturnType::new(arg.clone(), wrappers.clone())
                            .method_generics_for_where()
                            .into_iter()
                    })
                    .collect_vec();
                let where_clause = if generic_types.is_empty() {
                    quote! {}
                } else {
                    quote! { <#(#generic_types),*> }
                };

                let fn_arguments: Vec<TokenStream> = method
                    .arguments
                    .iter()
                    .filter_map(|arg| {
                        let ty = &arg.c_type;
                        let t = if arg.is_single_mut_pointer() {
                            ty.split(" ").last().unwrap()
                        } else {
                            "notfound"
                        };
                        if let Some(matching_wrapper) = wrappers.get(t) {
                            if matching_wrapper.type_name == self.type_name {
                                None
                            } else {
                                let arg_name = arg.as_ident();
                                let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                                    .get_new_return_type(false, true);
                                if arg_type.is_empty() {
                                    None
                                } else {
                                    Some(quote! { #arg_name: #arg_type })
                                }
                            }
                        } else {
                            let arg_name = arg.as_ident();
                            let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                                .get_new_return_type(false, true);
                            if arg_type.is_empty() {
                                None
                            } else {
                                Some(quote! { #arg_name: #arg_type })
                            }
                        }
                    })
                    .filter(|t| !t.is_empty())
                    .collect();

                let mut uses_self = false;

                // Filter out argument names for the FFI call
                let mut arg_names: Vec<TokenStream> = method
                    .arguments
                    .iter()
                    .filter_map(|arg| {
                        let ty = &arg.c_type;
                        let t = if arg.is_single_mut_pointer() {
                            ty.split(" ").last().unwrap()
                        } else {
                            "notfound"
                        };
                        if let Some(_matching_wrapper) = wrappers.get(t) {
                            let field_name = arg.as_ident();
                            if ty.ends_with(self.type_name.as_str()) {
                                uses_self = true;
                                Some(quote! { self.get_inner() })
                            } else {
                                Some(quote! { #field_name.get_inner() })
                            }
                        } else {
                            let arg_name = arg.as_ident();
                            let arg_name = quote! { #arg_name };
                            let arg_name = ReturnType::new(arg.clone(), wrappers.clone())
                                .handle_rs_to_c_return(arg_name, false);
                            Some(quote! { #arg_name })
                        }
                    })
                    .filter(|t| !t.is_empty())
                    .collect();

                let converter = return_type_helper.handle_c_to_rs_return(quote! { result }, true, false);

                let mut method_docs: Vec<TokenStream> = get_docs(&method.docs, wrappers, Some(&fn_arguments) );

                // Generate logging expression for arguments
                let args_log_expr = Self::generate_arg_logging(&method.arguments, &arg_names);

                let possible_self = if uses_self  {
                    quote! { &self, }
                } else {
                    if return_type.to_string().eq("& str") {
                        return_type = quote! { &'static str  };
                        method_docs.push(quote! {#[doc = "SAFETY: this is static for performance reasons, so you should not store this without copying it!!"]});
                    }
                    quote! {}
                };



                let mut additional_methods = vec![];

                Self::add_mut_string_methods_if_applicable(method, &fn_name, uses_self, &method_docs, &mut additional_methods);

                // getter methods
                Self::add_getter_instead_of_mut_arg_if_applicable(wrappers, method, &fn_name, &where_clause, &possible_self, &method_docs, &mut additional_methods, debug_fields);

                Self::add_once_methods_for_handlers(closure_handlers, method, &fn_name, &return_type, &ffi_call, &where_clause, &fn_arguments, &mut arg_names, &converter, &possible_self, &method_docs, &mut additional_methods, &set_closed);

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

                                #[cfg(feature = "log-c-bindings")]
                                log::info!("  -> err_code = {:?}, result = {:?}", err_code, mut_result);

                                if err_code < 0 {
                                    return Err(AeronCError::from_code(err_code));
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
                            unsafe {
                                #[cfg(feature = "log-c-bindings")]
                                log::info!(
                                    "{}({})",
                                    stringify!(#ffi_call),
                                    #args_log_expr
                                );

                                let result = #ffi_call(#(#arg_names),*);

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

    fn add_once_methods_for_handlers(
        closure_handlers: &Vec<CHandler>,
        method: &Method,
        fn_name: &Ident,
        return_type: &TokenStream,
        ffi_call: &Ident,
        where_clause: &TokenStream,
        fn_arguments: &Vec<TokenStream>,
        arg_names: &mut Vec<TokenStream>,
        converter: &TokenStream,
        possible_self: &TokenStream,
        method_docs: &Vec<TokenStream>,
        additional_methods: &mut Vec<TokenStream>,
        set_closed: &TokenStream,
    ) {
        if method.arguments.iter().any(|arg| {
            matches!(arg.processing, ArgProcessing::Handler(_))
                && !method.fn_name.starts_with("set_")
                && !method.fn_name.starts_with("add_")
        }) {
            let fn_name = format_ident!("{}_once", fn_name);

            // replace type to be FnMut
            let mut where_clause = where_clause.to_string();

            for c in closure_handlers.iter() {
                if !c.closure_type_name.is_empty() {
                    where_clause = where_clause.replace(
                        &c.closure_type_name.to_string(),
                        &c.fn_mut_signature.to_string(),
                    );
                }
            }
            let where_clause = parse_str::<TokenStream>(&where_clause).unwrap();

            // replace arguments from Handler to Closure
            let fn_arguments = fn_arguments.iter().map(|arg| {
                let mut arg = arg.clone();
                let str = arg.to_string();
                if str.contains("& Handler ") {
                    // e.g. callback : Option < & Handler < AeronErrorLogReaderFuncHandlerImpl >>
                    let parts = str.split(" ").collect_vec();
                    let variable_name = parse_str::<TokenStream>(parts[0]).unwrap();
                    let closure_type = parse_str::<TokenStream>(parts[parts.len() - 2]).unwrap();
                    arg = quote! { mut #variable_name : #closure_type };
                }

                arg
            });

            // update code to directly call closure without need of box or handler
            let arg_names = arg_names.iter().map(|x| {
                let mut str = x.to_string()
                    .replace("_callback :: <", "_callback_for_once_closure :: <")
                    ;

                if str.contains("_callback_for_once_closure") {
                    /*
                        let callback: aeron_counters_reader_foreach_counter_func_t = if func.is_none() {
                                None
                            } else {
                                Some(
                                    aeron_counters_reader_foreach_counter_func_t_callback_for_once_closure::<
                                        AeronCountersReaderForeachCounterFuncHandlerImpl,
                                    >,
                                )
                            };
                            callback
                        },
                        func.map(|m| m.as_raw())
                            .unwrap_or_else(|| std::ptr::null_mut()),

                     */
                    let caps = regex::Regex::new(
                        r#"let callback\s*:\s*(?P<type>[\w_]+)\s*=\s*if\s*(?P<handler_var_name>[\w_]+)\s*\.\s*is_none\s*\(\).*Some\s*\(\s*(?P<callback>[\w_]+)\s*::\s*<\s*(?P<handler>[\w_]+)\s*>\s*\).*"#
                    )
                        .unwrap()
                        .captures(&str)
                        .expect(&format!("regex failed for {str}"));
                    let func_type = parse_str::<TokenStream>(&caps["type"]).unwrap();
                    let handler_var_name = parse_str::<TokenStream>(&caps["handler_var_name"]).unwrap();
                    let callback = parse_str::<TokenStream>(&caps["callback"]).unwrap();
                    let handler_type = parse_str::<TokenStream>(&caps["handler"]).unwrap();

                    let new_code = quote! {
                                Some(#callback::<#handler_type>),
                                &mut #handler_var_name as *mut _ as *mut std::os::raw::c_void
                            };
                    str = new_code.to_string();
                }

                parse_str::<TokenStream>(&str).unwrap()
            }).collect_vec();

            // Generate logging expression for arguments
            let args_log_expr = Self::generate_arg_logging(&method.arguments, &arg_names);

            additional_methods.push(quote! {
                #[inline]
                #(#method_docs)*
                ///
                ///
                /// _NOTE: aeron must not store this closure and instead use it immediately. If not you will get undefined behaviour,
                ///  use with care_
                pub fn #fn_name #where_clause(#possible_self #(#fn_arguments),*) -> #return_type {
                    #set_closed
                    unsafe {
                        #[cfg(feature = "log-c-bindings")]
                        log::info!(
                            "{}({})",
                            stringify!(#ffi_call),
                            #args_log_expr
                        );

                        let result = #ffi_call(#(#arg_names),*);

                        #[cfg(feature = "log-c-bindings")]
                        log::info!("  -> {:?}", result);

                        #converter
                    }
                }
            })
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
        debug_fields:  &mut Vec<TokenStream>,
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
            debug_fields.push( quote! {
                .field(stringify!(#fn_name), &self.#getter_method() )
            } );
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
                !arg.name.starts_with("_")
                    && !self
                        .methods
                        .iter()
                        .any(|m| m.struct_method_name.as_str() == arg.name)
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
                    if !rt.original.is_any_pointer() {
                        debug_fields
                            .push(quote! { .field(stringify!(#fn_name), &self.#fn_name()) });
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
            .filter(|m| m.arguments.iter().any(|arg| arg.is_double_mut_pointer() ))
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
                    let lets: Vec<TokenStream> =
                        Self::lets_for_copying_arguments(wrappers, &method.arguments, true);

                    constructor_fields.clear();
                    constructor_fields.extend(Self::constructor_fields(
                        wrappers,
                        &method.arguments,
                        &self.class_name,
                    ));

                    let new_ref_args =
                        Self::new_args(wrappers, &method.arguments, &self.class_name, false);

                    new_ref_set_none.clear();
                    new_ref_set_none.extend(Self::new_args(
                        wrappers,
                        &method.arguments,
                        &self.class_name,
                        true,
                    ));

                    let new_args: Vec<TokenStream> = method
                        .arguments
                        .iter()
                        .enumerate()
                        .filter_map(|(_idx, arg)| {
                            if arg.is_double_mut_pointer() {
                                None
                            } else {
                                let arg_name = arg.as_ident();
                                let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                                    .get_new_return_type(false, true);
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
                                .method_generics_for_where()
                                .into_iter()
                        })
                        .collect_vec();
                    let where_clause = if generic_types.is_empty() {
                        quote! {}
                    } else {
                        quote! { <#(#generic_types),*> }
                    };

                    let method_docs: Vec<TokenStream> =
                        get_docs(&method.docs, wrappers, Some(&new_args));

                    // Generate logging expression token stream (will be evaluated in closure)
                    let init_log_expr_tokens = Self::generate_arg_logging(&method.arguments, &init_args);
                    // Generate logging for close method arguments
                    let close_log_expr_tokens = if let Some(close_m) = close_method {
                        Self::generate_arg_logging(&close_m.arguments, &close_args)
                    } else {
                        quote! { "" }
                    };

                    let is_closed_method = self.get_is_closed_method_quote();
                    quote! {
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
                                Some(Box::new(move |ctx_field| unsafe {
                                    #[cfg(feature = "log-c-bindings")]
                                    {
                                        let log_args = #close_log_expr_tokens;
                                        log::info!("{}({})", stringify!(#close_fn), log_args);
                                    }
                                    #close_fn(#(#close_args),*)
                                } )),
                                false,
                                #is_closed_method,
                            )?;

                            Ok(Self {
                                inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource_constructor)),
                                #(#new_ref_args)*
                            })
                        }
                    }
                } else {
                    quote! {}
                }
            })
            .collect_vec();

        let no_constructor = constructors
            .iter()
            .map(|x| x.to_string())
            .join("")
            .trim()
            .is_empty();
        if no_constructor {
            let type_name = format_ident!("{}", self.type_name);
            let is_closed_method = self.get_is_closed_method_quote();

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
                        #is_closed_method
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
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
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
                            .method_generics_for_where()
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
                let lets: Vec<TokenStream> =
                    Self::lets_for_copying_arguments(wrappers, &cloned_fields, false);

                let is_closed_method = self.get_is_closed_method_quote();

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
                            #is_closed_method
                        )?;

                        Ok(Self {
                            inner: CResource::OwnedOnHeap(std::rc::Rc::new(r_constructor)),
                        })
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

                    if let ArgProcessing::StringWithLength(_args)
                    | ArgProcessing::ByteArrayWithLength(_args) =
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
            let method = self
                .methods
                .iter()
                .find(|m| close_fn.to_string().contains(&m.fn_name));
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
            m.arguments.iter().any(|arg| {
                arg.is_double_mut_pointer()
                    || (arg.is_single_mut_pointer() && m.fn_name.contains("_init_"))
            })
        });

        no_init_method
            && !self.fields.iter().any(|arg| arg.name.starts_with("_"))
            && !self.fields.is_empty()
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
                    && arguments.unwrap().iter().any(|a| {
                        s.contains(
                            format!(" {}", a.to_string().split_whitespace().next().unwrap())
                                .as_str(),
                        )
                    }))
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

            doc = wrappers.values().fold(doc, |acc, v| {
                acc.replace(&v.type_name, &format!("`{}`", v.class_name))
            });

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

pub fn generate_handlers(handler: &mut CHandler, bindings: &CBinding) -> TokenStream {
    if handler
        .args
        .iter()
        .any(|arg| arg.is_primitive() && arg.is_mut_pointer())
    {
        return quote! {};
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
        .unwrap()
        .name
        .clone();
    let closure_name = format_ident!("{}", closure);
    let closure_type_name = format_ident!("{}Callback", snake_to_pascal_case(&handler.type_name));
    let closure_return_type = handler.return_type.as_type();

    let logger_type_name = format_ident!("{}Logger", snake_to_pascal_case(&handler.type_name));

    let handle_method_name = format_ident!(
        "handle_{}",
        &handler.type_name[..handler.type_name.len() - 2]
    );

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
            if name != &closure {
                let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone());
                Some(return_type.handle_c_to_rs_return(quote! {#arg_name}, false, false))
            } else {
                None
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
                log_field_names.push({
                    Some(quote! { format!("{} : {:?}", stringify!(#field_name), #field_name) })
                });

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
                let owned_type: Type =
                    parse_str(arg.c_type.split_whitespace().last().unwrap()).unwrap();
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
            let return_type = ReturnType::new(arg.clone(), bindings.wrappers.clone())
                .get_new_return_type(false, false);
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

        impl Handlers {
            /// No handler is set i.e. None with correct type
            pub fn #no_method_name() -> Option<&'static Handler<#logger_type_name>> {
                None::<&Handler<#logger_type_name>>
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

    let methods = wrapper.generate_methods(wrappers, closure_handlers, &mut additional_outer_impls, &mut debug_fields);
    let mut constructor_fields = vec![];
    let mut new_ref_set_none = vec![];
    let constructor =
        wrapper.generate_constructor(wrappers, &mut constructor_fields, &mut new_ref_set_none);

    let async_impls = if wrapper.type_name.starts_with("aeron_async_")
        || wrapper.type_name.starts_with("aeron_archive_async_")
    {
        let new_method = wrapper
            .methods
            .iter()
            .find(|m| m.fn_name == wrapper.without_name);

        if let Some(new_method) = new_method {
            let main_type = &wrapper
                .type_name
                .replace("_async_", "_")
                .replace("_add_", "_");
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
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                            .get_new_return_type(false, true);
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
            let async_log_expr_tokens =
                CWrapper::generate_arg_logging(&new_method.arguments, &async_init_args);

            let generic_types: Vec<TokenStream> = new_method
                .arguments
                .iter()
                .flat_map(|arg| {
                    ReturnType::new(arg.clone(), wrappers.clone())
                        .method_generics_for_where()
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
                        .method_generics_for_where()
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
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                            .get_new_return_type(false, true);
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
                .filter(|a| {
                    a.to_string().contains(" : Aeron") || a.to_string().contains(" : & Aeron")
                })
                .map(|e| {
                    let var_name =
                        format_ident!("{}", e.to_string().split_whitespace().next().unwrap());
                    quote! {
                        result.inner.add_dependency(#var_name.clone());
                    }
                })
                .collect_vec();

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
                        let arg_type = ReturnType::new(arg.clone(), wrappers.clone())
                            .get_new_return_type(false, false);
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
            let poll_log_expr_tokens =
                CWrapper::generate_arg_logging(&poll_method.arguments, &init_args);

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
                                None,
                                false,
                                None,
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
                                None,
                            )?;
                            let result = Self {
                                inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource_async)),
                            };
                            #(#async_dependancies)*
                            Ok(result)
                        }

                        pub fn poll(&self) -> Result<Option<#main_class_name>, AeronCError> {

                            let mut result = #main_class_name::new(self);
                            if let Ok(result) = &mut result {
                                unsafe {
                                    for d in (&mut *self.inner.as_owned().unwrap().dependencies.get()).iter_mut() {
                                      result.inner.add_dependency(d.clone());
                                    }
                                    result.inner.as_owned().unwrap().auto_close.set(true);
                                }
                            }

                            match result {
                                Ok(result) => Ok(Some(result)),
                                Err(AeronCError {code }) if code == 0 => {
                                  Ok(None) // try again
                                }
                                Err(e) => Err(e)
                            }
                        }

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
        if !wrapper.methods.iter().any(|m| m.fn_name.contains("_init")) {
            let close_method_call = if close_method.arguments.len() > 1 {
                let ident = format_ident!("close_with_no_args");
                quote! {#ident}
            } else {
                let ident = format_ident!("{}", close_method.struct_method_name);
                quote! {#ident}
            };
            let is_closed_method = if wrapper.get_is_closed_method().is_some() {
                quote! { self.is_closed() }
            } else {
                quote! { false }
            };

            additional_impls.push(quote! {
                impl Drop for #class_name {
                    fn drop(&mut self) {
                        if let Some(inner) = self.inner.as_owned() {
                            if (inner.cleanup.is_none() ) && std::rc::Rc::strong_count(inner) == 1 && !inner.is_closed_already_called() {
                                if inner.auto_close.get() {
                                    log::info!("auto closing {self:?}");
                                    let result = self.#close_method_call();
                                    log::debug!("result {:?}", result);
                                } else {
                                    #[cfg(feature = "extra-logging")]
                                    log::warn!("{} not closed", stringify!(#class_name));
                                }
                            }
                        }
                    }
                }
            });
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

    let default_impl = if wrapper.has_default_method()
        && !constructor
            .iter()
            .map(|x| x.to_string())
            .join("")
            .trim()
            .is_empty()
    {
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
                /// So its danagerous to use with Aeron/AeronContext/AeronPublication/AeronSubscription
                /// More intended for AeronArchiveRecordingDescriptor
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

    let is_closed_method = wrapper.get_is_closed_method_quote();

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
       #common_code
    }
}
