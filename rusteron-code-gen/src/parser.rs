use crate::generator::{CBinding, CWrapper, Method};
use crate::{Arg, ArgProcessing, CHandler};
use itertools::Itertools;
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use syn::{Attribute, Item, ItemForeignMod, ItemStruct, ItemType, Lit, Meta, MetaNameValue};

pub fn parse_bindings(out: &PathBuf) -> CBinding {
    let file_content = fs::read_to_string(out.clone()).expect("Unable to read file");
    let syntax_tree = syn::parse_file(&file_content).expect("Unable to parse file");
    let mut wrappers = BTreeMap::new();
    let mut methods = Vec::new();
    let mut handlers = Vec::new();

    // Iterate through the items in the file
    for item in syntax_tree.items {
        match item {
            Item::Struct(s) => {
                process_struct(&mut wrappers, &s);
            }
            Item::Type(ty) => {
                process_type(&mut wrappers, &mut handlers, &ty);
            }
            Item::ForeignMod(fm) => {
                process_c_method(&mut wrappers, &mut methods, fm);
            }
            _ => {}
        }
    }

    /*    // need to filter out args which don't match
        for wrapper in wrappers.values_mut() {
          for method in wrapper.methods.iter_mut() {
              let method_debug = format!("{:?}", method);
              for arg in method.arguments.iter_mut() {
                if let ArgProcessing::Handler(args) = &arg.processing {
                    let handler = args.get(0).unwrap();
                    if !handlers.iter().any(|h| h.type_name == handler.c_type) {
                      log::info!("replacing {} back to default", method_debug);
                      // arg.processing = ArgProcessing::Default;
                    }
                }
              }
          }
        }
    */
    let bindings = CBinding {
        wrappers: wrappers
            .into_iter()
            .filter(|(_, wrapper)| {
                // these are from media driver and do not follow convention
                ![
                    "aeron_thread",
                    "aeron_command",
                    "aeron_executor",
                    "aeron_name_resolver",
                    "aeron_udp_channel_transport", // this one I have issues with handlers
                    "aeron_udp_transport",         // this one I have issues with handlers
                ]
                .iter()
                .any(|&filter| wrapper.type_name.starts_with(filter))
            })
            .collect(),
        methods,
        handlers: handlers
            .into_iter()
            .filter(|h| {
                !["aeron_udp_channel", "aeron_udp_transport"]
                    .iter()
                    .any(|&filter| h.type_name.starts_with(filter))
            })
            .collect(),
    };

    let mismatched_types = bindings
        .wrappers
        .iter()
        .filter(|(key, w)| key.as_str() != w.type_name)
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect_vec();
    assert_eq!(Vec::<(String, CWrapper)>::new(), mismatched_types);
    bindings
}

fn process_c_method(
    wrappers: &mut BTreeMap<String, CWrapper>,
    methods: &mut Vec<Method>,
    fm: ItemForeignMod,
) {
    // Extract functions inside extern "C" blocks
    if fm.abi.name.is_some() && fm.abi.name.as_ref().unwrap().value() == "C" {
        for foreign_item in fm.items {
            if let syn::ForeignItem::Fn(f) = foreign_item {
                let docs = get_doc_comments(&f.attrs);
                let fn_name = f.sig.ident.to_string();

                // Get function arguments and return type as Rust code
                let args = extract_function_arguments(&f.sig.inputs);
                let ret = extract_return_type(&f.sig.output);

                let option = if let Some(arg) = args
                    .iter()
                    .skip_while(|a| a.is_mut_pointer() && a.is_primitive())
                    .next()
                {
                    let ty = &arg.c_type;
                    let ty = ty.split(' ').last().map(|t| t.to_string()).unwrap();
                    if wrappers.contains_key(&ty) {
                        Some(ty)
                    } else {
                        find_closest_wrapper_from_method_name(wrappers, &fn_name)
                    }
                } else {
                    find_closest_wrapper_from_method_name(wrappers, &fn_name)
                };

                match option {
                    Some(key) => {
                        let wrapper = wrappers.get_mut(&key).unwrap();
                        wrapper.methods.push(Method {
                            fn_name: fn_name.clone(),
                            struct_method_name: fn_name
                                .replace(&wrapper.type_name[..wrapper.type_name.len() - 1], "")
                                .to_string(),
                            return_type: Arg {
                                name: "".to_string(),
                                c_type: ret.clone(),
                                processing: ArgProcessing::Default,
                            },
                            arguments: process_types(args.clone()),
                            docs: docs.clone(),
                        });
                    }
                    None => methods.push(Method {
                        fn_name: fn_name.clone(),
                        struct_method_name: "".to_string(),
                        return_type: Arg {
                            name: "".to_string(),
                            c_type: ret.clone(),
                            processing: ArgProcessing::Default,
                        },
                        arguments: process_types(args.clone()),
                        docs: docs.clone(),
                    }),
                }
            }
        }
    }
}

fn find_closest_wrapper_from_method_name(
    wrappers: &mut BTreeMap<String, CWrapper>,
    fn_name: &String,
) -> Option<String> {
    let type_names = get_possible_wrappers(&fn_name);

    let mut value = None;
    for ty in type_names {
        if wrappers.contains_key(&ty) {
            value = Some(ty);
            break;
        }
    }

    value
}

pub fn get_possible_wrappers(fn_name: &str) -> Vec<String> {
    fn_name
        .char_indices()
        .filter(|(_, c)| *c == '_')
        .map(|(i, _)| format!("{}_t", &fn_name[..i]))
        .rev()
        .collect_vec()
}

fn process_type(
    wrappers: &mut BTreeMap<String, CWrapper>,
    handlers: &mut Vec<CHandler>,
    ty: &ItemType,
) {
    // Handle type definitions and get docs
    let docs = get_doc_comments(&ty.attrs);

    let type_name = ty.ident.to_string();
    let class_name = snake_to_pascal_case(&type_name);

    if ty.to_token_stream().to_string().contains("_stct") {
        wrappers
            .entry(type_name.clone())
            .or_insert(CWrapper {
                class_name,
                without_name: type_name[..type_name.len() - 2].to_string(),
                type_name,
                ..Default::default()
            })
            .docs
            .extend(docs);
    } else {
        // Parse the function pointer type -> it is typically used for handlers/callbacks
        if let syn::Type::Path(type_path) = &*ty.ty {
            if let Some(segment) = type_path.path.segments.last() {
                if segment.ident.to_string() == "Option" {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(syn::GenericArgument::Type(syn::Type::BareFn(bare_fn))) =
                            args.args.first()
                        {
                            let args: Vec<Arg> = bare_fn
                                .inputs
                                .iter()
                                .map(|arg| {
                                    let arg_name = match &arg.name {
                                        Some((ident, _)) => ident.to_string(),
                                        None => "".to_string(),
                                    };
                                    let arg_type = arg.ty.to_token_stream().to_string();
                                    (arg_name, arg_type)
                                })
                                .map(|(field_name, field_type)| Arg {
                                    name: field_name,
                                    c_type: field_type,
                                    processing: ArgProcessing::Default,
                                })
                                .collect();
                            let string = bare_fn.output.to_token_stream().to_string();
                            let mut return_type = string.trim();

                            if return_type.starts_with("-> ") {
                                return_type = &return_type[3..];
                            }

                            if return_type.is_empty() {
                                return_type = "()";
                            }

                            if args.iter().filter(|a| a.is_c_void()).count() == 1 {
                                let value = CHandler {
                                    type_name: ty.ident.to_string(),
                                    args: process_types(args),
                                    return_type: Arg {
                                        name: "".to_string(),
                                        c_type: return_type.to_string(),
                                        processing: ArgProcessing::Default,
                                    },
                                    docs: docs.clone(),
                                    fn_mut_signature: Default::default(),
                                    closure_type_name: Default::default(),
                                };
                                handlers.push(value);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn process_struct(wrappers: &mut BTreeMap<String, CWrapper>, s: &ItemStruct) {
    // Print the struct name and its doc comments
    let docs = get_doc_comments(&s.attrs);
    let type_name = s.ident.to_string().replace("_stct", "_t");
    let class_name = snake_to_pascal_case(&type_name);

    let fields: Vec<Arg> = s
        .fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap().to_string();
            let field_type = f.ty.to_token_stream().to_string();
            (field_name, field_type)
        })
        .map(|(field_name, field_type)| Arg {
            name: field_name,
            c_type: field_type,
            processing: ArgProcessing::Default,
        })
        .collect();

    let w = wrappers.entry(type_name.to_string()).or_insert(CWrapper {
        class_name,
        without_name: type_name[..type_name.len() - 2].to_string(),
        type_name,
        ..Default::default()
    });
    w.docs.extend(docs);
    w.fields = process_types(fields);
}

fn process_types(mut name_and_type: Vec<Arg>) -> Vec<Arg> {
    // now mark arguments which can be reduced
    for i in 1..name_and_type.len() {
        let param1 = &name_and_type[i - 1];
        let param2 = &name_and_type[i];

        let is_int = param2.c_type == "usize" || param2.c_type == "i32";
        let length_field = param2.name == "length"
            || param2.name == "len"
            || (param2.name.ends_with("_length") && param2.name.starts_with(&param1.name));
        if param2.is_c_void() && !param1.is_mut_pointer() && param1.c_type.ends_with("_t") {
            // closures
            //         handler: aeron_on_available_counter_t,
            //         clientd: *mut ::std::os::raw::c_void,
            let processing = ArgProcessing::Handler(vec![param1.clone(), param2.clone()]);
            name_and_type[i - 1].processing = processing.clone();
            name_and_type[i].processing = processing.clone();
        } else if param1.is_c_string_any() && !param1.is_mut_pointer() && is_int && length_field {
            //     pub stripped_channel: *mut ::std::os::raw::c_char,
            //     pub stripped_channel_length: usize,
            let processing = ArgProcessing::StringWithLength(vec![param1.clone(), param2.clone()]);
            name_and_type[i - 1].processing = processing.clone();
            name_and_type[i].processing = processing.clone();
        } else if param1.is_byte_array()
            // && !param1.is_mut_pointer()
            && is_int
            && length_field
        {
            //         key_buffer: *const u8,
            //         key_buffer_length: usize,
            let processing =
                ArgProcessing::ByteArrayWithLength(vec![param1.clone(), param2.clone()]);
            name_and_type[i - 1].processing = processing.clone();
            name_and_type[i].processing = processing.clone();
        }

        //
    }

    name_and_type
}

// Helper function to extract doc comments
fn get_doc_comments(attrs: &[Attribute]) -> BTreeSet<String> {
    attrs
        .iter()
        .filter_map(|attr| {
            // Parse the attribute meta to check if it is a `Meta::NameValue`
            if let Meta::NameValue(MetaNameValue {
                path,
                value: syn::Expr::Lit(expr_lit),
                ..
            }) = &attr.meta
            {
                // Check if the path is "doc"
                if path.is_ident("doc") {
                    // Check if the literal is a string and return its value
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        return Some(lit_str.value().trim().to_string());
                    }
                }
            }
            None
        })
        .collect()
}

pub fn snake_to_pascal_case(mut snake: &str) -> String {
    if snake.ends_with("_t") {
        snake = &snake[..snake.len() - 2];
    }
    snake
        .split('_')
        .filter(|x| *x != "on") // Split the string by underscores
        .map(|word| {
            let mut chars = word.chars();
            // Capitalize the first letter and collect the rest of the letters
            match chars.next() {
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

// Helper function to extract function arguments as Rust code
fn extract_function_arguments(
    inputs: &syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
) -> Vec<Arg> {
    inputs
        .iter()
        .map(|arg| match arg {
            syn::FnArg::Receiver(_) => "self".to_string(), // Handle self receiver
            syn::FnArg::Typed(pat_type) => pat_type.to_token_stream().to_string(), // Convert the pattern and type to Rust code
        })
        .map(|arg| {
            arg.splitn(2, ':')
                .map(|s| s.trim().to_string())
                .collect_tuple()
                .unwrap()
        })
        .map(|(name, ty)| Arg {
            name,
            c_type: ty,
            processing: ArgProcessing::Default,
        })
        .collect_vec()
}

// Helper function to extract return type as Rust code
fn extract_return_type(output: &syn::ReturnType) -> String {
    match output {
        syn::ReturnType::Default => "()".to_string(), // No return type, equivalent to ()
        syn::ReturnType::Type(_, ty) => ty.to_token_stream().to_string(), // Convert the type to Rust code
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::parse_bindings;

    #[test]
    fn media_driver() {
        let bindings = parse_bindings(&"../rusteron-code-gen/bindings/media-driver.rs".into());
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );
    }
    #[test]
    fn client() {
        let bindings = parse_bindings(&"../rusteron-code-gen/bindings/client.rs".into());
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );
        assert!(bindings.handlers.len() > 1);
    }
}
