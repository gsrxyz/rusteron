#![allow(improper_ctypes_definitions)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]

mod common;
mod generator;
mod parser;

pub use common::*;
pub use generator::*;
pub use parser::*;

use proc_macro2::TokenStream;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::{Command, Stdio};

pub const CUSTOM_AERON_CODE: &str = include_str!("./aeron_custom.rs");
pub const COMMON_CODE: &str = include_str!("./common.rs");

pub fn append_to_file(file_path: &str, code: &str) -> std::io::Result<()> {
    // Open the file in append mode
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_path)?;

    // Write the generated code to the file
    writeln!(file, "\n{}", code)?;

    Ok(())
}

#[allow(dead_code)]
pub fn format_with_rustfmt(code: &str) -> Result<String, std::io::Error> {
    let mut rustfmt = Command::new("rustfmt")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = rustfmt.stdin.take() {
        stdin.write_all(code.as_bytes())?;
    }

    let output = rustfmt.wait_with_output()?;
    let formatted_code = String::from_utf8_lossy(&output.stdout).to_string();

    Ok(formatted_code)
}

#[allow(dead_code)]
pub fn format_token_stream(tokens: TokenStream) -> String {
    let code = tokens.to_string();

    match format_with_rustfmt(&code) {
        Ok(formatted_code) if !formatted_code.trim().is_empty() => formatted_code,
        _ => code.replace("{", "{\n"), // Fallback to unformatted code in case of error
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::MEDIA_DRIVER_BINDINGS;
    use crate::parser::parse_bindings;
    use crate::{
        append_to_file, format_token_stream, format_with_rustfmt, ARCHIVE_BINDINGS,
        CLIENT_BINDINGS, CUSTOM_AERON_CODE,
    };
    use proc_macro2::TokenStream;
    use std::fs;

    #[test]
    #[cfg(not(target_os = "windows"))] // the generated bindings have different sizes
    fn client() {
        let mut bindings = parse_bindings(&"../rusteron-code-gen/bindings/client.rs".into());
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );
        assert_eq!(
            0,
            bindings.methods.len(),
            "expected all methods to have been matched {:#?}",
            bindings.methods
        );

        let file = write_to_file(TokenStream::new(), true, "client.rs");
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        for (p, w) in bindings.wrappers.values().enumerate() {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                true,
                true,
                &bindings.handlers,
            );
            write_to_file(code, false, "client.rs");
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            append_to_file(&file, &format_with_rustfmt(&code.to_string()).unwrap()).unwrap();
        }

        let t = trybuild::TestCases::new();
        append_to_file(&file, "use bindings::*; mod bindings { ").unwrap();
        append_to_file(&file, CLIENT_BINDINGS).unwrap();
        append_to_file(&file, "}").unwrap();
        append_to_file(&file, CUSTOM_AERON_CODE).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(file)
    }

    #[test]
    #[cfg(not(target_os = "windows"))] // the generated bindings have different sizes
    fn media_driver() {
        let mut bindings = parse_bindings(&"../rusteron-code-gen/bindings/media-driver.rs".into());
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );

        let file = write_to_file(TokenStream::new(), true, "md.rs");

        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        for (p, w) in bindings
            .wrappers
            .values()
            .filter(|w| !w.type_name.contains("_t_") && w.type_name != "in_addr")
            .enumerate()
        {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                true,
                true,
                &bindings.handlers,
            );
            write_to_file(code, false, "md.rs");
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            append_to_file(&file, &format_with_rustfmt(&code.to_string()).unwrap()).unwrap();
        }
        let t = trybuild::TestCases::new();
        append_to_file(&file, "use bindings::*; mod bindings { ").unwrap();
        append_to_file(&file, MEDIA_DRIVER_BINDINGS).unwrap();
        append_to_file(&file, "}").unwrap();
        append_to_file(&file, CUSTOM_AERON_CODE).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(&file)
    }

    #[test]
    #[cfg(not(target_os = "windows"))] // the generated bindings have different sizes
    fn archive() {
        let mut bindings = parse_bindings(&"../rusteron-code-gen/bindings/archive.rs".into());
        assert_eq!(
            "AeronImageFragmentAssembler",
            bindings
                .wrappers
                .get("aeron_image_fragment_assembler_t")
                .unwrap()
                .class_name
        );

        let file = write_to_file(TokenStream::new(), true, "archive.rs");
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            // need to run this first so I know the FnMut(xxxx) which is required in generate_rust_code
            let _ = crate::generate_handlers(handler, &bindings_copy);
        }
        for (p, w) in bindings.wrappers.values().enumerate() {
            let code = crate::generate_rust_code(
                w,
                &bindings.wrappers,
                p == 0,
                true,
                true,
                &bindings.handlers,
            );
            write_to_file(code, false, "archive.rs");
        }
        let bindings_copy = bindings.clone();
        for handler in bindings.handlers.iter_mut() {
            let code = crate::generate_handlers(handler, &bindings_copy);
            append_to_file(&file, &format_with_rustfmt(&code.to_string()).unwrap()).unwrap();
        }
        let t = trybuild::TestCases::new();
        append_to_file(&file, "use bindings::*; mod bindings { ").unwrap();
        append_to_file(&file, ARCHIVE_BINDINGS).unwrap();
        append_to_file(&file, "}").unwrap();
        append_to_file(&file, CUSTOM_AERON_CODE).unwrap();
        append_to_file(&file, "\npub fn main() {}\n").unwrap();
        t.pass(file)
    }

    fn write_to_file(rust_code: TokenStream, delete: bool, name: &str) -> String {
        let src = format_token_stream(rust_code);
        let path = format!("../target/{name}");
        let path = &path;
        if delete {
            let _ = fs::remove_file(path);
        }
        append_to_file(path, &src).unwrap();
        path.to_string()
    }
}

#[cfg(test)]
mod test {
    use crate::ManagedCResource;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn make_resource(val: i32) -> *mut i32 {
        Box::into_raw(Box::new(val))
    }

    #[test]
    fn test_drop_calls_cleanup_non_borrowed_no_cleanup_struct() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(10);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            // Set the resource to null to simulate cleanup.
            unsafe {
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                false,
                None,
            );
            assert!(_resource.is_ok())
        }
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_calls_cleanup_non_borrowed_with_cleanup_struct() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(20);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                true,
                None,
            );
            assert!(_resource.is_ok())
        }
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_does_not_call_cleanup_if_already_closed() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(30);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let mut resource = ManagedCResource::new(
            |res: *mut *mut i32| {
                unsafe {
                    *res = resource_ptr;
                }
                0
            },
            cleanup,
            false,
            None,
        );
        assert!(resource.is_ok());

        if let Ok(ref mut resource) = &mut resource {
            assert!(resource.close().is_ok())
        }

        // Reset the flag to ensure drop does not call cleanup a second time.
        flag.store(false, Ordering::SeqCst);
        drop(resource);
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_does_not_call_cleanup_if_check_for_is_closed_returns_true() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(60);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let check_fn = Some(|_res: *mut i32| -> bool { true } as fn(_) -> bool);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                false,
                check_fn,
            );
            assert!(_resource.is_ok());
        }
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_drop_does_call_cleanup_if_check_for_is_closed_returns_false() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        let resource_ptr = make_resource(60);

        let cleanup = Some(Box::new(move |res: *mut *mut i32| -> i32 {
            flag_clone.store(true, Ordering::SeqCst);
            unsafe {
                *res = std::ptr::null_mut();
            }
            0
        }) as Box<dyn FnMut(*mut *mut i32) -> i32>);

        let check_fn = Some(|_res: *mut i32| -> bool { false } as fn(*mut i32) -> bool);

        {
            let _resource = ManagedCResource::new(
                |res: *mut *mut i32| {
                    unsafe {
                        *res = resource_ptr;
                    }
                    0
                },
                cleanup,
                false,
                check_fn,
            );
            assert!(_resource.is_ok())
        }
        assert!(flag.load(Ordering::SeqCst));
    }
}
