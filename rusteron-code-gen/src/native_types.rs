//! Type facts used to decide whether generated value APIs need caller guarantees.

use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use syn::{Item, Type, UseTree};

/// Parsed aliases and value layouts shared by generated wrappers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NativeTypeMetadata {
    types: BTreeMap<String, Definition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Definition {
    Alias(String),
    Struct(Vec<String>),
    Enum { zero_valid: Option<bool> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TypeFacts {
    pub borrows_storage: bool,
    pub zero_valid: Option<bool>,
}

impl TypeFacts {
    const UNKNOWN: Self = Self {
        borrows_storage: true,
        zero_valid: None,
    };
    const SCALAR: Self = Self {
        borrows_storage: false,
        zero_valid: Some(true),
    };

    fn aggregate(items: impl Iterator<Item = Self>) -> Self {
        items.fold(Self::SCALAR, |a, b| Self {
            borrows_storage: a.borrows_storage || b.borrows_storage,
            zero_valid: match (a.zero_valid, b.zero_valid) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
        })
    }
}

impl NativeTypeMetadata {
    pub(crate) fn from_items(items: &[Item]) -> Self {
        let mut types = BTreeMap::new();
        for item in items {
            match item {
                Item::Type(item) => {
                    types.insert(
                        item.ident.to_string(),
                        Definition::Alias(item.ty.to_token_stream().to_string()),
                    );
                }
                Item::Struct(item) => {
                    types.insert(
                        item.ident.to_string(),
                        Definition::Struct(
                            item.fields
                                .iter()
                                .map(|field| field.ty.to_token_stream().to_string())
                                .collect(),
                        ),
                    );
                }
                Item::Enum(item) if item.variants.iter().all(|v| matches!(v.fields, syn::Fields::Unit)) => {
                    let mut next = Some(0i128);
                    let mut zero_valid = false;
                    let mut known = true;
                    for variant in &item.variants {
                        let value = match &variant.discriminant {
                            Some((_, expr)) => integer(expr),
                            None => next,
                        };
                        zero_valid |= value == Some(0);
                        known &= value.is_some();
                        next = value.and_then(|value| value.checked_add(1));
                    }
                    types.insert(
                        item.ident.to_string(),
                        Definition::Enum {
                            zero_valid: if zero_valid {
                                Some(true)
                            } else if known {
                                Some(false)
                            } else {
                                None
                            },
                        },
                    );
                }
                Item::Use(item) => collect_renames(&item.tree, &mut Vec::new(), &mut types),
                // Unions and unknown aggregates need a separate validity contract.
                _ => {}
            }
        }
        Self { types }
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.types.contains_key(name)
    }

    pub(crate) fn facts(&self, ty: &Type) -> TypeFacts {
        self.inspect(ty, &mut BTreeSet::new())
    }

    fn inspect(&self, ty: &Type, visiting: &mut BTreeSet<String>) -> TypeFacts {
        match ty {
            Type::Array(array) if integer(&array.len) == Some(0) => TypeFacts::SCALAR,
            Type::Array(array) => self.inspect(&array.elem, visiting),
            Type::Tuple(tuple) => TypeFacts::aggregate(tuple.elems.iter().map(|ty| self.inspect(ty, visiting))),
            Type::Paren(paren) => self.inspect(&paren.elem, visiting),
            Type::Group(group) => self.inspect(&group.elem, visiting),
            Type::Ptr(_) => TypeFacts {
                borrows_storage: true,
                zero_valid: Some(true),
            },
            Type::Path(path) if path.qself.is_none() => {
                let Some(segment) = path.path.segments.last() else {
                    return TypeFacts::UNKNOWN;
                };
                if !matches!(segment.arguments, syn::PathArguments::None) {
                    return TypeFacts::UNKNOWN;
                }
                let name = segment.ident.to_string();
                let local = path.path.segments.len() == 1
                    || path
                        .path
                        .segments
                        .first()
                        .is_some_and(|segment| segment.ident == "self");
                if let Some(definition) = local.then(|| self.types.get(&name)).flatten() {
                    if !visiting.insert(name.clone()) {
                        return TypeFacts::UNKNOWN;
                    }
                    let facts = match definition {
                        Definition::Alias(target) => self.inspect(&syn::parse_str::<Type>(target).unwrap(), visiting),
                        Definition::Struct(fields) => TypeFacts::aggregate(
                            fields
                                .iter()
                                .map(|field| self.inspect(&syn::parse_str::<Type>(field).unwrap(), visiting)),
                        ),
                        Definition::Enum { zero_valid } => TypeFacts {
                            borrows_storage: false,
                            zero_valid: *zero_valid,
                        },
                    };
                    visiting.remove(&name);
                    facts
                } else if is_scalar_path(&path.path) {
                    TypeFacts::SCALAR
                } else {
                    TypeFacts::UNKNOWN
                }
            }
            _ => TypeFacts::UNKNOWN,
        }
    }
}

fn is_scalar_path(path: &syn::Path) -> bool {
    let names: Vec<_> = path.segments.iter().map(|segment| segment.ident.to_string()).collect();
    let Some((name, namespace)) = names.split_last() else {
        return false;
    };
    let namespace: Vec<_> = namespace.iter().map(String::as_str).collect();
    matches!(
        namespace.as_slice(),
        [] | ["std", "os", "raw"] | ["std" | "core", "ffi" | "primitive"]
    ) && matches!(
        name.as_str(),
        "u8" | "i8"
            | "u16"
            | "i16"
            | "u32"
            | "i32"
            | "u64"
            | "i64"
            | "u128"
            | "i128"
            | "usize"
            | "isize"
            | "f32"
            | "f64"
            | "bool"
            | "c_char"
            | "c_schar"
            | "c_uchar"
            | "c_short"
            | "c_ushort"
            | "c_int"
            | "c_uint"
            | "c_long"
            | "c_ulong"
            | "c_longlong"
            | "c_ulonglong"
            | "c_float"
            | "c_double"
    )
}

fn integer(expr: &syn::Expr) -> Option<i128> {
    match expr {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(value),
            ..
        }) => value.base10_parse().ok(),
        syn::Expr::Unary(unary) if matches!(unary.op, syn::UnOp::Neg(_)) => integer(&unary.expr)?.checked_neg(),
        syn::Expr::Paren(paren) => integer(&paren.expr),
        _ => None,
    }
}

fn collect_renames(tree: &UseTree, prefix: &mut Vec<String>, types: &mut BTreeMap<String, Definition>) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_renames(&path.tree, prefix, types);
            prefix.pop();
        }
        UseTree::Rename(rename) => {
            let mut target = prefix.clone();
            target.push(rename.ident.to_string());
            types.insert(rename.rename.to_string(), Definition::Alias(target.join("::")));
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_renames(item, prefix, types);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(source: &str) -> NativeTypeMetadata {
        NativeTypeMetadata::from_items(&syn::parse_file(source).unwrap().items)
    }

    #[test]
    fn aliases_arrays_and_nested_structs_preserve_type_facts() {
        let types =
            metadata("type A = B; type B = u32; struct Inner { x: [A; 2] } type C = Inner; struct Outer { x: C }");
        for name in ["A", "B", "C", "Inner", "Outer"] {
            assert_eq!(types.facts(&syn::parse_str(name).unwrap()), TypeFacts::SCALAR, "{name}");
        }
    }

    #[test]
    fn pointer_aliases_cycles_callbacks_and_unions_stay_conservative() {
        let types = metadata(
            "type Ptr = *mut u8; type P = Ptr; type A = B; type B = A; union U { n: u64, p: Ptr } type Callback = Option<unsafe extern \"C\" fn()>; struct Nested { p: [P; 2] }",
        );
        for name in [
            "Ptr",
            "P",
            "A",
            "B",
            "U",
            "Callback",
            "Nested",
            "Future",
            "other::c_int",
            "other::Nested",
        ] {
            assert!(types.facts(&syn::parse_str(name).unwrap()).borrows_storage, "{name}");
        }
    }

    #[test]
    fn enum_zero_validity_is_separate_from_pointer_ownership() {
        let types = metadata(
            "enum Zero { A = -1, B, C } enum Nonzero { A = 1, B } enum Implicit { A, B } enum Unknown { A = VALUE } pub use self::Zero as Alias; type Indirect = Alias; struct Good { x: [Indirect; 2] } struct Bad { x: Nonzero }",
        );
        for name in ["Zero", "Implicit", "Alias", "Indirect", "Good"] {
            assert_eq!(types.facts(&syn::parse_str(name).unwrap()), TypeFacts::SCALAR, "{name}");
        }
        assert_eq!(types.facts(&syn::parse_str("Unknown").unwrap()).zero_valid, None);
        for name in ["Nonzero", "Bad"] {
            assert_eq!(
                types.facts(&syn::parse_str(name).unwrap()),
                TypeFacts {
                    borrows_storage: false,
                    zero_valid: Some(false)
                },
                "{name}"
            );
        }
    }
}
