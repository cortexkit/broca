//! Declarative macros that define a type and its encoding in one place, so a field can't be
//! added to a struct without being encoded, or encoded under a number nobody declared.
//!
//! `message!` declares a struct. Each field is `<number> <cardinality> [must]? <name>: <type>`:
//! - `req`: the field has type `T`; omitted when it holds `T`'s default, refused when absent and
//!   `T` has no default.
//! - `opt`: the field has type `Option<T>`; written whenever `Some`.
//! - `rep`: the field has type `Vec<T>`; one occurrence per element.
//! - `map`: the field's type is a `BTreeMap<K, V>`, written in full; one entry per pair.
//!
//! `[must]` sets the must-understand bit (see the `codec` module). Numbers must ascend, which a
//! compile-time assertion checks.
//!
//! `oneof!` declares an enum where every variant carries one payload (use `()` for none). It
//! encodes as a message holding exactly one field, numbered by the variant.
//!
//! `values!` declares a field-less enum encoded as a varint.

#[doc(hidden)]
#[macro_export]
macro_rules! __codec_must {
    () => {
        false
    };
    (must) => {
        true
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __codec_field_ty {
    (req $ty:ty) => { $ty };
    (opt $ty:ty) => { ::std::option::Option<$ty> };
    (rep $ty:ty) => { ::std::vec::Vec<$ty> };
    (map $ty:ty) => { $ty };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __codec_slot_ty {
    (req $ty:ty) => { ::std::option::Option<$ty> };
    (opt $ty:ty) => { ::std::option::Option<$ty> };
    (rep $ty:ty) => { ::std::vec::Vec<$ty> };
    (map $ty:ty) => { $ty };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __codec_put {
    (req, $out:expr, $num:expr, $must:expr, $value:expr) => {
        $crate::codec::put_req($out, $num, $must, $value)
    };
    (opt, $out:expr, $num:expr, $must:expr, $value:expr) => {
        $crate::codec::put_opt($out, $num, $must, $value)
    };
    (rep, $out:expr, $num:expr, $must:expr, $value:expr) => {
        $crate::codec::put_rep($out, $num, $must, $value)
    };
    (map, $out:expr, $num:expr, $must:expr, $value:expr) => {
        $crate::codec::put_map($out, $num, $must, $value)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __codec_take {
    (req, $slot:expr, $field:expr, $at:expr) => {
        $crate::codec::take_one($slot, $field, $at)
    };
    (opt, $slot:expr, $field:expr, $at:expr) => {
        $crate::codec::take_one($slot, $field, $at)
    };
    (rep, $slot:expr, $field:expr, $at:expr) => {
        $crate::codec::take_rep($slot, $field, $at)
    };
    (map, $slot:expr, $field:expr, $at:expr) => {
        $crate::codec::take_map($slot, $field, $at)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __codec_finish {
    (req, $slot:expr, $at:expr) => {
        $crate::codec::finish_req($slot, $at)?
    };
    (opt, $slot:expr, $at:expr) => {
        $slot
    };
    (rep, $slot:expr, $at:expr) => {
        $slot
    };
    (map, $slot:expr, $at:expr) => {
        $slot
    };
}

#[macro_export]
macro_rules! message {
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$fattr:meta])*
                $num:literal $card:ident $([$must:ident])? $field:ident : $ty:ty
            ),* $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis struct $name {
            $(
                $(#[$fattr])*
                pub $field: $crate::__codec_field_ty!($card $ty),
            )*
        }

        const _: () = assert!(
            $crate::codec::ascending(&[$($num),*]),
            concat!("field numbers of ", stringify!($name), " must ascend from 1")
        );

        impl $crate::codec::Value for $name {
            const WIRE: $crate::codec::Wire = $crate::codec::Wire::Len;
            const NAME: &'static str = stringify!($name);

            #[allow(unused_variables)]
            fn write(&self, out: &mut ::std::vec::Vec<u8>) {
                $(
                    $crate::__codec_put!(
                        $card, out, $num, $crate::__codec_must!($($must)?), &self.$field
                    );
                )*
            }

            #[allow(unused_mut)]
            fn read(
                raw: $crate::codec::Raw<'_>,
            ) -> ::std::result::Result<Self, $crate::codec::DecodeError> {
                let at = stringify!($name);
                let bytes = $crate::codec::len_payload(raw, at)?;
                $(
                    let mut $field: $crate::__codec_slot_ty!($card $ty) =
                        ::std::default::Default::default();
                )*
                for field in $crate::codec::Fields::new(bytes, at) {
                    let field = field?;
                    match field.number {
                        $(
                            $num => $crate::__codec_take!(
                                $card,
                                &mut $field,
                                field,
                                concat!(stringify!($name), ".", stringify!($field))
                            )?,
                        )*
                        _ => $crate::codec::skip_unknown(field, at)?,
                    }
                }
                Ok(Self {
                    $(
                        $field: $crate::__codec_finish!(
                            $card,
                            $field,
                            concat!(stringify!($name), ".", stringify!($field))
                        ),
                    )*
                })
            }
        }

        impl $crate::codec::Message for $name {}
    };
}

#[macro_export]
macro_rules! oneof {
    (
        $(#[$attr:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$vattr:meta])*
                $num:literal $var:ident ( $ty:ty )
            ),* $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis enum $name {
            $(
                $(#[$vattr])*
                $var($ty),
            )*
        }

        const _: () = assert!(
            $crate::codec::ascending(&[$($num),*]),
            concat!("variant numbers of ", stringify!($name), " must ascend from 1")
        );

        impl $crate::codec::Value for $name {
            const WIRE: $crate::codec::Wire = $crate::codec::Wire::Len;
            const NAME: &'static str = stringify!($name);

            fn write(&self, out: &mut ::std::vec::Vec<u8>) {
                match self {
                    $(
                        Self::$var(value) => $crate::codec::put_one(out, $num, false, value),
                    )*
                }
            }

            fn read(
                raw: $crate::codec::Raw<'_>,
            ) -> ::std::result::Result<Self, $crate::codec::DecodeError> {
                let at = stringify!($name);
                let bytes = $crate::codec::len_payload(raw, at)?;
                let mut found: ::std::option::Option<Self> = None;
                for field in $crate::codec::Fields::new(bytes, at) {
                    let field = field?;
                    if found.is_some() {
                        return Err($crate::codec::DecodeError::new(
                            at,
                            $crate::codec::DecodeErrorKind::OneofMultiple,
                        ));
                    }
                    found = Some(match field.number {
                        $(
                            $num => {
                                let mut slot: ::std::option::Option<$ty> = None;
                                $crate::codec::take_one(
                                    &mut slot,
                                    field,
                                    concat!(stringify!($name), "::", stringify!($var)),
                                )?;
                                Self::$var(slot.expect("take_one fills the slot"))
                            }
                        )*
                        other => {
                            return Err($crate::codec::DecodeError::new(
                                at,
                                $crate::codec::DecodeErrorKind::UnknownVariant(u64::from(other)),
                            ))
                        }
                    });
                }
                found.ok_or_else(|| {
                    $crate::codec::DecodeError::new(at, $crate::codec::DecodeErrorKind::OneofEmpty)
                })
            }
        }

        impl $crate::codec::Message for $name {}
    };
}

#[macro_export]
macro_rules! values {
    (
        $(#[$attr:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$vattr:meta])*
                $num:literal $var:ident
            ),* $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis enum $name {
            $(
                $(#[$vattr])*
                $var,
            )*
        }

        const _: () = assert!(
            $crate::codec::ascending(&[$($num),*]),
            concat!("values of ", stringify!($name), " must ascend from 1")
        );

        impl $crate::codec::Value for $name {
            const WIRE: $crate::codec::Wire = $crate::codec::Wire::Varint;
            const NAME: &'static str = stringify!($name);

            fn write(&self, out: &mut ::std::vec::Vec<u8>) {
                let n: u64 = match self {
                    $( Self::$var => $num, )*
                };
                $crate::codec::put_varint(out, n);
            }

            fn read(
                raw: $crate::codec::Raw<'_>,
            ) -> ::std::result::Result<Self, $crate::codec::DecodeError> {
                let n = <u64 as $crate::codec::Value>::read(raw).map_err(|e| {
                    $crate::codec::DecodeError::new(stringify!($name), e.kind)
                })?;
                match n {
                    $( $num => Ok(Self::$var), )*
                    other => Err($crate::codec::DecodeError::new(
                        stringify!($name),
                        $crate::codec::DecodeErrorKind::UnknownVariant(other),
                    )),
                }
            }
        }
    };
}
