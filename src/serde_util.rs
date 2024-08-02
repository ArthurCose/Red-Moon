macro_rules! impl_serde_serialize_stub_fn {
    ($name:ident, $type:ty) => {
        fn $name<S>(_: &$type, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Serialize::serialize(&(), serializer)
        }
    };
}

macro_rules! impl_serde_deserialize_stub_fn {
    ($name:ident, $type:ty, $default_value:expr) => {
        fn $name<'de, D>(deserializer: D) -> Result<$type, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let _: () = serde::Deserialize::deserialize(deserializer)?;
            Ok($default_value)
        }
    };
}

macro_rules! impl_serde_rc {
    ($module_name:ident, $type:ty, $borrowed:ty) => {
        mod $module_name {
            #[allow(unused_imports)]
            use super::*;
            use std::rc::Rc;

            pub(super) fn serialize<S>(s: &Rc<$type>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let s: &$type = &**s;
                serde::Serialize::serialize(s, serializer)
            }

            pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Rc<$type>, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserialize::deserialize(deserializer).map(|s: $borrowed| s.into())
            }
        }
    };
}

use serde::de::Error;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

macro_rules! impl_serde_deduplicating_rc {
    ($module_name:ident, $boxed:ty, $id_callback:expr, $se_conversion:expr, $de_conversion:expr) => {
        pub(crate) mod $module_name {
            use super::*;

            thread_local! {
                static DEDUPLICATING: Cell<bool> = Default::default();
                static MAP: RefCell<HashMap<usize, $boxed>> = Default::default();
                static SET: RefCell<HashSet<usize>> = Default::default();
            }

            pub(crate) fn serialize<S>(rc: &$boxed, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let conversion = $se_conversion;

                if DEDUPLICATING.get() {
                    let id: usize = ($id_callback)(rc);
                    let data = if SET.with_borrow_mut(|set| set.insert(id)) {
                        let s = conversion(&**rc);
                        Some(s)
                    } else {
                        None
                    };

                    serde::Serialize::serialize(&(id, data), serializer)
                } else {
                    let s = conversion(&**rc);
                    serde::Serialize::serialize(&s, serializer)
                }
            }

            pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<$boxed, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let conversion = $de_conversion;

                if DEDUPLICATING.get() {
                    let (id, data) = serde::Deserialize::deserialize(deserializer)?;

                    match data {
                        Some(data) => {
                            let rc: $boxed = conversion(data);
                            MAP.with_borrow_mut(|map| {
                                map.insert(id, rc.clone());
                            });
                            Ok(rc)
                        }
                        None => MAP.with_borrow(|map| match map.get(&id) {
                            Some(rc) => Ok(rc.clone()),
                            None => Err(D::Error::invalid_value(
                                serde::de::Unexpected::Option,
                                &stringify!($boxed),
                            )),
                        }),
                    }
                } else {
                    let data = serde::Deserialize::deserialize(deserializer)?;
                    Ok(conversion(data))
                }
            }

            pub(crate) fn begin_dedup() {
                DEDUPLICATING.set(true);
            }

            pub(crate) fn end_dedup() {
                MAP.with_borrow_mut(|map| map.clear());
                SET.with_borrow_mut(|set| set.clear());
                DEDUPLICATING.set(false);
            }
        }
    };
}

use crate::interpreter::{FunctionDefinition, StackObjectKey};
use erasable::Thin;
use slice_dst::SliceWithHeader;

fn rc_id<T: ?Sized>(rc: &Rc<T>) -> usize {
    Rc::as_ptr(rc) as *const () as usize
}

// self.keys.slice.as_ptr()

impl_serde_deduplicating_rc!(
    serde_str_rc,
    Rc<str>,
    rc_id,
    |data| { data },
    |data: &str| { data.into() }
);
// todo: should we use &[u8] instead of Box<[u8]>? can we use some cow type?
impl_serde_deduplicating_rc!(
    serde_u8_thin_slice_rc,
    Thin<Rc<SliceWithHeader<(), u8>>>,
    |x: &Thin<Rc<slice_dst::SliceWithHeader<(), u8>>>| { x.slice.as_ptr() as *const () as usize },
    |data| {
        let data: &slice_dst::SliceWithHeader<(), u8> = data;
        &data.slice
    },
    |data: Vec<u8>| slice_dst::SliceWithHeader::new::<Rc<_>, _>((), data).into()
);
impl_serde_deduplicating_rc!(
    serde_stack_object_key_slice_rc,
    Rc<[StackObjectKey]>,
    rc_id,
    |data| { data },
    |data: Box<[StackObjectKey]>| { data.into() }
);
impl_serde_deduplicating_rc!(
    serde_function_definition_rc,
    Rc<FunctionDefinition>,
    rc_id,
    |data| { data },
    |data: FunctionDefinition| { data.into() }
);

pub(crate) fn begin_dedup() {
    serde_str_rc::begin_dedup();
    serde_u8_thin_slice_rc::begin_dedup();
    serde_stack_object_key_slice_rc::begin_dedup();
    serde_function_definition_rc::begin_dedup();
}
pub(crate) fn end_dedup() {
    serde_str_rc::end_dedup();
    serde_u8_thin_slice_rc::end_dedup();
    serde_stack_object_key_slice_rc::end_dedup();
    serde_function_definition_rc::end_dedup();
}

pub(crate) use impl_serde_deduplicating_rc;
pub(crate) use impl_serde_deserialize_stub_fn;
pub(crate) use impl_serde_rc;
pub(crate) use impl_serde_serialize_stub_fn;
