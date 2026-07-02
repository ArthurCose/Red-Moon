#![cfg_attr(any(not(doctest), feature = "serde"), doc = include_str!("../README.md"))]

mod utf8;
mod vec_cell;

#[cfg(feature = "serde")]
mod serde_util;

#[cfg(feature = "serde")]
pub use typetag;

pub mod errors;
pub mod interpreter;
pub mod languages;
pub mod values;

type BuildFastHasher = rustc_hash::FxBuildHasher;
type FastHashMap<K, V> = std::collections::HashMap<K, V, BuildFastHasher>;
type FastHashSet<K> = std::collections::HashSet<K, BuildFastHasher>;

macro_rules! debug_unreachable {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        unreachable!($($arg)*)
    };
}

macro_rules! debug_unreachable_or {
    ($expr:expr) => {
        if cfg!(debug_assertions) {
            unreachable!()
        } else {
            $expr
        }
    };
}

pub(crate) use debug_unreachable;
pub(crate) use debug_unreachable_or;
