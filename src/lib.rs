#![doc = include_str!("../README.md")]

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

// https://github.com/rust-lang/cargo/issues/383#issuecomment-720873790
#[cfg(doctest)]
mod test_readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            unsafe extern "C" {}
        };
    }

    external_doc_test!(include_str!("../README.md"));
}

macro_rules! debug_unreachable {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        unreachable!($($arg)*)
    };
}

pub(crate) use debug_unreachable;
