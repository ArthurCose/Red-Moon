mod vec_cell;

#[cfg(feature = "serde")]
mod serde_util;

pub mod errors;
pub mod interpreter;
pub mod languages;

type FastHashMap<K, V> = rustc_hash::FxHashMap<K, V>;
type FastHashSet<K> = rustc_hash::FxHashSet<K>;

// https://github.com/rust-lang/cargo/issues/383#issuecomment-720873790
#[cfg(doctest)]
mod test_readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            extern "C" {}
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
