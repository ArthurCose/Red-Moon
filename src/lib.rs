mod vec_cell;

#[cfg(feature = "serde")]
mod serde_util;

pub mod errors;
pub mod interpreter;
pub mod languages;

type FastHashMap<K, V> = rustc_hash::FxHashMap<K, V>;
type FastHashSet<K> = rustc_hash::FxHashSet<K>;
