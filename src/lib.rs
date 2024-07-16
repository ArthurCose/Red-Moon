mod vec_cell;

pub mod errors;
pub mod interpreter;
pub mod languages;

type FastHashMap<K, V> = rustc_hash::FxHashMap<K, V>;
type FastHashSet<K> = rustc_hash::FxHashSet<K>;
