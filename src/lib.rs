pub mod errors;
pub mod interpreter;
pub mod languages;

type FastHashMap<K, V> = rustc_hash::FxHashMap<K, V>;
