use super::{CounterRef, HeapKey};

#[derive(Clone)]
pub(crate) struct HeapRef {
    pub(super) key: HeapKey,
    #[allow(dead_code)]
    pub(super) counter_ref: CounterRef,
}

impl HeapRef {
    pub(crate) fn key(&self) -> HeapKey {
        self.key
    }
}

impl PartialEq for HeapRef {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapRef {}

impl std::fmt::Debug for HeapRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.key)
    }
}

impl std::hash::Hash for HeapRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}
