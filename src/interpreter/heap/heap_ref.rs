use super::CounterRef;

#[derive(Clone)]
pub(crate) struct HeapRef<K> {
    pub(super) key: K,
    #[allow(dead_code)]
    pub(super) counter_ref: CounterRef,
}

impl<K: Copy> HeapRef<K> {
    pub(crate) fn key(&self) -> K {
        self.key
    }
}

impl<K> PartialEq for HeapRef<K>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K> Eq for HeapRef<K> where K: Eq {}

impl<K> std::fmt::Debug for HeapRef<K>
where
    K: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.key)
    }
}

impl<K> std::hash::Hash for HeapRef<K>
where
    K: std::hash::Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}
