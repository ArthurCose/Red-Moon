use super::heap::StackObjectKey;
use std::rc::Rc;

#[cfg(feature = "serde")]
use {
    crate::serde_util::serde_stack_object_key_slice_rc,
    serde::{Deserialize, Serialize},
};

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct UpValues {
    #[cfg_attr(feature = "serde", serde(with = "serde_stack_object_key_slice_rc"))]
    keys: Rc<[StackObjectKey]>,
}

impl UpValues {
    pub(crate) fn heap_size(&self) -> usize {
        // weak count + strong count + data
        std::mem::size_of::<usize>() * 2 + std::mem::size_of_val(&*self.keys)
    }

    pub(crate) fn get(&self, index: usize) -> Option<&StackObjectKey> {
        self.keys.get(index)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &StackObjectKey> {
        self.keys.iter()
    }
}

impl From<Vec<StackObjectKey>> for UpValues {
    fn from(value: Vec<StackObjectKey>) -> Self {
        Self { keys: value.into() }
    }
}
