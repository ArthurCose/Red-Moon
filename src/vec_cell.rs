use std::cell::UnsafeCell;

/// A Vec with interior mutability, using the ideas behind Cell:
/// no references, only taking, setting, and replacing values are valid operations
pub(crate) struct VecCell<T> {
    inner: UnsafeCell<Vec<T>>,
}

impl<T> VecCell<T> {
    pub(crate) fn len(&self) -> usize {
        let inner = unsafe { &mut *self.inner.get() };
        inner.len()
    }

    pub(crate) fn push(&self, value: T) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.push(value);
    }

    pub(crate) fn pop(&self) -> Option<T> {
        let inner = unsafe { &mut *self.inner.get() };
        inner.pop()
    }
}

impl<T> Default for VecCell<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}
