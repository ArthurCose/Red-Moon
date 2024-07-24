use crate::Lua;
use red_moon::interpreter::CoroutineRef;
use std::ffi::c_void;
use std::fmt;

#[derive(Clone)]
pub struct Thread<'lua> {
    pub(crate) lua: &'lua Lua,
    pub(crate) coroutine_ref: CoroutineRef,
}

impl<'lua> Thread<'lua> {
    /// Converts the thread to a generic C pointer.
    ///
    /// There is no way to convert the pointer back to its original value.
    ///
    /// Typically this function is used only for hashing and debug information.
    #[inline]
    pub fn to_pointer(&self) -> *const c_void {
        self.coroutine_ref.id() as _
    }
}

impl<'lua> fmt::Debug for Thread<'lua> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Thread(Ref({:p}))", self.to_pointer())
    }
}

impl<'lua> PartialEq for Thread<'lua> {
    fn eq(&self, other: &Self) -> bool {
        self.to_pointer() == other.to_pointer()
    }
}

#[cfg(test)]
mod assertions {
    use super::*;

    static_assertions::assert_not_impl_any!(Thread: Send);
}
