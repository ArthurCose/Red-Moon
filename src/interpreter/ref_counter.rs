use std::rc::{Rc, Weak};

#[cfg(feature = "serde")]
use crate::serde_util::serde_unit_rc;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct CounterRef(
    #[cfg_attr(feature = "serde", serde(with = "serde_unit_rc::weak"))] Weak<()>,
);

impl CounterRef {
    pub fn new_empty() -> Self {
        Self(Weak::new())
    }
}

impl Clone for CounterRef {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct RefCounter {
    #[cfg_attr(feature = "serde", serde(with = "serde_unit_rc"))]
    rc: Rc<()>,
}

impl RefCounter {
    pub(super) fn create_counter_ref(&self) -> CounterRef {
        CounterRef(Rc::downgrade(&self.rc))
    }

    pub(super) fn count(&self) -> usize {
        Rc::weak_count(&self.rc)
    }
}
