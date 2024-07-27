use std::rc::{Rc, Weak};

pub(super) struct CounterRef(Weak<()>);

impl Clone for CounterRef {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Default, Clone)]
pub(crate) struct RefCounter {
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
