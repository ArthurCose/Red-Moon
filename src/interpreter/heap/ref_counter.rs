use std::rc::{Rc, Weak};

#[derive(Default, Clone)]
pub(super) struct StrongRef {
    rc: Rc<()>,
}

#[derive(Clone)]
pub(super) struct RefCounter {
    weak: Weak<()>,
}

impl RefCounter {
    pub(super) fn from_strong(strong_ref: &StrongRef) -> Self {
        Self {
            weak: Rc::downgrade(&strong_ref.rc),
        }
    }

    pub(super) fn create_strong(&mut self) -> StrongRef {
        let rc = self.weak.upgrade().unwrap_or_else(|| {
            let rc = Default::default();
            self.weak = Rc::downgrade(&rc);
            rc
        });

        StrongRef { rc }
    }

    pub(super) fn count(&self) -> usize {
        self.weak.strong_count()
    }
}
