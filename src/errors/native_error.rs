use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct NativeError {
    rc: Rc<Box<dyn std::error::Error>>,
}

impl PartialEq for NativeError {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.rc, &other.rc)
    }
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.rc, f)
    }
}

impl<T> From<T> for NativeError
where
    T: std::error::Error + 'static,
{
    fn from(value: T) -> Self {
        Self {
            rc: Rc::new(Box::new(value)),
        }
    }
}
