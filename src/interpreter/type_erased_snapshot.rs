/// Requires Clone for snapshotting and must be serializable + deserializable + marked with `typetag` when the "serde" feature flag is enabled.
///
/// Automatically derived if the "serde" flag is not enabled.
///
/// ### impl Example
/// ```
/// use red_moon::interpreter::TypeErasedSnapshot;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MySingleton {}
///
/// // typetag is required when the "serde" feature is enabled
/// #[typetag::serde]
/// impl TypeErasedSnapshot for MySingleton {}
/// ```
#[cfg_attr(feature = "serde", typetag::serde)]
pub trait TypeErasedSnapshot: downcast::Any + CloneBoxedSnapshot {}

#[cfg(not(feature = "serde"))]
impl<T: Clone + 'static> TypeErasedSnapshot for T {}

/// Automatically implemented for all types supporting Clone.
pub trait CloneBoxedSnapshot {
    fn clone_to_boxed_singleton(&self) -> Box<dyn TypeErasedSnapshot>;
}

impl<T: TypeErasedSnapshot + Clone + 'static> CloneBoxedSnapshot for T {
    fn clone_to_boxed_singleton(&self) -> Box<dyn TypeErasedSnapshot> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn TypeErasedSnapshot> {
    fn clone(&self) -> Self {
        self.clone_to_boxed_singleton()
    }
}

downcast::downcast!(dyn TypeErasedSnapshot);
