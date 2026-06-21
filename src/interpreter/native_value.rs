/// Allows Rust values to be passed into the VM with snapshotting and serialization support.
///
/// Requires Clone for snapshotting and must be serializable + deserializable + marked with `typetag` when the "serde" feature flag is enabled.
///
/// Automatically derived if the "serde" flag is not enabled, [tag_native_type] can be used otherwise.
///
/// ### impl Example
/// ```
/// # #![cfg(feature = "serde")]
/// use red_moon::interpreter::tag_native_type;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MySingleton {}
///
/// // required only when the "serde" feature is enabled, has no effect otherwise
/// tag_native_type!(MySingleton);
/// ```
#[cfg_attr(feature = "serde", typetag::serde)]
pub trait NativeValue: downcast::Any + CloneBoxedNativeValue {}

#[cfg(not(feature = "serde"))]
impl<T: Clone + 'static> NativeValue for T {}

/// Automatically implemented for all types supporting [NativeValue].
pub trait CloneBoxedNativeValue {
    fn clone_to_boxed_native_value(&self) -> Box<dyn NativeValue>;
}

impl<T: NativeValue + Clone + 'static> CloneBoxedNativeValue for T {
    fn clone_to_boxed_native_value(&self) -> Box<dyn NativeValue> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn NativeValue> {
    fn clone(&self) -> Self {
        self.clone_to_boxed_native_value()
    }
}

downcast::downcast!(dyn NativeValue);

#[cfg(feature = "serde")]
#[macro_export]
macro_rules! tag_native_type {
    ($struct: ty) => {
        #[$crate::typetag::serde]
        impl $crate::interpreter::NativeValue for $struct {}
    };
}

#[cfg(not(feature = "serde"))]
#[macro_export]
macro_rules! tag_native_type {
    ($struct: ty) => {};
}

pub use tag_native_type;
