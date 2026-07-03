use std::rc::Rc;

/// Allows Rust values to be passed into the VM with snapshotting and serialization support.
///
/// Requires Clone for snapshotting and must be serializable + deserializable + marked with `typetag` when the "serde" feature flag is enabled.
///
/// Automatically derived if the "serde" flag is not enabled, [tag_native_type] can be used otherwise.
///
/// ```
/// # fn main() {
/// # #[cfg(feature = "serde")] {
/// use red_moon::values::tag_native_type;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MySingleton {}
///
/// // required only when the "serde" feature is enabled, has no effect otherwise
/// tag_native_type!(MySingleton);
/// # } }
/// ```
#[cfg_attr(feature = "serde", typetag::serde)]
pub trait NativeValue: downcast::Any + CloneSharedNativeValue {}

#[cfg(not(feature = "serde"))]
impl<T: Clone + 'static> NativeValue for T {}

/// Automatically implemented for all types supporting [NativeValue].
pub trait CloneSharedNativeValue {
    fn deep_clone_shared_native_value(&self) -> Rc<dyn NativeValue>;
}

impl<T: NativeValue + Clone + 'static> CloneSharedNativeValue for T {
    fn deep_clone_shared_native_value(&self) -> Rc<dyn NativeValue> {
        Rc::new(self.clone())
    }
}

downcast::downcast!(dyn NativeValue);

/// Implements [NativeValue] and applies `typetag::serde` when the `serde` feature is enabled.
///
/// ```
/// use red_moon::values::tag_native_type;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MySingleton {}
///
/// // required only when the "serde" feature is enabled, has no effect otherwise
/// tag_native_type!(MySingleton);
/// ```
#[cfg(feature = "serde")]
#[macro_export]
macro_rules! tag_native_type {
    ($struct: ty) => {
        const _: () = {
            use $crate::typetag;

            #[typetag::serde]
            impl $crate::values::NativeValue for $struct {}
        };
    };
}

/// Implements [NativeValue] and applies `typetag::serde` when the `serde` feature is enabled.
///
/// ```
/// # fn main() {
/// # #[cfg(feature = "serde")] {
/// use red_moon::values::tag_native_type;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MySingleton {}
///
/// // required only when the "serde" feature is enabled, has no effect otherwise
/// tag_native_type!(MySingleton);
/// # } }
/// ```
#[cfg(not(feature = "serde"))]
#[macro_export]
macro_rules! tag_native_type {
    ($struct: ty) => {};
}

pub use tag_native_type;

/// Shares NativeValues between VM clones, creates a private copy of the value when it's time to mutate
#[derive(Clone)]
pub(crate) struct SharedNativeValue {
    v: Rc<dyn NativeValue>,
}

impl SharedNativeValue {
    pub(crate) fn new<T: NativeValue>(value: T) -> Self {
        Self { v: Rc::from(value) }
    }

    #[cfg(feature = "serde")]
    pub(crate) fn stored_type_id(&self) -> std::any::TypeId {
        self.v.type_id()
    }

    pub(crate) fn take<T: Clone + 'static>(self) -> Option<T> {
        let rc = self.v.downcast_rc::<T>().ok()?;
        let weak = Rc::downgrade(&rc);

        Some(Rc::into_inner(rc).unwrap_or_else(move || T::clone(&weak.upgrade().unwrap())))
    }

    pub(crate) fn get<T: 'static>(&self) -> Option<&T> {
        self.v.downcast_ref::<T>().ok()
    }

    pub(crate) fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        if Rc::strong_count(&self.v) != 1 {
            // avoid modifying data in vm clones
            self.v = (*self.v).deep_clone_shared_native_value();
        }

        Rc::get_mut(&mut self.v).and_then(|v| v.downcast_mut().ok())
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for SharedNativeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_newtype_struct("SharedNativeValue", &*self.v)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for SharedNativeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::Deserialize;

        struct SharedNativeValueVisitor;

        impl<'de> serde::de::Visitor<'de> for SharedNativeValueVisitor {
            type Value = SharedNativeValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("SharedNativeValue")
            }

            fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = Box::<dyn NativeValue>::deserialize(deserializer)?;

                Ok(SharedNativeValue {
                    v: Rc::<dyn NativeValue>::from(value),
                })
            }
        }

        deserializer.deserialize_newtype_struct("SharedNativeValue", SharedNativeValueVisitor)
    }
}
