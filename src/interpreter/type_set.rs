use crate::FastHashMap;
use crate::interpreter::NativeValue;
use std::any::TypeId;

#[derive(Default, Clone)]
pub(crate) struct TypeSet(FastHashMap<TypeId, Box<dyn NativeValue>>);

impl TypeSet {
    pub fn insert<T: NativeValue + Clone + 'static>(&mut self, value: T) -> Option<T> {
        self.0
            .insert(TypeId::of::<T>(), Box::new(value))
            .map(|b| *b.downcast::<T>().unwrap())
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .map(|b| b.downcast_ref::<T>().unwrap())
    }

    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.0
            .get_mut(&TypeId::of::<T>())
            .map(|b| b.downcast_mut::<T>().unwrap())
    }

    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        self.0
            .remove(&TypeId::of::<T>())
            .map(|b| *b.downcast::<T>().unwrap())
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for TypeSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut state = serializer.serialize_seq(Some(self.0.len()))?;

        for value in self.0.values() {
            state.serialize_element(value)?;
        }

        state.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for TypeSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SeqVisitor;

        impl<'de> serde::de::Visitor<'de> for SeqVisitor {
            type Value = TypeSet;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("TypeSet")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut set = TypeSet::default();

                while let Some(element) = seq.next_element::<Box<dyn NativeValue>>()? {
                    set.0.insert(element.type_id(), element);
                }

                Ok(set)
            }
        }

        deserializer.deserialize_seq(SeqVisitor)
    }
}
