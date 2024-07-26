macro_rules! impl_serde_serialize_stub_fn {
    ($name:ident, $type:ty) => {
        fn $name<S>(_: &$type, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Serialize::serialize(&(), serializer)
        }
    };
}

macro_rules! impl_serde_deserialize_stub_fn {
    ($name:ident, $type:ty, $default_value:expr) => {
        fn $name<'de, D>(deserializer: D) -> Result<$type, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let _: () = serde::Deserialize::deserialize(deserializer)?;
            Ok($default_value)
        }
    };
}

macro_rules! impl_serde_rc {
    ($module_name:ident, $type:ty, $borrowed:ty) => {
        mod $module_name {
            use super::*;

            pub(super) fn serialize<S>(s: &$type, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let s = &**s;
                serde::Serialize::serialize(&s, serializer)
            }

            pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<$type, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserialize::deserialize(deserializer).map(|s: $borrowed| s.into())
            }
        }
    };
}

pub(crate) use impl_serde_deserialize_stub_fn;
pub(crate) use impl_serde_rc;
pub(crate) use impl_serde_serialize_stub_fn;
