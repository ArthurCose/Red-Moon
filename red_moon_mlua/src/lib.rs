mod app_data;
mod chunk;
mod conversion;
mod error;
mod function;
mod lua;
mod multi;
mod stdlib;
mod string;
mod table;
mod thread;
mod types;
mod value;

pub use crate::chunk::{AsChunk, Chunk, ChunkMode};
pub use crate::table::{Table, TableExt, TablePairs, TableSequence};
pub use error::{Error, ErrorContext, ExternalError, ExternalResult, Result};
pub use function::Function;
pub use lua::{Lua, RegistryKey};
pub use stdlib::StdLib;
pub use string::String;
pub use thread::Thread;
pub use types::{Integer, LightUserData, Number};
pub use value::{FromLua, FromLuaMulti, IntoLua, IntoLuaMulti, MultiValue, Nil, Value};

#[cfg(feature = "serialize")]
#[doc(inline)]
pub use crate::serde::{
    de::Options as DeserializeOptions, ser::Options as SerializeOptions, LuaSerdeExt,
};

#[cfg(feature = "serialize")]
#[cfg_attr(docsrs, doc(cfg(feature = "serialize")))]
pub mod serde;

pub mod prelude {
    pub use crate::chunk::Chunk as LuaChunk;
    pub use crate::error::{
        Error as LuaError, ErrorContext as LuaErrorContext, ExternalError as LuaExternalError,
        ExternalResult as LuaExternalResult, Result as LuaResult,
    };
    pub use crate::function::Function as LuaFunction;
    pub use crate::lua::{Lua, RegistryKey as LuaRegistryKey};
    pub use crate::stdlib::StdLib as LuaStdLib;
    pub use crate::string::String as LuaString;
    pub use crate::table::{
        Table as LuaTable, TableExt as LuaTableExt, TablePairs as LuaTablePairs,
        TableSequence as LuaTableSequence,
    };
    pub use crate::thread::Thread as LuaThread;
    pub use crate::types::{
        Integer as LuaInteger, LightUserData as LuaLightUserData, Number as LuaNumber,
    };
    pub use crate::value::{
        FromLua, FromLuaMulti, IntoLua, IntoLuaMulti, MultiValue, Nil as LuaNil, Value as LuaValue,
    };
}

pub(crate) mod private {
    use super::*;

    pub trait Sealed {}

    impl Sealed for Error {}
    impl<T> Sealed for std::result::Result<T, Error> {}
    impl Sealed for Lua {}
    impl Sealed for Table<'_> {}
}
