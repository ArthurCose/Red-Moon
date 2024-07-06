use std::borrow::Cow;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::string::String as StdString;

use crate::error::{Error, ErrorContext, Result};
use crate::function::Function;
use crate::lua::Lua;
use crate::table::Table;
use crate::value::{FromLuaMulti, IntoLua, IntoLuaMulti};

/// Trait for types [loadable by Lua] and convertible to a [`Chunk`]
///
/// [loadable by Lua]: https://www.lua.org/manual/5.4/manual.html#3.3.2
/// [`Chunk`]: crate::Chunk
pub trait AsChunk<'lua, 'a> {
    /// Returns optional chunk name
    fn name(&self) -> Option<StdString> {
        None
    }

    /// Returns optional chunk [environment]
    ///
    /// [environment]: https://www.lua.org/manual/5.4/manual.html#2.2
    fn environment(&self, lua: &'lua Lua) -> Result<Option<Table<'lua>>> {
        let _lua = lua; // suppress warning
        Ok(None)
    }

    /// Returns optional chunk mode (text or binary)
    fn mode(&self) -> Option<ChunkMode> {
        None
    }

    /// Returns chunk data (can be text or binary)
    fn source(self) -> IoResult<Cow<'a, [u8]>>;
}

impl<'a> AsChunk<'_, 'a> for &'a str {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self.as_ref()))
    }
}

impl AsChunk<'_, 'static> for StdString {
    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self.into_bytes()))
    }
}

impl<'a> AsChunk<'_, 'a> for &'a StdString {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

impl<'a> AsChunk<'_, 'a> for &'a [u8] {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self))
    }
}

impl AsChunk<'_, 'static> for Vec<u8> {
    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self))
    }
}

impl<'a> AsChunk<'_, 'a> for &'a Vec<u8> {
    fn source(self) -> IoResult<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(self.as_ref()))
    }
}

impl AsChunk<'_, 'static> for &Path {
    fn name(&self) -> Option<StdString> {
        Some(format!("@{}", self.display()))
    }

    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        std::fs::read(self).map(Cow::Owned)
    }
}

impl AsChunk<'_, 'static> for PathBuf {
    fn name(&self) -> Option<StdString> {
        Some(format!("@{}", self.display()))
    }

    fn source(self) -> IoResult<Cow<'static, [u8]>> {
        std::fs::read(self).map(Cow::Owned)
    }
}

/// Returned from [`Lua::load`] and is used to finalize loading and executing Lua main chunks.
///
/// [`Lua::load`]: crate::Lua::load
#[must_use = "`Chunk`s do nothing unless one of `exec`, `eval`, `call`, or `into_function` are called on them"]
pub struct Chunk<'lua, 'a> {
    pub(crate) lua: &'lua Lua,
    pub(crate) name: StdString,
    pub(crate) env: Result<Option<Table<'lua>>>,
    pub(crate) source: IoResult<Cow<'a, [u8]>>,
}

/// Represents chunk mode (text or binary).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkMode {
    Text,
    Binary,
}

impl<'lua, 'a> Chunk<'lua, 'a> {
    /// Sets the name of this chunk, which results in more informative error traces.
    pub fn set_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Sets the environment of the loaded chunk to the given value.
    ///
    /// In Lua >=5.2 main chunks always have exactly one upvalue, and this upvalue is used as the `_ENV`
    /// variable inside the chunk. By default this value is set to the global environment.
    ///
    /// Calling this method changes the `_ENV` upvalue to the value provided, and variables inside
    /// the chunk will refer to the given environment rather than the global one.
    ///
    /// All global variables (including the standard library!) are looked up in `_ENV`, so it may be
    /// necessary to populate the environment in order for scripts using custom environments to be
    /// useful.
    pub fn set_environment<V: IntoLua<'lua>>(mut self, env: V) -> Self {
        self.env = env
            .into_lua(self.lua)
            .and_then(|val| self.lua.unpack(val))
            .context("bad environment value");
        self
    }

    /// Execute this chunk of code.
    ///
    /// This is equivalent to calling the chunk function with no arguments and no return values.
    pub fn exec(self) -> Result<()> {
        self.call(())?;
        Ok(())
    }

    /// Evaluate the chunk as either an expression or block.
    ///
    /// If the chunk can be parsed as an expression, this loads and executes the chunk and returns
    /// the value that it evaluates to. Otherwise, the chunk is interpreted as a block as normal,
    /// and this is equivalent to calling `exec`.
    pub fn eval<R: FromLuaMulti<'lua>>(self) -> Result<R> {
        // Bytecode is always interpreted as a statement.
        // For source code, first try interpreting the lua as an expression by adding
        // "return", then as a statement. This is the same thing the
        // actual lua repl does.
        let res = self.to_expression();

        match res {
            Ok(function) => function.call(()),
            Err(Error::MemoryError(_)) => Err(res.unwrap_err()),
            _ => self.call(()),
        }
    }

    /// Load the chunk function and call it with the given arguments.
    ///
    /// This is equivalent to `into_function` and calling the resulting function.
    pub fn call<A: IntoLuaMulti<'lua>, R: FromLuaMulti<'lua>>(self, args: A) -> Result<R> {
        self.into_function()?.call(args)
    }

    /// Load this chunk into a regular `Function`.
    ///
    /// This simply compiles the chunk without actually executing it.
    pub fn into_function(self) -> Result<Function<'lua>> {
        let compiler = self.lua.compiler();
        let source = self.source?;
        let module = compiler.compile(std::str::from_utf8(&source)?)?;
        let environment = self.env?.map(|table| table.table_ref);

        let vm = unsafe { self.lua.vm_mut() };

        Ok(Function {
            lua: self.lua,
            function_ref: vm.load_function(self.name.as_str(), environment, module)?,
        })
    }

    fn to_expression(&self) -> Result<Function<'lua>> {
        // We assume that mode is Text
        let source = self.source.as_ref();
        let source = source.map_err(Error::runtime)?;
        let source = Self::expression_source(source);

        let compiler = self.lua.compiler();
        let module = compiler.compile(std::str::from_utf8(&source)?)?;
        let environment = self.env.clone()?.map(|table| table.table_ref.clone());

        let vm = unsafe { self.lua.vm_mut() };

        Ok(Function {
            lua: self.lua,
            function_ref: vm.load_function(self.name.as_str(), environment, module)?,
        })
    }

    fn expression_source(source: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(b"return ".len() + source.len());
        buf.extend(b"return ");
        buf.extend(source);
        buf
    }
}
