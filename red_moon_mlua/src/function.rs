use crate::error::Result;
use crate::lua::Lua;
use crate::value::{FromLuaMulti, IntoLuaMulti};
use crate::MultiValue;
use red_moon::interpreter::FunctionRef;
use std::ffi::c_void;
use std::fmt;

#[derive(Clone)]
pub struct Function<'lua> {
    pub(crate) lua: &'lua Lua,
    pub(crate) function_ref: FunctionRef,
}

impl<'lua> PartialEq for Function<'lua> {
    fn eq(&self, other: &Self) -> bool {
        self.function_ref == other.function_ref
    }
}

impl<'lua> Function<'lua> {
    /// Calls the function, passing `args` as function arguments.
    ///
    /// The function's return values are converted to the generic type `R`.
    ///
    /// # Examples
    ///
    /// Call Lua's built-in `tostring` function:
    ///
    /// ```
    /// # use rollback_mlua::{Function, Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let globals = lua.globals();
    ///
    /// let tostring: Function = globals.get("tostring")?;
    ///
    /// assert_eq!(tostring.call::<_, String>(123)?, "123");
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Call a function with multiple arguments:
    ///
    /// ```
    /// # use rollback_mlua::{Function, Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let sum: Function = lua.load(
    ///     r#"
    ///         function(a, b)
    ///             return a + b
    ///         end
    /// "#).eval()?;
    ///
    /// assert_eq!(sum.call::<_, u32>((3, 4))?, 3 + 4);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn call<A: IntoLuaMulti<'lua>, R: FromLuaMulti<'lua>>(&self, args: A) -> Result<R> {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();

        // translate args
        let mlua_multi = A::into_lua_multi(args, self.lua)?;
        let red_moon_args = mlua_multi.into_red_moon(self.lua)?;

        // call function
        let results: red_moon::interpreter::MultiValue =
            self.function_ref.call(red_moon_args, ctx)?;

        // translate results
        let mlua_multi = MultiValue::from_red_moon(self.lua, results);

        R::from_lua_multi(mlua_multi, self.lua)
    }

    /// Converts the function to a generic C pointer.
    ///
    /// There is no way to convert the pointer back to its original value.
    ///
    /// Typically this function is used only for hashing and debug information.
    #[inline]
    pub fn to_pointer(&self) -> *const c_void {
        self.function_ref.id() as _
    }
}

impl<'lua> fmt::Debug for Function<'lua> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Function(Ref({:p}))", self.to_pointer())
    }
}
