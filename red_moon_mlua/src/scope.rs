use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::rc::Rc;

use crate::error::Result;
use crate::function::Function;
use crate::lua::Lua;

use crate::value::{FromLuaMulti, IntoLuaMulti};
use crate::{Error, MultiValue};

type FunctionRc = Rc<dyn for<'a> Fn(&'a Lua, MultiValue<'a>) -> Result<MultiValue<'a>>>;

struct FunctionLifetime {
    inner: Rc<Cell<Option<FunctionRc>>>,
}

impl Drop for FunctionLifetime {
    fn drop(&mut self) {
        self.inner.take();
    }
}

/// Constructed by the [`Lua::scope`] method, allows temporarily creating Lua userdata and
/// callbacks that are not required to be Send or 'static.
///
/// See [`Lua::scope`] for more details.
///
/// [`Lua::scope`]: crate::Lua.html::scope
pub struct Scope<'lua, 'scope>
where
    'lua: 'scope,
{
    lua: &'lua Lua,
    functions: RefCell<Vec<FunctionLifetime>>,
    _scope_invariant: PhantomData<Cell<&'scope ()>>,
}

impl<'lua, 'scope> Scope<'lua, 'scope> {
    pub(crate) fn new(lua: &'lua Lua) -> Scope<'lua, 'scope> {
        Scope {
            lua,
            functions: Default::default(),
            _scope_invariant: PhantomData,
        }
    }

    /// Wraps a Rust function or closure, creating a callable Lua function handle to it.
    ///
    /// This is a version of [`Lua::create_function`] that creates a callback which expires on
    /// scope drop. See [`Lua::scope`] for more details.
    ///
    /// [`Lua::create_function`]: crate::Lua::create_function
    /// [`Lua::scope`]: crate::Lua::scope
    pub fn create_function<'callback, A, R, F>(&'callback self, func: F) -> Result<Function<'lua>>
    where
        A: FromLuaMulti<'callback>,
        R: IntoLuaMulti<'callback>,
        F: Fn(&'callback Lua, A) -> Result<R> + 'scope,
    {
        let func: Rc<
            dyn Fn(&'callback Lua, MultiValue<'callback>) -> Result<MultiValue<'callback>> + 'scope,
        > = Rc::new(move |lua, args| func(lua, A::from_lua_multi(args, lua)?)?.into_lua_multi(lua));
        let func: FunctionRc = unsafe { std::mem::transmute(func) };
        let container = FunctionLifetime {
            inner: Rc::new(Cell::new(Some(func.clone()))),
        };

        let func = container.inner.clone();
        self.functions.borrow_mut().push(container);

        self.lua.create_function(move |lua, a| {
            if let Some(f) = (*func.clone()).take() {
                f(lua, a)
            } else {
                Err(Error::CallbackDestructed)
            }
        })
    }
}
