use crate::*;
use app_data::{AppDataRef, AppDataRefMut};
use red_moon::interpreter::{ByteString, TableRef, Vm};
use red_moon::languages::lua::std::{impl_basic, impl_math, impl_table};
use red_moon::languages::lua::{coerce_integer, parse_number, LuaCompiler};
use rustc_hash::FxHashMap;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::panic::Location;
use std::sync::Arc;

const MULTIVALUE_POOL_SIZE: usize = 64;

pub struct RegistryKey {
    id: slotmap::DefaultKey,
    lua_identifier: Arc<()>,
}

#[derive(Clone)]
struct MutableResources {
    multivalue_pool: Vec<Vec<Value<'static>>>,
    registry: slotmap::SlotMap<slotmap::DefaultKey, red_moon::interpreter::Value>,
    named_registry: FxHashMap<ByteString, red_moon::interpreter::Value>,
}

#[derive(Clone)]
struct Snapshot {
    vm: Vm,
    resources: MutableResources,
    count: usize,
}

pub struct Lua {
    /// Mutable usage must either be short lived, or only reused when access is given back to the user
    ///
    /// Examples:
    /// - Creating a string, creating a table, accessing or setting values, should never use vm longer than a function call and never overlap
    /// - Native function calls pass `&mut VM`` as a parameter, we're safe to use our UnsafeCell in there
    vm: UnsafeCell<Vm>,
    snapshots: Vec<Snapshot>,
    modified: Cell<bool>,
    compiler: LuaCompiler,
    resources: RefCell<MutableResources>,
    nil_registry_id: slotmap::DefaultKey,
    app_data_borrows: Cell<usize>,
    identifier: Arc<()>,
    #[cfg(feature = "serialize")]
    array_metatable: TableRef,
}

impl Default for Lua {
    fn default() -> Self {
        let mut vm = Vm::default();
        let array_metatable = vm.create_table();
        let mut registry = slotmap::SlotMap::default();
        let nil_id = registry.insert(Default::default());

        impl_basic(&mut vm).unwrap();

        Self {
            vm: UnsafeCell::new(vm),
            snapshots: Default::default(),
            modified: Cell::new(false),
            compiler: Default::default(),
            resources: RefCell::new(MutableResources {
                multivalue_pool: Default::default(),
                registry,
                named_registry: Default::default(),
            }),
            nil_registry_id: nil_id,
            identifier: Arc::default(),
            app_data_borrows: Default::default(),
            #[cfg(feature = "serialize")]
            array_metatable,
        }
    }
}

impl std::fmt::Debug for Lua {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Lua(?)")
    }
}

impl Lua {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) unsafe fn vm(&self) -> &Vm {
        &*self.vm.get()
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn vm_mut(&self) -> &mut Vm {
        &mut *self.vm.get()
    }

    pub(crate) fn compiler(&self) -> &LuaCompiler {
        &self.compiler
    }

    #[cfg(feature = "serialize")]
    pub(crate) fn array_metatable_ref(&self) -> &TableRef {
        &self.array_metatable
    }

    pub(crate) fn pop_multivalue_from_pool(&self) -> Option<Vec<Value>> {
        let mut resources = self.resources.borrow_mut();

        resources.multivalue_pool.pop()
    }

    pub(crate) fn push_multivalue_to_pool(&self, mut multivalue: Vec<Value>) {
        let mut resources = self.resources.borrow_mut();
        let multivalue_pool = &mut resources.multivalue_pool;

        if multivalue_pool.len() < MULTIVALUE_POOL_SIZE {
            multivalue.clear();
            multivalue_pool.push(unsafe {
                std::mem::transmute::<Vec<Value<'_>>, Vec<Value<'static>>>(multivalue)
            });
        }
    }

    pub fn snap(&mut self) {
        if !self.modified.get() {
            if let Some(snapshot) = self.snapshots.last_mut() {
                snapshot.count += 1;
                return;
            }
        }

        self.snapshots.push(Snapshot {
            vm: self.vm.get_mut().clone(),
            resources: self.resources.borrow_mut().clone(),
            count: 1,
        });
    }

    pub fn rollback(&mut self, mut n: usize) {
        while let Some(snapshot) = self.snapshots.last_mut() {
            if snapshot.count < n {
                n -= snapshot.count;
                self.snapshots.pop();
                continue;
            }

            snapshot.count -= n;

            if snapshot.count == 0 {
                let snapshot = self.snapshots.pop().unwrap();
                *self.vm.get_mut() = snapshot.vm;
                *self.resources.borrow_mut() = snapshot.resources;
            } else {
                self.vm.get_mut().clone_from(&snapshot.vm);
                self.resources.borrow_mut().clone_from(&snapshot.resources);
            }

            return;
        }

        #[cfg(debug_assertions)]
        panic!("Not enough snapshots")
    }

    /// Loads the specified subset of the standard libraries into an existing Lua state.
    ///
    /// Use the [`StdLib`] flags to specify the libraries you want to load.
    ///
    /// [`StdLib`]: crate::StdLib
    pub fn load_from_std_lib(&self, libs: StdLib) -> Result<()> {
        self.modified.set(true);

        if libs.contains(StdLib::TABLE) {
            impl_table(unsafe { self.vm_mut() })?;
        }

        if libs.contains(StdLib::MATH) {
            impl_math(unsafe { self.vm_mut() })?;
        }

        Ok(())
    }

    /// Perform a full garbage-collection cycle.
    ///
    /// It may be necessary to call this function twice to collect all currently unreachable
    /// objects. Once to finish the current gc cycle, and once to start and finish the next cycle.
    pub fn gc_collect(&self) -> Result<()> {
        let vm = unsafe { self.vm_mut() };
        vm.gc_collect();
        Ok(())
    }

    /// Returns Lua source code as a `Chunk` builder type.
    ///
    /// In order to actually compile or run the resulting code, you must call [`Chunk::exec`] or
    /// similar on the returned builder. Code is not even parsed until one of these methods is
    /// called.
    ///
    /// [`Chunk::exec`]: crate::Chunk::exec
    #[track_caller]
    pub fn load<'lua, 'a>(&'lua self, chunk: impl AsChunk<'lua, 'a>) -> Chunk<'lua, 'a> {
        self.modified.set(true);

        let caller = Location::caller();

        Chunk {
            lua: self,
            name: chunk.name().unwrap_or_else(|| caller.to_string()),
            env: chunk.environment(self),
            source: chunk.source(),
        }
    }

    pub fn environment(&self) -> Result<Table> {
        self.modified.set(true);

        let vm = unsafe { self.vm_mut() };
        let Some(table_ref) = vm.environment_up_value() else {
            return Ok(self.globals());
        };

        Ok(Table {
            lua: self,
            table_ref,
        })
    }

    #[inline]
    pub fn create_string(&self, s: impl AsRef<[u8]>) -> Result<String> {
        let vm = unsafe { self.vm_mut() };
        let string_ref = vm.intern_string(s.as_ref());

        Ok(String {
            lua: self,
            string_ref,
            byte_string: Default::default(),
        })
    }

    /// Creates and returns a new empty table.
    pub fn create_table(&self) -> Result<Table> {
        let vm = unsafe { self.vm_mut() };
        let table_ref = vm.create_table();

        Ok(Table {
            lua: self,
            table_ref,
        })
    }

    /// Creates and returns a new empty table, with the specified capacity.
    /// `narr` is a hint for how many elements the table will have as a sequence;
    /// `nrec` is a hint for how many other elements the table will have.
    /// Lua may use these hints to preallocate memory for the new table.
    pub fn create_table_with_capacity(&self, narr: usize, nrec: usize) -> Result<Table> {
        let vm = unsafe { self.vm_mut() };
        let table_ref = vm.create_table_with_capacity(narr, nrec);

        Ok(Table {
            lua: self,
            table_ref,
        })
    }

    /// Creates a table and fills it with values from an iterator.
    pub fn create_table_from<'lua, K, V, I>(&'lua self, iter: I) -> Result<Table<'lua>>
    where
        K: IntoLua<'lua>,
        V: IntoLua<'lua>,
        I: IntoIterator<Item = (K, V)>,
    {
        let vm = unsafe { self.vm_mut() };
        let table_ref = vm.create_table();

        for (k, v) in iter.into_iter() {
            table_ref.raw_set(
                k.into_lua(self)?.into_red_moon(),
                v.into_lua(self)?.into_red_moon(),
                vm,
            )?;
        }

        Ok(Table {
            lua: self,
            table_ref,
        })
    }

    /// Creates a table from an iterator of values, using `1..` as the keys.
    pub fn create_sequence_from<'lua, T, I>(&'lua self, iter: I) -> Result<Table<'lua>>
    where
        T: IntoLua<'lua>,
        I: IntoIterator<Item = T>,
    {
        let vm = unsafe { self.vm_mut() };
        let table_ref = vm.create_table();

        for (i, v) in iter.into_iter().enumerate() {
            table_ref.raw_set(i + 1, v.into_lua(self)?.into_red_moon(), vm)?;
        }

        Ok(Table {
            lua: self,
            table_ref,
        })
    }

    /// Wraps a Rust function or closure, creating a callable Lua function handle to it.
    ///
    /// The function's return value is always a `Result`: If the function returns `Err`, the error
    /// is raised as a Lua error, which can be caught using `(x)pcall` or bubble up to the Rust code
    /// that invoked the Lua code. This allows using the `?` operator to propagate errors through
    /// intermediate Lua code.
    ///
    /// If the function returns `Ok`, the contained value will be converted to one or more Lua
    /// values. For details on Rust-to-Lua conversions, refer to the [`IntoLua`] and [`IntoLuaMulti`]
    /// traits.
    ///
    /// # Examples
    ///
    /// Create a function which prints its argument:
    ///
    /// ```
    /// # use rollback_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let greet = lua.create_function(|_, name: String| {
    ///     println!("Hello, {}!", name);
    ///     Ok(())
    /// });
    /// # let _ = greet;    // used
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Use tuples to accept multiple arguments:
    ///
    /// ```
    /// # use rollback_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let print_person = lua.create_function(|_, (name, age): (String, u8)| {
    ///     println!("{} is {} years old!", name, age);
    ///     Ok(())
    /// });
    /// # let _ = print_person;    // used
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`IntoLua`]: crate::IntoLua
    /// [`IntoLuaMulti`]: crate::IntoLuaMulti
    pub fn create_function<'lua, A, R, F>(&'lua self, func: F) -> Result<Function<'lua>>
    where
        A: FromLuaMulti<'lua>,
        R: IntoLuaMulti<'lua>,
        F: Fn(&'lua Lua, A) -> Result<R> + 'static,
    {
        let func = std::rc::Rc::new(func);

        let vm = unsafe { self.vm_mut() };
        let function_ref = vm.create_native_function(move |args, vm| {
            let lua = vm.app_data::<*const Lua>().unwrap();
            let lua = unsafe { &**lua };

            // translate args
            let mlua_multi = MultiValue::from_red_moon(lua, args);

            // call function
            let results = func(lua, A::from_lua_multi(mlua_multi, lua)?)?;

            // translate results
            let mlua_multi = results.into_lua_multi(lua)?;

            Ok(mlua_multi.into_red_moon(lua)?)
        });

        Ok(Function {
            lua: self,
            function_ref,
        })
    }

    #[inline]
    pub fn globals(&self) -> Table {
        let vm = unsafe { self.vm() };
        let table_ref = vm.default_environment();

        self.modified.set(true);

        Table {
            lua: self,
            table_ref,
        }
    }

    /// Calls the given function with a `Scope` parameter, giving the function the ability to create
    /// userdata and callbacks from rust types that are !Send or non-'static.
    ///
    /// The lifetime of any function or userdata created through `Scope` lasts only until the
    /// completion of this method call, on completion all such created values are automatically
    /// dropped and Lua references to them are invalidated. If a script accesses a value created
    /// through `Scope` outside of this method, a Lua error will result. Since we can ensure the
    /// lifetime of values created through `Scope`, and we know that `Lua` cannot be sent to another
    /// thread while `Scope` is live, it is safe to allow !Send datatypes and whose lifetimes only
    /// outlive the scope lifetime.
    ///
    /// Inside the scope callback, all handles created through Scope will share the same unique 'lua
    /// lifetime of the parent `Lua`. This allows scoped and non-scoped values to be mixed in
    /// API calls, which is very useful (e.g. passing a scoped userdata to a non-scoped function).
    /// However, this also enables handles to scoped values to be trivially leaked from the given
    /// callback. This is not dangerous, though!  After the callback returns, all scoped values are
    /// invalidated, which means that though references may exist, the Rust types backing them have
    /// dropped. `Function` types will error when called, and `AnyUserData` will be typeless. It
    /// would be impossible to prevent handles to scoped values from escaping anyway, since you
    /// would always be able to smuggle them through Lua state.
    pub fn scope<'lua, 'scope, R>(
        &'lua self,
        f: impl FnOnce(&Scope<'lua, 'scope>) -> Result<R>,
    ) -> Result<R>
    where
        'lua: 'scope,
    {
        f(&Scope::new(self))
    }

    /// Attempts to coerce a Lua value into a String in a manner consistent with Lua's internal
    /// behavior.
    ///
    /// To succeed, the value must be a string (in which case this is a no-op), an integer, or a
    /// number.
    pub fn coerce_string<'lua>(&'lua self, v: Value<'lua>) -> Result<Option<String<'lua>>> {
        Ok(match v {
            Value::String(s) => Some(s),
            Value::Integer(i) => Some(self.create_string(i.to_string())?),
            Value::Number(f) => Some(self.create_string(f.to_string())?),
            _ => None,
        })
    }

    /// Attempts to coerce a Lua value into an integer in a manner consistent with Lua's internal
    /// behavior.
    ///
    /// To succeed, the value must be an integer, a floating point number that has an exact
    /// representation as an integer, or a string that can be converted to an integer. Refer to the
    /// Lua manual for details.
    pub fn coerce_integer(&self, v: Value) -> Result<Option<Integer>> {
        use red_moon::languages::lua::Number;

        Ok(match v {
            Value::Integer(i) => Some(i),
            Value::Number(f) => coerce_integer(f),
            Value::String(s) => {
                let s = s.to_str()?;

                parse_number(s).and_then(|number| match number {
                    Number::Integer(i) => Some(i),
                    Number::Float(n) => coerce_integer(n),
                })
            }
            _ => None,
        })
    }

    /// Attempts to coerce a Lua value into a Number in a manner consistent with Lua's internal
    /// behavior.
    ///
    /// To succeed, the value must be a number or a string that can be converted to a number. Refer
    /// to the Lua manual for details.
    pub fn coerce_number(&self, v: Value) -> Result<Option<Number>> {
        use red_moon::languages::lua::Number;

        Ok(match v {
            Value::Number(n) => Some(n),
            Value::Integer(n) => Some(n as f64),
            Value::String(s) => {
                let s = s.to_str()?;

                parse_number(s).map(|number| match number {
                    Number::Integer(i) => i as f64,
                    Number::Float(f) => f,
                })
            }
            _ => None,
        })
    }

    /// Converts a value that implements `IntoLua` into a `Value` instance.
    pub fn pack<'lua, T: IntoLua<'lua>>(&'lua self, t: T) -> Result<Value<'lua>> {
        t.into_lua(self)
    }

    /// Converts a `Value` instance into a value that implements `FromLua`.
    pub fn unpack<'lua, T: FromLua<'lua>>(&'lua self, value: Value<'lua>) -> Result<T> {
        T::from_lua(value, self)
    }

    /// Converts a value that implements `IntoLuaMulti` into a `MultiValue` instance.
    pub fn pack_multi<'lua, T: IntoLuaMulti<'lua>>(&'lua self, t: T) -> Result<MultiValue<'lua>> {
        t.into_lua_multi(self)
    }

    /// Converts a `MultiValue` instance into a value that implements `FromLuaMulti`.
    pub fn unpack_multi<'lua, T: FromLuaMulti<'lua>>(
        &'lua self,
        value: MultiValue<'lua>,
    ) -> Result<T> {
        T::from_lua_multi(value, self)
    }

    /// Set a value in the Lua registry based on a string name.
    ///
    /// This value will be available to rust from all `Lua` instances which share the same main
    /// state.
    pub fn set_named_registry_value<'lua, T>(&'lua self, name: &str, t: T) -> Result<()>
    where
        T: IntoLua<'lua>,
    {
        self.modified.set(true);

        let vm = unsafe { self.vm_mut() };
        let name = vm.intern_string(name.as_bytes());
        let name = name.fetch(vm)?.clone();
        let value = t.into_lua(self)?.into_red_moon();

        self.resources
            .borrow_mut()
            .named_registry
            .insert(name, value);

        Ok(())
    }

    /// Get a value from the Lua registry based on a string name.
    ///
    /// Any Lua instance which shares the underlying main state may call this method to
    /// get a value previously set by [`set_named_registry_value`].
    ///
    /// [`set_named_registry_value`]: #method.set_named_registry_value
    pub fn named_registry_value<'lua, T>(&'lua self, name: &str) -> Result<T>
    where
        T: FromLua<'lua>,
    {
        self.modified.set(true);

        let resources = self.resources.borrow();
        let value = resources
            .named_registry
            .get(name.as_bytes())
            .cloned()
            .unwrap_or_default();

        let value = Value::from_red_moon(self, value);

        T::from_lua(value, self)
    }

    /// Removes a named value in the Lua registry.
    ///
    /// Equivalent to calling [`set_named_registry_value`] with a value of Nil.
    ///
    /// [`set_named_registry_value`]: #method.set_named_registry_value
    pub fn unset_named_registry_value(&self, name: &str) -> Result<()> {
        self.modified.set(true);

        let mut resources = self.resources.borrow_mut();
        resources.named_registry.remove(name.as_bytes());

        Ok(())
    }

    /// Place a value in the Lua registry with an auto-generated key.
    ///
    /// This value will be available to Rust from all `Lua` instances which share the same main
    /// state.
    ///
    /// Be warned, garbage collection of values held inside the registry is not automatic, see
    /// [`RegistryKey`] for more details.
    /// However, dropped [`RegistryKey`]s automatically reused to store new values.
    ///
    /// [`RegistryKey`]: crate::RegistryKey
    pub fn create_registry_value<'lua, T: IntoLua<'lua>>(&'lua self, t: T) -> Result<RegistryKey> {
        let t = t.into_lua(self)?;
        if t == Value::Nil {
            // Special case to skip calling `luaL_ref` and use `LUA_REFNIL` instead
            return Ok(RegistryKey {
                id: self.nil_registry_id,
                lua_identifier: self.identifier.clone(),
            });
        }

        self.modified.set(true);

        let value = t.into_red_moon();
        let mut resources = self.resources.borrow_mut();
        let registry_id = resources.registry.insert(value);

        let registry_key = RegistryKey {
            id: registry_id,
            lua_identifier: self.identifier.clone(),
        };

        Ok(registry_key)
    }

    /// Get a value from the Lua registry by its `RegistryKey`
    ///
    /// Any Lua instance which shares the underlying main state may call this method to get a value
    /// previously placed by [`create_registry_value`].
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    pub fn registry_value<'lua, T: FromLua<'lua>>(&'lua self, key: &RegistryKey) -> Result<T> {
        if !self.owns_registry_value(key) {
            return Err(Error::MismatchedRegistryKey);
        }

        self.modified.set(true);

        let red_moon_value = {
            let resources = self.resources.borrow();
            resources.registry.get(key.id).cloned().unwrap_or_default()
        };
        let value = Value::from_red_moon(self, red_moon_value);

        T::from_lua(value, self)
    }

    /// Removes a value from the Lua registry.
    ///
    /// You may call this function to manually remove a value placed in the registry with
    /// [`create_registry_value`]. In addition to manual `RegistryKey` removal, you can also call
    /// [`expire_registry_values`] to automatically remove values from the registry whose
    /// `RegistryKey`s have been dropped.
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    /// [`expire_registry_values`]: #method.expire_registry_values
    pub fn remove_registry_value(&self, key: RegistryKey) -> Result<()> {
        if !self.owns_registry_value(&key) {
            return Err(Error::MismatchedRegistryKey);
        }

        if key.id != self.nil_registry_id {
            self.modified.set(true);
            self.resources.borrow_mut().registry.remove(key.id);
        }

        Ok(())
    }

    /// Replaces a value in the Lua registry by its `RegistryKey`.
    ///
    /// See [`create_registry_value`] for more details.
    ///
    /// [`create_registry_value`]: #method.create_registry_value
    pub fn replace_registry_value<'lua, T: IntoLua<'lua>>(
        &'lua self,
        key: &RegistryKey,
        t: T,
    ) -> Result<()> {
        if !self.owns_registry_value(key) {
            return Err(Error::MismatchedRegistryKey);
        }

        self.modified.set(true);
        let t = t.into_lua(self)?;

        if t == Value::Nil && key.id == self.nil_registry_id {
            // Nothing to replace
            return Ok(());
        } else if t != Value::Nil && key.id == self.nil_registry_id {
            // We cannot update `LUA_REFNIL` slot
            return Err(Error::runtime("cannot replace nil value with non-nil"));
        }

        let value = t.into_red_moon();
        self.resources.borrow_mut().registry[key.id] = value;

        Ok(())
    }

    /// Returns true if the given `RegistryKey` was created by a `Lua` which shares the underlying
    /// main state with this `Lua` instance.
    ///
    /// Other than this, methods that accept a `RegistryKey` will return
    /// `Error::MismatchedRegistryKey` if passed a `RegistryKey` that was not created with a
    /// matching `Lua` state.
    pub fn owns_registry_value(&self, key: &RegistryKey) -> bool {
        Arc::ptr_eq(&key.lua_identifier, &self.identifier)
    }

    /// Remove any registry values whose `RegistryKey`s have all been dropped.
    ///
    /// Unlike normal handle values, `RegistryKey`s do not automatically remove themselves on Drop,
    /// but you can call this method to remove any unreachable registry values not manually removed
    /// by `Lua::remove_registry_value`.
    pub fn expire_registry_values(&self) {}

    /// Sets or replaces an application data object of type `T`.
    ///
    /// Application data could be accessed at any time by using [`Lua::app_data_ref()`] or [`Lua::app_data_mut()`]
    /// methods where `T` is the data type.
    ///
    /// # Panics
    ///
    /// Panics if the app data container is currently borrowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use mlua::{Lua, Result};
    ///
    /// fn hello(lua: &Lua, _: ()) -> Result<()> {
    ///     let mut s = lua.app_data_mut::<&str>().unwrap();
    ///     assert_eq!(*s, "hello");
    ///     *s = "world";
    ///     Ok(())
    /// }
    ///
    /// fn main() -> Result<()> {
    ///     let lua = Lua::new();
    ///     lua.set_app_data("hello");
    ///     lua.create_function(hello)?.call(())?;
    ///     let s = lua.app_data_ref::<&str>().unwrap();
    ///     assert_eq!(*s, "world");
    ///     Ok(())
    /// }
    /// ```
    #[track_caller]
    pub fn set_app_data<T: Clone + 'static>(&self, data: T) -> Option<T> {
        if self.app_data_borrows.get() > 0 {
            panic!("cannot mutably borrow app data container");
        }

        self.modified.set(true);

        let vm = unsafe { self.vm_mut() };
        vm.set_app_data(RefCell::new(data))
            .map(|cell| cell.into_inner())
    }

    /// Tries to set or replace an application data object of type `T`.
    ///
    /// Returns:
    /// - `Ok(Some(old_data))` if the data object of type `T` was successfully replaced.
    /// - `Ok(None)` if the data object of type `T` was successfully inserted.
    /// - `Err(data)` if the data object of type `T` was not inserted because the container is currently borrowed.
    ///
    /// See [`Lua::set_app_data()`] for examples.
    pub fn try_set_app_data<T: Clone + 'static>(
        &self,
        mut data: T,
    ) -> std::result::Result<Option<T>, T> {
        if self.app_data_borrows.get() > 0 {
            return Err(data);
        }

        let vm = unsafe { self.vm_mut() };
        let existing_cell: Option<&RefCell<T>> = vm.app_data();

        if let Some(existing_cell) = existing_cell {
            // data exists
            if let Ok(mut existing) = existing_cell.try_borrow_mut() {
                // try to swap
                std::mem::swap(&mut *existing, &mut data);

                self.modified.set(true);

                Ok(Some(data))
            } else {
                // failed
                Err(data)
            }
        } else {
            // no existing data
            vm.set_app_data(RefCell::new(data));
            Ok(None)
        }
    }

    /// Gets a reference to an application data object stored by [`Lua::set_app_data()`] of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the data object of type `T` is currently mutably borrowed. Multiple immutable reads
    /// can be taken out at the same time.
    #[track_caller]
    pub fn app_data_ref<T: 'static>(&self) -> Option<AppDataRef<T>> {
        let vm = unsafe { self.vm_mut() };

        vm.app_data::<RefCell<T>>().map(|cell| {
            let data_ref = AppDataRef {
                data: cell.borrow(),
                borrow: &self.app_data_borrows,
            };

            // only increment on success
            self.app_data_borrows.set(self.app_data_borrows.get() + 1);

            data_ref
        })
    }

    /// Gets a mutable reference to an application data object stored by [`Lua::set_app_data()`] of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the data object of type `T` is currently borrowed.
    #[track_caller]
    pub fn app_data_mut<T: 'static>(&self) -> Option<AppDataRefMut<T>> {
        let vm = unsafe { self.vm_mut() };

        self.modified.set(true);

        vm.app_data::<RefCell<T>>().map(|cell| {
            let data_ref = AppDataRefMut {
                data: cell.borrow_mut(),
                borrow: &self.app_data_borrows,
            };

            // only increment on success
            self.app_data_borrows.set(self.app_data_borrows.get() + 1);

            data_ref
        })
    }

    /// Removes an application data of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the app data container is currently borrowed.
    #[track_caller]
    pub fn remove_app_data<T: 'static>(&self) -> Option<T> {
        if self.app_data_borrows.get() > 0 {
            panic!("cannot mutably borrow app data container");
        }

        self.modified.set(true);

        let vm = unsafe { self.vm_mut() };
        vm.remove_app_data::<RefCell<T>>()
            .map(|cell| cell.into_inner())
    }
}
