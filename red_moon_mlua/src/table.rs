use crate::private::Sealed;
use crate::{
    Error, FromLua, FromLuaMulti, Function, Integer, IntoLua, IntoLuaMulti, Lua, MultiValue, Nil,
    Result, Value,
};
use red_moon::interpreter::TableRef;
use std::collections::HashSet;
use std::ffi::c_void;
use std::fmt;
use std::marker::PhantomData;

#[cfg(feature = "serialize")]
use {
    rustc_hash::FxHashSet,
    serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer},
    std::{cell::RefCell, rc::Rc, result::Result as StdResult},
};

#[derive(Clone)]
pub struct Table<'lua> {
    pub(crate) lua: &'lua Lua,
    pub(crate) table_ref: TableRef,
}

impl<'lua> Table<'lua> {
    /// Sets a key-value pair in the table.
    ///
    /// If the value is `nil`, this will effectively remove the pair.
    ///
    /// This might invoke the `__newindex` metamethod. Use the [`raw_set`] method if that is not
    /// desired.
    ///
    /// # Examples
    ///
    /// Export a value as a global to make it usable from Lua:
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let globals = lua.globals();
    ///
    /// globals.set("assertions", cfg!(debug_assertions))?;
    ///
    /// lua.load(r#"
    ///     if assertions == true then
    ///         -- ...
    ///     elseif assertions == false then
    ///         -- ...
    ///     else
    ///         error("assertions neither on nor off?")
    ///     end
    /// "#).exec()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`raw_set`]: #method.raw_set
    pub fn set<K: IntoLua<'lua>, V: IntoLua<'lua>>(&self, key: K, value: V) -> Result<()> {
        let key = key.into_lua(self.lua)?.into_red_moon();
        let value = value.into_lua(self.lua)?.into_red_moon();

        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref.set(key, value, ctx)?;

        Ok(())
    }

    /// Gets the value associated to `key` from the table.
    ///
    /// If no value is associated to `key`, returns the `nil` value.
    ///
    /// This might invoke the `__index` metamethod. Use the [`raw_get`] method if that is not
    /// desired.
    ///
    /// # Examples
    ///
    /// Query the version of the Lua interpreter:
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let globals = lua.globals();
    ///
    /// let version: String = globals.get("_VERSION")?;
    /// println!("Lua version: {}", version);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`raw_get`]: #method.raw_get
    pub fn get<K: IntoLua<'lua>, V: FromLua<'lua>>(&self, key: K) -> Result<V> {
        let key = key.into_lua(self.lua)?.into_red_moon();

        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        let red_moon_value = self.table_ref.get(key, ctx)?;

        let value = Value::from_red_moon(self.lua, red_moon_value);
        V::from_lua(value, self.lua)
    }

    /// Checks whether the table contains a non-nil value for `key`.
    ///
    /// This might invoke the `__index` metamethod.
    pub fn contains_key<K: IntoLua<'lua>>(&self, key: K) -> Result<bool> {
        Ok(self.get::<_, Value>(key)? != Nil)
    }

    /// Appends a value to the back of the table.
    ///
    /// This might invoke the `__len` and `__newindex` metamethods.
    pub fn push<V: IntoLua<'lua>>(&self, value: V) -> Result<()> {
        let value = value.into_lua(self.lua)?.into_red_moon();

        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        let next_index = self.table_ref.len(ctx)?;
        self.table_ref.set(next_index, value, ctx)?;

        Ok(())
    }

    /// Removes the last element from the table and returns it.
    ///
    /// This might invoke the `__len` and `__newindex` metamethods.
    pub fn pop<V: FromLua<'lua>>(&self) -> Result<V> {
        // Fast track
        if !self.has_metatable() {
            return self.raw_pop();
        }

        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        let next_index = self.table_ref.len(ctx)?;

        if next_index == 0 {
            return V::from_lua(Nil, self.lua);
        }

        let index = next_index - 1;
        let value = Value::from_red_moon(self.lua, self.table_ref.get(index, ctx)?);
        self.table_ref.set(index, Nil.into_red_moon(), ctx)?;

        V::from_lua(value, self.lua)
    }

    /// Compares two tables for equality.
    ///
    /// Tables are compared by reference first.
    /// If they are not primitively equals, then mlua will try to invoke the `__eq` metamethod.
    /// mlua will check `self` first for the metamethod, then `other` if not found.
    ///
    /// # Examples
    ///
    /// Compare two tables using `__eq` metamethod:
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result, Table};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let table1 = lua.create_table()?;
    /// table1.set(1, "value")?;
    ///
    /// let table2 = lua.create_table()?;
    /// table2.set(2, "value")?;
    ///
    /// let always_equals_mt = lua.create_table()?;
    /// always_equals_mt.set("__eq", lua.create_function(|_, (_t1, _t2): (Table, Table)| Ok(true))?)?;
    /// table2.set_metatable(Some(always_equals_mt));
    ///
    /// assert!(table1.equals(&table1.clone())?);
    /// assert!(table1.equals(&table2)?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn equals<T: AsRef<Self>>(&self, other: T) -> Result<bool> {
        let other = other.as_ref();
        if self == other {
            return Ok(true);
        }

        // Compare using __eq metamethod if exists
        // First, check the self for the metamethod.
        // If self does not define it, then check the other table.
        if let Some(mt) = self.get_metatable() {
            if mt.contains_key("__eq")? {
                return mt
                    .get::<_, Function>("__eq")?
                    .call((self.clone(), other.clone()));
            }
        }
        if let Some(mt) = other.get_metatable() {
            if mt.contains_key("__eq")? {
                return mt
                    .get::<_, Function>("__eq")?
                    .call((self.clone(), other.clone()));
            }
        }

        Ok(false)
    }

    /// Sets a key-value pair without invoking metamethods.
    pub fn raw_set<K: IntoLua<'lua>, V: IntoLua<'lua>>(&self, key: K, value: V) -> Result<()> {
        let key = key.into_lua(self.lua)?.into_red_moon();
        let value = value.into_lua(self.lua)?.into_red_moon();

        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref.raw_set(key, value, ctx)?;

        Ok(())
    }

    /// Gets the value associated to `key` without invoking metamethods.
    pub fn raw_get<K: IntoLua<'lua>, V: FromLua<'lua>>(&self, key: K) -> Result<V> {
        let key = key.into_lua(self.lua)?.into_red_moon();

        let red_moon_value = {
            let vm = unsafe { self.lua.vm_mut() };
            let ctx = &mut vm.context();
            self.table_ref.raw_get(key, ctx)?
        };

        let value = Value::from_red_moon(self.lua, red_moon_value);
        V::from_lua(value, self.lua)
    }

    /// Inserts element value at position `idx` to the table, shifting up the elements from `table[idx]`.
    /// The worst case complexity is O(n), where n is the table length.
    pub fn raw_insert<V: IntoLua<'lua>>(&self, idx: Integer, value: V) -> Result<()> {
        let size = self.raw_len() as Integer;
        if idx < 1 || idx > size + 1 {
            return Err(Error::runtime("index out of bounds"));
        }

        let value = value.into_lua(self.lua)?.into_red_moon();
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref.raw_insert(idx, value, ctx)?;

        Ok(())
    }

    /// Appends a value to the back of the table without invoking metamethods.
    pub fn raw_push<V: IntoLua<'lua>>(&self, value: V) -> Result<()> {
        let value = value.into_lua(self.lua)?.into_red_moon();

        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        let next_index = self.table_ref.raw_len(ctx)?;
        self.table_ref.raw_set(next_index, value, ctx)?;

        Ok(())
    }

    /// Removes the last element from the table and returns it, without invoking metamethods.
    pub fn raw_pop<V: FromLua<'lua>>(&self) -> Result<V> {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        let next_index = self.table_ref.len(ctx)?;

        if next_index == 0 {
            return V::from_lua(Nil, self.lua);
        }

        let index = next_index - 1;
        let value = Value::from_red_moon(self.lua, self.table_ref.raw_get(index, ctx)?);
        self.table_ref.raw_set(index, Nil.into_red_moon(), ctx)?;

        V::from_lua(value, self.lua)
    }

    /// Removes a key from the table.
    ///
    /// If `key` is an integer, mlua shifts down the elements from `table[key+1]`,
    /// and erases element `table[key]`. The complexity is O(n) in the worst case,
    /// where n is the table length.
    ///
    /// For other key types this is equivalent to setting `table[key] = nil`.
    pub fn raw_remove<K: IntoLua<'lua>>(&self, key: K) -> Result<()> {
        let key = key.into_lua(self.lua)?;

        match key {
            Value::Integer(idx) => {
                let size = self.raw_len() as Integer;
                if idx < 1 || idx > size {
                    return Err(Error::runtime("index out of bounds"));
                }

                let vm = unsafe { self.lua.vm_mut() };
                let ctx = &mut vm.context();
                self.table_ref
                    .raw_remove::<red_moon::interpreter::Value>(idx, ctx)?;

                Ok(())
            }
            _ => self.raw_set(key, Nil),
        }
    }

    /// Clears the table, removing all keys and values from array and hash parts,
    /// without invoking metamethods.
    ///
    /// This method is useful to clear the table while keeping its capacity.
    pub fn clear(&self) -> Result<()> {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref.clear(ctx)?;

        Ok(())
    }

    /// Returns the result of the Lua `#` operator.
    ///
    /// This might invoke the `__len` metamethod. Use the [`raw_len`] method if that is not desired.
    ///
    /// [`raw_len`]: #method.raw_len
    pub fn len(&self) -> Result<Integer> {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        Ok(self.table_ref.len(ctx)? as _)
    }

    /// Returns the result of the Lua `#` operator, without invoking the `__len` metamethod.
    pub fn raw_len(&self) -> usize {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref.raw_len(ctx).unwrap()
    }

    /// Returns `true` if the table is empty, without invoking metamethods.
    ///
    /// It checks both the array part and the hash part.
    pub fn is_empty(&self) -> bool {
        // Check array part
        if self.raw_len() != 0 {
            return false;
        }

        // Check hash part
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref.is_map_empty(ctx).unwrap()
    }

    /// Returns a reference to the metatable of this table, or `None` if no metatable is set.
    ///
    /// Unlike the `getmetatable` Lua function, this method ignores the `__metatable` field.
    pub fn get_metatable(&self) -> Option<Table<'lua>> {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        let metatable = self.table_ref.metatable(ctx).unwrap();

        metatable.map(|table_ref| Table {
            lua: self.lua,
            table_ref,
        })
    }

    /// Sets or removes the metatable of this table.
    ///
    /// If `metatable` is `None`, the metatable is removed (if no metatable is set, this does
    /// nothing).
    pub fn set_metatable(&self, metatable: Option<Table<'lua>>) {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();
        self.table_ref
            .set_metatable(metatable.as_ref().map(|t| &t.table_ref), ctx)
            .unwrap();
    }

    /// Returns true if the table has metatable attached.
    #[doc(hidden)]
    #[inline]
    pub fn has_metatable(&self) -> bool {
        self.get_metatable().is_some()
    }

    /// Converts the table to a generic C pointer.
    ///
    /// There is no way to convert the pointer back to its original value.
    ///
    /// Typically this function is used only for hashing and debug information.
    #[inline]
    pub fn to_pointer(&self) -> *const c_void {
        self.table_ref.id() as _
    }

    /// Convert this handle to owned version.
    #[cfg(all(feature = "unstable", any(not(feature = "send"), doc)))]
    #[cfg_attr(docsrs, doc(cfg(all(feature = "unstable", not(feature = "send")))))]
    #[inline]
    pub fn into_owned(self) -> OwnedTable {
        OwnedTable(self.into_owned())
    }

    /// Consume this table and return an iterator over the pairs of the table.
    ///
    /// This works like the Lua `pairs` function, but does not invoke the `__pairs` metamethod.
    ///
    /// The pairs are wrapped in a [`Result`], since they are lazily converted to `K` and `V` types.
    ///
    /// # Note
    ///
    /// While this method consumes the `Table` object, it can not prevent code from mutating the
    /// table while the iteration is in progress. Refer to the [Lua manual] for information about
    /// the consequences of such mutation.
    ///
    /// # Examples
    ///
    /// Iterate over all globals:
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result, Value};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let globals = lua.globals();
    ///
    /// for pair in globals.pairs::<Value, Value>() {
    ///     let (key, value) = pair?;
    /// #   let _ = (key, value);   // used
    ///     // ...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Result`]: crate::Result
    /// [Lua manual]: http://www.lua.org/manual/5.4/manual.html#pdf-next
    pub fn pairs<K: FromLua<'lua>, V: FromLua<'lua>>(self) -> TablePairs<'lua, K, V> {
        TablePairs {
            table: self,
            key: Some(Nil),
            _phantom: PhantomData,
        }
    }

    /// Consume this table and return an iterator over all values in the sequence part of the table.
    ///
    /// The iterator will yield all values `t[1]`, `t[2]` and so on, until a `nil` value is
    /// encountered. This mirrors the behavior of Lua's `ipairs` function but does not invoke
    /// any metamethods.
    ///
    /// # Note
    ///
    /// While this method consumes the `Table` object, it can not prevent code from mutating the
    /// table while the iteration is in progress. Refer to the [Lua manual] for information about
    /// the consequences of such mutation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result, Table};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let my_table: Table = lua.load(r#"
    ///     {
    ///         [1] = 4,
    ///         [2] = 5,
    ///         [4] = 7,
    ///         key = 2
    ///     }
    /// "#).eval()?;
    ///
    /// let expected = [4, 5];
    /// for (&expected, got) in expected.iter().zip(my_table.sequence_values::<u32>()) {
    ///     assert_eq!(expected, got?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`pairs`]: #method.pairs
    /// [`Result`]: crate::Result
    /// [Lua manual]: http://www.lua.org/manual/5.4/manual.html#pdf-next
    pub fn sequence_values<V: FromLua<'lua>>(self) -> TableSequence<'lua, V> {
        TableSequence {
            table: self,
            index: 1,
            len: None,
            _phantom: PhantomData,
        }
    }

    #[doc(hidden)]
    #[deprecated(since = "0.9.0", note = "use `sequence_values` instead")]
    pub fn raw_sequence_values<V: FromLua<'lua>>(self) -> TableSequence<'lua, V> {
        self.sequence_values()
    }

    #[cfg(feature = "serialize")]
    pub(crate) fn sequence_values_by_len<V: FromLua<'lua>>(
        self,
        len: Option<usize>,
    ) -> TableSequence<'lua, V> {
        let len = len.unwrap_or_else(|| self.raw_len());

        TableSequence {
            table: self,
            index: 1,
            len: Some(len as i64),
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "serialize")]
    pub(crate) fn is_array(&self) -> bool {
        let lua = self.lua;
        let vm = unsafe { lua.vm_mut() };
        let ctx = &mut vm.context();

        self.table_ref.metatable(ctx).unwrap().as_ref() == Some(lua.array_metatable_ref())
    }

    pub(crate) fn fmt_pretty(
        &self,
        fmt: &mut fmt::Formatter,
        ident: usize,
        visited: &mut HashSet<*const c_void>,
    ) -> fmt::Result {
        visited.insert(self.to_pointer());

        let t = self.clone();
        // Collect key/value pairs into a vector so we can sort them
        let mut pairs = t.pairs::<Value, Value>().flatten().collect::<Vec<_>>();
        // Sort keys
        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
        if pairs.is_empty() {
            return write!(fmt, "{{}}");
        }
        writeln!(fmt, "{{")?;
        for (key, value) in pairs {
            write!(fmt, "{}[", " ".repeat(ident + 2))?;
            key.fmt_pretty(fmt, false, ident + 2, visited)?;
            write!(fmt, "] = ")?;
            value.fmt_pretty(fmt, true, ident + 2, visited)?;
            writeln!(fmt, ",")?;
        }
        write!(fmt, "{}}}", " ".repeat(ident))
    }
}

impl fmt::Debug for Table<'_> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if fmt.alternate() {
            return self.fmt_pretty(fmt, 0, &mut HashSet::new());
        }
        fmt.write_fmt(format_args!("Table(Ref({:p}))", self.to_pointer()))
    }
}

impl<'lua> PartialEq for Table<'lua> {
    fn eq(&self, other: &Self) -> bool {
        self.table_ref == other.table_ref
    }
}

impl<'lua> AsRef<Table<'lua>> for Table<'lua> {
    #[inline]
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'lua, T> PartialEq<[T]> for Table<'lua>
where
    T: IntoLua<'lua> + Clone,
{
    fn eq(&self, other: &[T]) -> bool {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();

        let len = self.table_ref.raw_len(ctx).unwrap();

        if len != other.len() {
            return false;
        }

        #[allow(clippy::needless_range_loop)]
        for i in 0..len {
            let red_moon_val: red_moon::interpreter::Value =
                self.table_ref.raw_get(i + 1, ctx).unwrap();

            let val = Value::from_red_moon(self.lua, red_moon_val);
            let Ok(other_val) = other[i].clone().into_lua(self.lua) else {
                return false;
            };

            if val != other_val {
                return false;
            }
        }

        true
    }
}

impl<'lua, T> PartialEq<&[T]> for Table<'lua>
where
    T: IntoLua<'lua> + Clone,
{
    #[inline]
    fn eq(&self, other: &&[T]) -> bool {
        self == *other
    }
}

impl<'lua, T, const N: usize> PartialEq<[T; N]> for Table<'lua>
where
    T: IntoLua<'lua> + Clone,
{
    #[inline]
    fn eq(&self, other: &[T; N]) -> bool {
        self == &other[..]
    }
}

/// An extension trait for `Table`s that provides a variety of convenient functionality.
pub trait TableExt<'lua>: Sealed {
    /// Calls the table as function assuming it has `__call` metamethod.
    ///
    /// The metamethod is called with the table as its first argument, followed by the passed arguments.
    fn call<A, R>(&self, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>;

    /// Gets the function associated to `key` from the table and executes it,
    /// passing the table itself along with `args` as function arguments.
    ///
    /// This is a shortcut for
    /// `table.get::<_, Function>(key)?.call((table.clone(), arg1, ..., argN))`
    ///
    /// This might invoke the `__index` metamethod.
    fn call_method<A, R>(&self, name: &str, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>;

    /// Gets the function associated to `key` from the table and executes it,
    /// passing `args` as function arguments.
    ///
    /// This is a shortcut for
    /// `table.get::<_, Function>(key)?.call(args)`
    ///
    /// This might invoke the `__index` metamethod.
    fn call_function<A, R>(&self, name: &str, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>;
}

impl<'lua> TableExt<'lua> for Table<'lua> {
    fn call<A, R>(&self, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>,
    {
        let vm = unsafe { self.lua.vm_mut() };
        let ctx = &mut vm.context();

        // translate args
        let mlua_multi = A::into_lua_multi(args, self.lua)?;
        let red_moon_args = mlua_multi.into_red_moon(self.lua)?;

        // call function
        let results: red_moon::interpreter::MultiValue =
            red_moon::interpreter::Value::Table(self.table_ref.clone()).call(red_moon_args, ctx)?;

        // translate results
        let mlua_multi = MultiValue::from_red_moon(self.lua, results);

        R::from_lua_multi(mlua_multi, self.lua)
    }

    fn call_method<A, R>(&self, name: &str, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>,
    {
        let lua = self.lua;
        let mut args = args.into_lua_multi(lua)?;
        args.push_front(Value::Table(self.clone()));
        self.get::<_, Function>(name)?.call(args)
    }

    fn call_function<A, R>(&self, name: &str, args: A) -> Result<R>
    where
        A: IntoLuaMulti<'lua>,
        R: FromLuaMulti<'lua>,
    {
        self.get::<_, Function>(name)?.call(args)
    }
}

/// A wrapped [`Table`] with customized serialization behavior.
#[cfg(feature = "serialize")]
pub(crate) struct SerializableTable<'a, 'lua> {
    table: &'a Table<'lua>,
    options: crate::serde::de::Options,
    visited: Rc<RefCell<FxHashSet<*const c_void>>>,
}

#[cfg(feature = "serialize")]
impl<'lua> Serialize for Table<'lua> {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> StdResult<S::Ok, S::Error> {
        SerializableTable::new(self, Default::default(), Default::default()).serialize(serializer)
    }
}

#[cfg(feature = "serialize")]
impl<'a, 'lua> SerializableTable<'a, 'lua> {
    #[inline]
    pub(crate) fn new(
        table: &'a Table<'lua>,
        options: crate::serde::de::Options,
        visited: Rc<RefCell<FxHashSet<*const c_void>>>,
    ) -> Self {
        Self {
            table,
            options,
            visited,
        }
    }
}

#[cfg(feature = "serialize")]
impl<'a, 'lua> Serialize for SerializableTable<'a, 'lua> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use crate::serde::de::{check_value_for_skip, MapPairs};
        use crate::value::SerializableValue;

        let options = self.options;
        let visited = &self.visited;
        visited.borrow_mut().insert(self.table.to_pointer());

        // Array
        let len = self.table.raw_len();
        if len > 0 || self.table.is_array() {
            let mut seq = serializer.serialize_seq(Some(len))?;
            for value in self.table.clone().sequence_values_by_len::<Value>(None) {
                let value = &value.map_err(serde::ser::Error::custom)?;
                let skip = check_value_for_skip(value, self.options, &self.visited)
                    .map_err(serde::ser::Error::custom)?;
                if skip {
                    continue;
                }
                seq.serialize_element(&SerializableValue::new(value, options, Some(visited)))?;
            }
            return seq.end();
        }

        // HashMap
        let mut map = serializer.serialize_map(None)?;
        let pairs = MapPairs::new(self.table.clone(), self.options.sort_keys)
            .map_err(serde::ser::Error::custom)?;
        for kv in pairs {
            let (key, value) = kv.map_err(serde::ser::Error::custom)?;
            let skip_key = check_value_for_skip(&key, self.options, &self.visited)
                .map_err(serde::ser::Error::custom)?;
            let skip_value = check_value_for_skip(&value, self.options, &self.visited)
                .map_err(serde::ser::Error::custom)?;
            if skip_key || skip_value {
                continue;
            }
            map.serialize_entry(
                &SerializableValue::new(&key, options, Some(visited)),
                &SerializableValue::new(&value, options, Some(visited)),
            )?;
        }
        map.end()
    }
}

/// An iterator over the pairs of a Lua table.
///
/// This struct is created by the [`Table::pairs`] method.
///
/// [`Table::pairs`]: crate::Table::pairs
pub struct TablePairs<'lua, K, V> {
    table: Table<'lua>,
    key: Option<Value<'lua>>,
    _phantom: PhantomData<(K, V)>,
}

impl<'lua, K, V> Iterator for TablePairs<'lua, K, V>
where
    K: FromLua<'lua>,
    V: FromLua<'lua>,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        use red_moon::interpreter::Value as RedMoonValue;

        let prev_key = self.key.take()?;

        let res = (|| -> Result<Option<(K, V)>> {
            let lua = self.table.lua;
            let vm = unsafe { lua.vm_mut() };
            let ctx = &mut vm.context();

            let Some((key, value)): Option<(RedMoonValue, RedMoonValue)> =
                self.table.table_ref.next(prev_key.into_red_moon(), ctx)?
            else {
                return Ok(None);
            };

            let key = Value::from_red_moon(lua, key);
            let value = Value::from_red_moon(lua, value);

            let key = K::from_lua(key, lua)?;
            let value = V::from_lua(value, lua)?;

            Ok(Some((key, value)))
        })();

        res.transpose()
    }
}

/// An iterator over the sequence part of a Lua table.
///
/// This struct is created by the [`Table::sequence_values`] method.
///
/// [`Table::sequence_values`]: crate::Table::sequence_values
pub struct TableSequence<'lua, V> {
    table: Table<'lua>,
    index: Integer,
    len: Option<i64>,
    _phantom: PhantomData<V>,
}

impl<'lua, V> Iterator for TableSequence<'lua, V>
where
    V: FromLua<'lua>,
{
    type Item = Result<V>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;

        if let Some(len) = self.len {
            if index > len {
                return None;
            }

            Some(self.table.raw_get(index))
        } else {
            self.table.raw_get::<_, Option<V>>(index).transpose()
        }
    }
}

#[cfg(test)]
mod assertions {
    use super::*;

    static_assertions::assert_not_impl_any!(Table: Send);

    #[cfg(feature = "unstable")]
    static_assertions::assert_not_impl_any!(OwnedTable: Send);
}
