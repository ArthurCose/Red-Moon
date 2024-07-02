use crate::*;
use red_moon::interpreter::{ByteString, StringRef};
use std::borrow::{Borrow, Cow};
use std::cell::OnceCell;
use std::ffi::c_void;
use std::fmt;
use std::hash::{Hash, Hasher};

#[cfg(feature = "serialize")]
use {
    ::serde::ser::{Serialize, Serializer},
    std::result::Result as StdResult,
};

#[derive(Clone)]
pub struct String<'lua> {
    pub(crate) lua: &'lua Lua,
    pub(crate) string_ref: StringRef,
    pub(crate) byte_string: OnceCell<ByteString>,
}

impl<'lua> String<'lua> {
    fn byte_string(&self) -> &ByteString {
        self.byte_string.get_or_init(|| {
            let vm = unsafe { self.lua.vm() };
            self.string_ref.fetch(vm).unwrap().clone()
        })
    }

    /// Get a `&str` slice if the Lua string is valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result, String};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let globals = lua.globals();
    ///
    /// let version: String = globals.get("_VERSION")?;
    /// assert!(version.to_str()?.contains("Lua"));
    ///
    /// let non_utf8: String = lua.load(r#"  "test\255"  "#).eval()?;
    /// assert!(non_utf8.to_str().is_err());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn to_str(&self) -> Result<&str> {
        std::str::from_utf8(self.byte_string().as_bytes()).map_err(|e| {
            Error::FromLuaConversionError {
                from: "string",
                to: "&str",
                message: Some(e.to_string()),
            }
        })
    }

    /// Converts this string to a [`Cow<str>`].
    ///
    /// Any non-Unicode sequences are replaced with [`U+FFFD REPLACEMENT CHARACTER`][U+FFFD].
    ///
    /// [U+FFFD]: std::char::REPLACEMENT_CHARACTER
    ///
    /// # Examples
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result};
    /// # fn main() -> Result<()> {
    /// let lua = Lua::new();
    ///
    /// let s = lua.create_string(b"test\xff")?;
    /// assert_eq!(s.to_string_lossy(), "test\u{fffd}");
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        std::string::String::from_utf8_lossy(self.byte_string().as_bytes())
    }

    /// Get the bytes that make up this string.
    ///
    /// The returned slice will not contain the terminating nul byte, but will contain any nul
    /// bytes embedded into the Lua string.
    ///
    /// # Examples
    ///
    /// ```
    /// # use red_moon_mlua::{Lua, Result, String};
    /// # fn main() -> Result<()> {
    /// # let lua = Lua::new();
    /// let non_utf8: String = lua.load(r#"  "test\255"  "#).eval()?;
    /// assert!(non_utf8.to_str().is_err());    // oh no :(
    /// assert_eq!(non_utf8.as_bytes(), &b"test\xff"[..]);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.byte_string().as_bytes()
    }

    /// Converts the string to a generic C pointer.
    ///
    /// There is no way to convert the pointer back to its original value.
    ///
    /// Typically this function is used only for hashing and debug information.
    #[inline]
    pub fn to_pointer(&self) -> *const c_void {
        self.string_ref.id() as _
    }
}

impl<'lua> fmt::Debug for String<'lua> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bytes = self.as_bytes();
        // Check if the string is valid utf8
        if let Ok(s) = std::str::from_utf8(bytes) {
            return s.fmt(f);
        }

        // Format as bytes
        write!(f, "b\"")?;
        for &b in bytes {
            // https://doc.rust-lang.org/reference/tokens.html#byte-escapes
            match b {
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                b'\\' | b'"' => write!(f, "\\{}", b as char)?,
                b'\0' => write!(f, "\\0")?,
                // ASCII printable
                0x20..=0x7e => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{b:02x}")?,
            }
        }
        write!(f, "\"")?;

        Ok(())
    }
}

impl<'lua> AsRef<[u8]> for String<'lua> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'lua> Borrow<[u8]> for String<'lua> {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

// Lua strings are basically &[u8] slices, so implement PartialEq for anything resembling that.
//
// This makes our `String` comparable with `Vec<u8>`, `[u8]`, `&str`, `String` and `red_moon_mlua::String`
// itself.
//
// The only downside is that this disallows a comparison with `Cow<str>`, as that only implements
// `AsRef<str>`, which collides with this impl. Requiring `AsRef<str>` would fix that, but limit us
// in other ways.
impl<'lua, T> PartialEq<T> for String<'lua>
where
    T: AsRef<[u8]> + ?Sized,
{
    fn eq(&self, other: &T) -> bool {
        self.as_bytes() == other.as_ref()
    }
}

impl<'lua> Eq for String<'lua> {}

impl<'lua> Hash for String<'lua> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

#[cfg(feature = "serialize")]
impl<'lua> Serialize for String<'lua> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.to_str() {
            Ok(s) => serializer.serialize_str(s),
            Err(_) => serializer.serialize_bytes(self.as_bytes()),
        }
    }
}
