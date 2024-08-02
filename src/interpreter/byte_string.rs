use std::rc::Rc;

#[cfg(feature = "serde")]
use {
    crate::serde_util::serde_u8_slice_rc,
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ByteString(
    #[cfg_attr(feature = "serde", serde(with = "serde_u8_slice_rc"))] pub(crate) Rc<[u8]>,
);

impl ByteString {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        // label: weak count + strong count + data
        size += std::mem::size_of::<usize>() * 2 + self.0.len();
        size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn to_string_lossy(&self) -> std::borrow::Cow<str> {
        String::from_utf8_lossy(&self.0)
    }
}

impl From<&[u8]> for ByteString {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Self(Rc::from(value))
    }
}

impl std::borrow::Borrow<[u8]> for ByteString {
    #[inline]
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for ByteString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.to_string_lossy(), f)
    }
}
