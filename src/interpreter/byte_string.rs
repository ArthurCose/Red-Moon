use erasable::Thin;
use slice_dst::SliceWithHeader;
use std::rc::Rc;

#[cfg(feature = "serde")]
use {
    crate::serde_util::serde_u8_thin_slice_rc,
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ByteString(
    #[cfg_attr(feature = "serde", serde(with = "serde_u8_thin_slice_rc"))]
    pub(crate)  Thin<Rc<SliceWithHeader<(), u8>>>,
);

impl std::hash::Hash for ByteString {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.slice.hash(state);
    }
}

impl PartialEq for ByteString {
    fn eq(&self, other: &Self) -> bool {
        self.0.slice == other.0.slice
    }
}

impl std::cmp::PartialOrd for ByteString {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for ByteString {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.slice.cmp(&other.0.slice)
    }
}

impl ByteString {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        // label: weak count + strong count + data
        size += std::mem::size_of::<usize>() * 2 + self.0.slice.len();
        size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.slice.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.slice.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.slice
    }

    #[inline]
    pub fn to_string_lossy(&self) -> std::borrow::Cow<str> {
        String::from_utf8_lossy(&self.0.slice)
    }
}

impl From<&[u8]> for ByteString {
    #[inline]
    fn from(value: &[u8]) -> Self {
        let rc = SliceWithHeader::new::<Rc<_>, _>((), value.iter().cloned());
        Self(rc.into())
    }
}

impl From<&str> for ByteString {
    fn from(value: &str) -> Self {
        value.as_bytes().into()
    }
}

impl std::borrow::Borrow<[u8]> for ByteString {
    #[inline]
    fn borrow(&self) -> &[u8] {
        &self.0.slice
    }
}

impl std::fmt::Display for ByteString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.to_string_lossy(), f)
    }
}
