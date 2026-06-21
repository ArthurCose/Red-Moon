// Comments in CharacterClass, PatternItem, and Repetition declarations are taken from https://www.lua.org/manual/5.4/manual.html#6.4.1

use crate::values::ByteString;
use std::ops::Range;
use std::rc::Rc;

const SEQUENCE_END: &[u8] = b"*+-?()[";

enum CharacterSetItem {
    Character(u8),
    Range { start: u8, end: u8 },
}

#[derive(PartialEq)]
enum Repetition {
    None,
    /// a single character class followed by '*'
    ZeroOrMore,
    /// a single character class followed by '+'
    OneOrMore,
    /// a single character class followed by '-'
    Shortest,
    /// a single character class followed by '?'
    Optional,
}

enum PatternItem {
    CharacterClass {
        character_class: CharacterClass,
        repetition: Repetition,
        invert: bool,
    },
    /// %n, for n between 1 and 9; such item matches a substring equal to the n-th captured string
    PrevCapture(usize),
    /// %bxy, where x and y are two distinct characters; such item matches strings that start with x, end with y,
    /// and where the x and y are balanced. This means that, if one reads the string from left to right,
    /// counting +1 for an x and -1 for a y, the ending y is the first y where the count reaches 0. For instance,
    /// the item %b() matches expressions with balanced parentheses.
    Balanced(u8, u8),
    /// %f[set], a frontier pattern; such item matches an empty string at any position such that the next character belongs
    /// to set and the previous character does not belong to set. The set set is interpreted as previously described.
    /// The beginning and the end of the subject are handled as if they were the character '\0'.
    Frontier {
        character_class: CharacterClass,
        invert: bool,
    },
    CaptureStart(usize),
    CaptureEnd(usize),
}

enum CharacterClass {
    /// x: (where x is not one of the magic characters ^$()%.[]*+-?) represents the character x itself.
    Sequence(Range<usize>),
    /// .: (a dot) represents all characters.
    WildCard,
    /// %a: represents all letters.
    Letter,
    /// %c: represents all control characters.
    ControlCharacter,
    /// %d: represents all digits.
    Digit,
    /// %g: represents all printable characters except space.
    Printable,
    /// %l: represents all lowercase letters.
    Lowercase,
    /// %p: represents all punctuation characters.
    Punctuation,
    /// %s: represents all space characters.
    Space,
    /// %u: represents all uppercase letters.
    Uppercase,
    /// %w: represents all alphanumeric characters.
    AlphaNumeric,
    /// %x: represents all hexadecimal digits.
    Hexadecimal,
    /// [set]
    Set(Range<usize>),
}

impl CharacterClass {
    fn is_single_character(pattern_string: &[u8], range: Range<usize>) -> bool {
        if range.len() == 1 {
            return true;
        }

        if range.len() > 2 {
            return false;
        }

        pattern_string.get(range.start) == Some(&b'%')
    }

    fn last_character_start(pattern_string: &[u8], range: Range<usize>) -> usize {
        let mut start = range.end - 1;

        let total_escapes = range
            .rev()
            .skip(1)
            .take_while(|&i| pattern_string[i] == b'%')
            .count();

        start -= total_escapes % 2;

        start
    }
}

impl From<CharacterClass> for PatternItem {
    fn from(character_class: CharacterClass) -> Self {
        Self::CharacterClass {
            character_class,
            repetition: Repetition::None,
            invert: false,
        }
    }
}

impl CharacterClass {
    fn resolve_sequence_range(bytes: &[u8], start: usize) -> Range<usize> {
        // character sequence
        let mut byte_iter = bytes.iter().skip(start);
        let mut end = start;

        while let Some(&b) = byte_iter.next() {
            if SEQUENCE_END.contains(&b) && end != start {
                // repetition marker
                break;
            }

            if b == b'%' {
                let Some(next_byte) = byte_iter.next() else {
                    break;
                };

                if !next_byte.is_ascii_alphanumeric() && end != start {
                    // starting a new pattern item
                    break;
                }

                end += 1;
            }

            end += 1;
        }

        start..end
    }

    /// `start` should be set to just after `[` and `[^`
    ///
    /// Returns bytes read, excluding the `]`
    fn resolve_set_items(
        bytes: &[u8],
        start: usize,
        mut callback: impl FnMut(CharacterSetItem),
    ) -> usize {
        let mut i = start;

        while let Some(mut a) = bytes.get(i) {
            match a {
                b']' if i > start => break,
                b'%' => {
                    // escaping a character
                    i += 1;

                    let Some(byte) = bytes.get(i) else {
                        // exhausted string, external functions will handle failure
                        break;
                    };

                    a = byte;
                }
                _ => {}
            }

            i += 1;

            // range test
            if bytes.get(i) != Some(&b'-') {
                callback(CharacterSetItem::Character(*a));
                continue;
            }

            let Some(mut b) = bytes.get(i + 1).filter(|&&b| b != b']') else {
                callback(CharacterSetItem::Character(*a));
                continue;
            };

            i += 2;

            if *b == b'%' {
                // escaping our range end
                let Some(byte) = bytes.get(i) else {
                    // exhausted string, external functions will handle failure
                    break;
                };

                i += 1;
                b = byte;
            }

            callback(CharacterSetItem::Range { start: *a, end: *b });
        }

        i - start
    }

    /// `start` should be set to just after `[`
    ///
    /// Returns bytes read (excluding the `]`),
    ///     a character class,
    ///     and whether the character class should be inverted
    fn resolve_set(
        bytes: &[u8],
        start: usize,
        character_set_items: &mut Vec<CharacterSetItem>,
    ) -> (usize, CharacterClass, bool) {
        let mut bytes_read = 0;
        let invert_set = bytes.get(start) == Some(&b'^');

        if invert_set {
            bytes_read += 1;
        }

        let set_range_start = character_set_items.len();
        bytes_read += CharacterClass::resolve_set_items(bytes, start + bytes_read, |set_item| {
            character_set_items.push(set_item)
        });
        let set_range = set_range_start..character_set_items.len();

        (bytes_read, CharacterClass::Set(set_range), invert_set)
    }
}

struct BytePatternInner {
    anchor_start: bool,
    anchor_end: bool,
    byte_string: ByteString,
    pattern_items: Vec<PatternItem>,
    character_set_items: Vec<CharacterSetItem>,
}

#[derive(Clone)]
pub struct BytePattern(Rc<BytePatternInner>);

impl BytePattern {
    pub fn from_byte_string(byte_string: ByteString) -> Result<Self, BytePatternError> {
        let mut pattern_items = Vec::new();
        let mut character_set_items = Vec::new();
        let mut anchor_start = false;
        let mut anchor_end = false;

        let mut bytes = byte_string.as_bytes();
        let mut next_capture = 0;
        let mut capture_level = 0;
        let mut i = 0;

        if bytes.starts_with(b"^") {
            i = 1;
            anchor_start = true;
        }

        if bytes.ends_with(b"$") {
            bytes = &bytes[0..bytes.len() - 1];
            anchor_end = true;
        }

        while let Some(byte) = bytes.get(i) {
            match byte {
                b'%' => {
                    i += 1;

                    let Some(b) = bytes.get(i) else {
                        return Err(BytePatternError::Malformed);
                    };

                    let mut pattern_item = match b.to_ascii_lowercase() {
                        b'a' => CharacterClass::Letter.into(),
                        b'c' => CharacterClass::ControlCharacter.into(),
                        b'd' => CharacterClass::Digit.into(),
                        b'g' => CharacterClass::Printable.into(),
                        b'l' => CharacterClass::Lowercase.into(),
                        b'p' => CharacterClass::Punctuation.into(),
                        b's' => CharacterClass::Space.into(),
                        b'u' => CharacterClass::Uppercase.into(),
                        b'w' => CharacterClass::AlphaNumeric.into(),
                        b'x' => CharacterClass::Hexadecimal.into(),
                        b'1'..b'9' => {
                            let capture_index = (b - b'1') as usize;

                            let index_captured = pattern_items.iter().rev().any(|item| {
                                matches!(item, PatternItem::CaptureEnd(i) if *i == capture_index)
                            });

                            if !index_captured {
                                return Err(BytePatternError::InvalidCaptureIndex(
                                    (capture_index + 1) as _,
                                ));
                            }

                            PatternItem::PrevCapture(capture_index)
                        }
                        b'b' => {
                            let Some(&[x, y]) = bytes.get(i + 1..i + 3) else {
                                return Err(BytePatternError::Malformed);
                            };

                            i += 2;

                            PatternItem::Balanced(x, y)
                        }
                        b'f' => {
                            if bytes.get(i + 1) != Some(&b'[') {
                                return Err(BytePatternError::Malformed);
                            }

                            // consume `f[`
                            i += 2;

                            let (bytes_read, character_class, invert) =
                                CharacterClass::resolve_set(bytes, i, &mut character_set_items);

                            // everything up until `]`
                            i += bytes_read;

                            if bytes.get(i) != Some(&b']') {
                                return Err(BytePatternError::Malformed);
                            }

                            PatternItem::Frontier {
                                character_class,
                                invert,
                            }
                        }
                        _ => {
                            if b.is_ascii_alphanumeric() {
                                return Err(BytePatternError::Malformed);
                            }

                            // escaped character, resolve to a sequence range
                            let range = CharacterClass::resolve_sequence_range(bytes, i - 1);

                            i = range.end - 1;

                            CharacterClass::Sequence(range).into()
                        }
                    };

                    if b.is_ascii_uppercase()
                        && let PatternItem::CharacterClass { invert, .. } = &mut pattern_item
                    {
                        *invert = true;
                    }

                    pattern_items.push(pattern_item);
                }
                b'.' => {
                    pattern_items.push(CharacterClass::WildCard.into());
                }
                b'[' => {
                    i += 1;

                    let (bytes_read, character_class, invert) =
                        CharacterClass::resolve_set(bytes, i, &mut character_set_items);

                    // consume everything up until `]`
                    i += bytes_read;

                    if bytes.get(i) != Some(&b']') {
                        return Err(BytePatternError::Malformed);
                    }

                    pattern_items.push(PatternItem::CharacterClass {
                        character_class,
                        repetition: Repetition::None,
                        invert,
                    });
                }
                b'(' => {
                    pattern_items.push(PatternItem::CaptureStart(next_capture));
                    next_capture += 1;
                    capture_level += 1;
                }
                b')' => {
                    // searching pattern items for the last CaptureStart to avoid creating a temporary vec
                    let mut capture_ends = 0;
                    let Some(prev_capture) = pattern_items
                        .iter()
                        .rev()
                        .flat_map(|item| {
                            match item {
                                PatternItem::CaptureStart(index) => {
                                    if capture_ends == 0 {
                                        return Some(*index);
                                    }
                                    capture_ends -= 1;
                                }
                                PatternItem::CaptureEnd(_) => {
                                    capture_ends += 1;
                                }
                                _ => {}
                            }

                            None
                        })
                        .next()
                    else {
                        return Err(BytePatternError::InvalidCapture);
                    };

                    pattern_items.push(PatternItem::CaptureEnd(prev_capture));
                    capture_level -= 1;
                }
                b'*' | b'+' | b'-' | b'?' => {
                    if let Some(pattern_item) = pattern_items.last_mut() {
                        if let PatternItem::CharacterClass {
                            character_class,
                            repetition,
                            ..
                        } = pattern_item
                        {
                            let resolved_repetition = match byte {
                                b'*' => Repetition::ZeroOrMore,
                                b'+' => Repetition::OneOrMore,
                                b'-' => Repetition::Shortest,
                                _ => Repetition::Optional,
                            };

                            if let CharacterClass::Sequence(range) = character_class
                                && !CharacterClass::is_single_character(bytes, range.clone())
                            {
                                let character_start =
                                    CharacterClass::last_character_start(bytes, range.clone());

                                let new_range = character_start..range.end;
                                range.end = character_start;

                                pattern_items.push(PatternItem::CharacterClass {
                                    character_class: CharacterClass::Sequence(new_range),
                                    repetition: resolved_repetition,
                                    invert: false,
                                })
                            } else {
                                *repetition = resolved_repetition;
                            }
                        } else {
                            // no character class, fall back to a sequence range
                            let range = CharacterClass::resolve_sequence_range(bytes, i);
                            i = range.end - 1;

                            pattern_items.push(CharacterClass::Sequence(range).into());
                        }
                    }
                }
                _ => {
                    let range = CharacterClass::resolve_sequence_range(bytes, i);
                    i = range.end - 1;

                    pattern_items.push(CharacterClass::Sequence(range).into());
                }
            }

            i += 1;
        }

        if capture_level > 0 {
            return Err(BytePatternError::UnfinishedCapture);
        }

        Ok(BytePattern(Rc::new(BytePatternInner {
            anchor_start,
            anchor_end,
            byte_string,
            pattern_items,
            character_set_items,
        })))
    }

    pub fn anchors_start(&self) -> bool {
        self.0.anchor_start
    }

    pub fn anchors_end(&self) -> bool {
        self.0.anchor_end
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BytePatternError {
    Malformed,
    InvalidCaptureIndex(u8),
    InvalidCapture,
    UnfinishedCapture,
}

impl std::error::Error for BytePatternError {}

impl std::fmt::Display for BytePatternError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BytePatternError::Malformed => write!(f, "malformed pattern"),
            BytePatternError::InvalidCaptureIndex(i) => write!(f, "invalid capture index %{i}"),
            BytePatternError::InvalidCapture => write!(f, "invalid pattern capture"),
            BytePatternError::UnfinishedCapture => write!(f, "unfinished capture"),
        }
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for BytePattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // compress the pattern to just the byte string representation
        serializer.serialize_newtype_struct("BytePattern", &self.0.byte_string)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for BytePattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::Deserialize;
        use serde::de::Error;

        struct BytePatternVisitor;

        impl<'de> serde::de::Visitor<'de> for BytePatternVisitor {
            type Value = BytePattern;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("BytePattern")
            }

            fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let byte_string = ByteString::deserialize(deserializer)?;
                BytePattern::from_byte_string(byte_string)
                    .map_err(|err| D::Error::custom(err.to_string()))
            }
        }

        deserializer.deserialize_newtype_struct("BytePattern", BytePatternVisitor)
    }
}

#[derive(Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PatternMatcher {
    start: usize,
    i: usize,
    captures: Vec<Range<usize>>,
}

impl PatternMatcher {
    /// Returns the captures from the previous match attempt
    pub fn captures(&self) -> &[Range<usize>] {
        &self.captures
    }

    /// Returns the length of bytes matching the pattern.
    ///
    /// Captures can be accessed after using [PatternMatcher::captures()]
    pub fn try_match(
        &mut self,
        pattern: &BytePattern,
        bytes: &[u8],
        start: usize,
    ) -> Option<usize> {
        let pattern = &pattern.0;

        self.captures.clear();

        if pattern.anchor_start && start > 0 {
            return None;
        }

        self.start = start;
        self.i = start;

        if !self.continue_match(pattern, 0, bytes) {
            return None;
        }

        Some(self.i - self.start)
    }

    #[inline]
    fn continue_match(
        &mut self,
        pattern: &BytePatternInner,
        mut pattern_i: usize,
        bytes: &[u8],
    ) -> bool {
        let mut i = self.i;

        for p in &pattern.pattern_items[pattern_i..] {
            match p {
                PatternItem::CharacterClass {
                    character_class,
                    repetition,
                    invert,
                } => {
                    let mut read = 0;

                    match repetition {
                        Repetition::None | Repetition::Optional => {
                            read += self.match_character_class(
                                pattern,
                                &bytes[i..],
                                character_class,
                                *invert,
                            )
                        }
                        Repetition::ZeroOrMore | Repetition::OneOrMore => {
                            loop {
                                let just_read = self.match_character_class(
                                    pattern,
                                    &bytes[i + read..],
                                    character_class,
                                    *invert,
                                );

                                if just_read == 0 {
                                    break;
                                }

                                debug_assert_eq!(
                                    just_read, 1,
                                    "pattern repetitions should work on one byte at a time"
                                );

                                read += 1;
                            }

                            // back up initial_i in case we fail
                            let initial_i = self.i;
                            self.i = i + read;

                            while read > 0 {
                                if self.continue_match(pattern, pattern_i + 1, bytes) {
                                    return true;
                                }

                                read -= 1;
                            }

                            self.i = initial_i;

                            if matches!(repetition, Repetition::OneOrMore) {
                                return false;
                            }
                        }
                        Repetition::Shortest => {
                            // back up initial_i in case we fail
                            let initial_i = self.i;
                            self.i = i;

                            loop {
                                if self.continue_match(pattern, pattern_i + 1, bytes) {
                                    return true;
                                }

                                let just_read = self.match_character_class(
                                    pattern,
                                    &bytes[self.i..],
                                    character_class,
                                    *invert,
                                );

                                if just_read == 0 {
                                    self.i = initial_i;
                                    return false;
                                }

                                self.i += just_read;
                            }
                        }
                    }

                    if read == 0 && matches!(repetition, Repetition::None | Repetition::OneOrMore) {
                        return false;
                    }

                    i += read;
                }
                PatternItem::PrevCapture(capture_index) => {
                    let range = self.captures[*capture_index].clone();
                    let len = range.len();

                    if !bytes[i..].starts_with(&bytes[range]) {
                        return false;
                    }

                    i += len;
                }
                PatternItem::Balanced(a, b) => {
                    if bytes.get(i) != Some(a) {
                        return false;
                    }

                    i += 1;

                    let mut level = 1;

                    while let Some(byte) = bytes.get(i) {
                        i += 1;

                        if byte == a {
                            level += 1;
                        } else if byte == b {
                            level -= 1;

                            if level == 0 {
                                break;
                            }
                        }
                    }

                    if level != 0 {
                        // failed to balance
                        return false;
                    }
                }
                PatternItem::Frontier {
                    character_class,
                    invert,
                } => {
                    // must not match the previous byte, edges default to \0
                    let prev_bytes = if i == 0 { b"\0" } else { &bytes[i - 1..] };

                    if self.match_character_class(pattern, prev_bytes, character_class, *invert) > 0
                    {
                        return false;
                    }

                    // must match the current byte, edges default to \0
                    let next_bytes = if i == bytes.len() { b"\0" } else { &bytes[i..] };

                    if self.match_character_class(pattern, next_bytes, character_class, *invert)
                        == 0
                    {
                        return false;
                    }

                    // frontiers match an empty string / do not consume any bytes
                }
                PatternItem::CaptureStart(capture_index) => {
                    if let Some(range) = self.captures.get_mut(*capture_index) {
                        *range = i..i;
                    } else {
                        self.captures.push(i..i);
                    }
                }
                PatternItem::CaptureEnd(capture_index) => {
                    self.captures[*capture_index].end = i;
                }
            }

            pattern_i += 1;
        }

        if pattern.anchor_end && i != bytes.len() {
            return false;
        }

        self.i = i;

        true
    }

    fn match_character_class(
        &self,
        pattern: &BytePatternInner,
        remaining_bytes: &[u8],
        character_class: &CharacterClass,
        invert: bool,
    ) -> usize {
        let Some(byte) = remaining_bytes.first() else {
            return 0;
        };

        let passed = match character_class {
            CharacterClass::Sequence(range) => {
                let reference_bytes = &pattern.byte_string.as_bytes()[range.clone()];

                let mut escaping = false;
                let mut read = 0;

                for &ref_byte in reference_bytes {
                    if !escaping && ref_byte == b'%' {
                        escaping = true;
                        continue;
                    }

                    escaping = false;

                    let Some(&byte) = remaining_bytes.get(read) else {
                        return 0;
                    };

                    if ref_byte != byte {
                        read = 0;
                        break;
                    }

                    read += 1;
                }

                if read > 1 {
                    // we're going to assume a true sequence can't be inverted
                    // and return the read byte count directly
                    return read;
                }

                read == 1
            }
            CharacterClass::WildCard => true,
            CharacterClass::Letter => byte.is_ascii_alphabetic(),
            CharacterClass::ControlCharacter => byte.is_ascii_control(),
            CharacterClass::Digit => byte.is_ascii_digit(),
            CharacterClass::Printable => byte.is_ascii_graphic(),
            CharacterClass::Lowercase => byte.is_ascii_lowercase(),
            CharacterClass::Punctuation => byte.is_ascii_punctuation(),
            CharacterClass::Space => byte.is_ascii_whitespace(),
            CharacterClass::Uppercase => byte.is_ascii_uppercase(),
            CharacterClass::AlphaNumeric => byte.is_ascii_alphanumeric(),
            CharacterClass::Hexadecimal => byte.is_ascii_hexdigit(),
            CharacterClass::Set(range) => {
                let set_items = &pattern.character_set_items[range.clone()];

                set_items.iter().any(|item| match item {
                    CharacterSetItem::Character(b) => byte == b,
                    CharacterSetItem::Range { start, end } => byte >= start && byte <= end,
                })
            }
        };

        if passed ^ invert { 1 } else { 0 }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    fn find_all_matches(
        matcher: &mut PatternMatcher,
        pattern: &BytePattern,
        bytes: &[u8],
    ) -> Vec<Range<usize>> {
        let mut i = 0;
        let mut matches = Vec::new();
        let mut last_read = 0;

        while i <= bytes.len() {
            let Some(read) = matcher.try_match(pattern, bytes, i) else {
                i += 1;
                continue;
            };

            if read > 0 || last_read == 0 {
                matches.push(i..i + read);
            }

            last_read = read;
            i += read.max(1);
        }

        matches
    }

    #[test]
    fn matcher() {
        let mut matcher = PatternMatcher::default();

        // pattern, string, expected
        let tests: [(&str, &str, &[Range<usize>]); _] = [
            ("", "", &[0..0]),
            ("", "1", &[0..0, 1..1]),
            ("a", "aaa", &[0..1, 1..2, 2..3]),
            ("%%", "%", &[0..1]),
            (".", "", &[]),
            (".", "1", &[0..1]),
            (".*", "", &[0..0]),
            ("%d", "1", &[0..1]),
            ("%d", "a", &[]),
            ("%D", "1", &[]),
            ("%D", "a", &[0..1]),
            ("%d*", "1234", &[0..4]),
            ("%d*", "1a", &[0..1, 2..2]),
            ("ab*", "aa", &[0..1, 1..2]),
            ("ab*a", "aa", &[0..2]),
            ("ab+", "abbb", &[0..4]),
            ("ab+", "aa", &[]),
            ("ab-", "abbb", &[0..1]),
            ("ab-a", "abbba", &[0..5]),
            (".-", "", &[0..0]),
            (".-", " ", &[0..0, 1..1]),
            ("a?b", "b", &[0..1]),
            ("a?b", "ab", &[0..2]),
            ("a?b", "aab", &[1..3]),
            ("[12]", "123", &[0..1, 1..2]),
            ("[^12]", "123", &[2..3]),
            ("[12]+", "123", &[0..2]),
            ("[1-4]+", "123456", &[0..4]),
            ("[1-]", "-", &[0..1]),
            ("[1%%%-2]+", "-%3", &[0..2]),
            ("[]]", "]", &[0..1]),
            ("(a+b)%1", "aaabaaabaaab", &[0..8]),
            ("%b()", "", &[]),
            ("%b()", "()", &[0..2]),
            ("%b()", "(()(())", &[1..3, 3..7]),
            ("%b()", ")()(())", &[1..3, 3..7]),
            ("%b()", "(()(()))", &[0..8]),
            ("%b()+", "()+", &[0..3]),
            ("%f[bc]", "aaabbbaaabbcac", &[3..3, 9..9, 13..13]),
            ("%f[1]", "1", &[0..0]),
            ("%f[^1]", "1", &[1..1]),
            ("%f[^1]", "", &[]),
            ("%[", "%[", &[1..2]),
            ("^a", "aaa", &[0..1]),
            ("a$", "aaa", &[2..3]),
            ("a-$", "aaa", &[0..3]),
        ];

        let mut pattern_cache = HashMap::new();

        for (pattern_str, string, expected) in tests.into_iter() {
            let pattern = pattern_cache.entry(pattern_str).or_insert_with(|| {
                match BytePattern::from_byte_string(pattern_str.into()) {
                    Ok(pattern) => pattern,
                    Err(err) => {
                        panic!("Failed to compile {pattern_str:?}: {err}");
                    }
                }
            });

            assert_eq!(
                find_all_matches(&mut matcher, pattern, string.as_bytes()),
                expected,
                "pattern {pattern_str:?} failed against {string:?}",
            );
        }
    }

    #[test]
    fn malformed_patterns() {
        let patterns = [
            ("abc%", BytePatternError::Malformed),
            ("(", BytePatternError::UnfinishedCapture),
            (")", BytePatternError::InvalidCapture),
            (")(", BytePatternError::InvalidCapture),
            ("())", BytePatternError::InvalidCapture),
            ("(()", BytePatternError::UnfinishedCapture),
            ("%1", BytePatternError::InvalidCaptureIndex(1)),
            ("%1()", BytePatternError::InvalidCaptureIndex(1)),
            ("()%2", BytePatternError::InvalidCaptureIndex(2)),
            ("[]", BytePatternError::Malformed),
        ];

        for (pattern_str, expected_err) in patterns {
            let Err(err) = BytePattern::from_byte_string(pattern_str.into()) else {
                panic!("malformed pattern {pattern_str:?} is incorrectly considered well formed",);
            };

            assert_eq!(
                err, expected_err,
                "malformed pattern {pattern_str:?} has incorrect error"
            )
        }
    }

    #[test]
    fn last_character_start() {
        let tests: [(&str, usize); _] = [("%%a", 2), ("a%%a", 3), ("%%%a", 2)];

        for (test_str, expected) in tests {
            assert_eq!(
                CharacterClass::last_character_start(test_str.as_bytes(), 0..test_str.len()),
                expected,
                "{test_str}"
            );
        }
    }

    #[test]
    #[cfg(feature = "serde")]
    fn serde() {
        let pattern_str = b"abcd";
        let pattern = BytePattern::from_byte_string(ByteString::from(&pattern_str[..])).unwrap();

        let serialized_vm = ron::to_string(&pattern).unwrap();

        let result: BytePattern = ron::from_str(&serialized_vm).unwrap();
        assert_eq!(result.0.byte_string.as_bytes(), pattern_str);
    }
}
