fn encode_6(codepoint: &mut u32) -> u8 {
    let byte = 0b1000_0000 | (*codepoint & 0b111111) as u8;
    *codepoint >>= 6;
    byte
}

pub fn encode(mut codepoint: u32) -> ([u8; 6], usize) {
    let mut buffer = [0u8; _];

    let (len_tag, len) = match codepoint {
        1..=0x7F => {
            buffer[0] = codepoint as u8;
            return (buffer, 1);
        }
        0x80..=0x7FF => (0b1100_0000, 2),
        0x800..=0xFFFF => (0b1110_0000, 3),
        0x10000..=0x1FFFFF => (0b1111_0000, 4),
        0x200000..=0x3FFFFFF => (0b1111_1000, 5),
        0x4000000..=0x7FFFFFFF => (0b1111_1100, 6),
        _ => return (buffer, 0),
    };

    for byte in buffer.iter_mut().take(len - 1) {
        *byte = encode_6(&mut codepoint);
    }

    buffer[len - 1] = len_tag | codepoint as u8;

    buffer[..len].reverse();

    (buffer, len)
}

#[cfg(test)]
#[allow(clippy::unusual_byte_groupings)]
mod test {
    use super as utf8;

    #[test]
    fn encode() {
        assert_eq!(utf8::encode(0).1, 0);
        assert_eq!(utf8::encode(1), ([1, 0, 0, 0, 0, 0], 1));
        assert_eq!(utf8::encode(0x1234), ([225, 136, 180, 0, 0, 0], 3));
        assert_eq!(utf8::encode(0x7FFFFFF), ([252, 135, 191, 191, 191, 191], 6));
        assert_eq!(utf8::encode(0x80000000), ([0, 0, 0, 0, 0, 0], 0));

        let range_starts = [
            (0x80, 0b1100_0010, 2),
            (0x800, 0b1110_0000, 3),
            (0x10000, 0b1111_0000, 4),
            (0x200000, 0b1111_1000, 5),
            (0x4000000, 0b1111_1100, 6),
        ];

        for (start, n, len) in range_starts {
            assert_eq!(utf8::encode(start).0[0], n, "first byte of 0x{start:x}");
            assert_eq!(utf8::encode(start).1, len, "len of 0x{start:x}");
            assert_eq!(
                utf8::encode(start - 1).1,
                len - 1,
                "boundary of 0x{start:x}"
            );
        }
    }
}
