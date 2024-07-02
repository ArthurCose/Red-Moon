pub fn line_and_col(source: &str, mut offset: usize) -> (usize, usize) {
    if source.is_empty() {
        // lines.last() is None for empty strings
        return (1, 1);
    }

    let source_substr = if offset > source.len() {
        offset = source.len();
        source
    } else {
        &source[..offset]
    };

    let line = source_substr.matches('\n').count() + 1;

    let col = match source_substr.rfind('\n') {
        Some(index) => offset - index,
        None => offset + 1,
    };

    (line, col)
}

#[cfg(test)]
#[test]
fn test() {
    let a = "\n\na b c d\n\ne";
    let b = "\n\na b ";

    assert_eq!(line_and_col(a, b.len()), (3, 5));
    assert_eq!(line_and_col(a, a.len() + 1), (5, 2));
    assert_eq!(line_and_col("", 1), (1, 1));
}
