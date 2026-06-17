use crate::languages::line_and_col;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SourcePosition {
    pub offset: usize,
    pub line: usize,
    pub col: usize,
}

impl SourcePosition {
    pub(crate) fn new(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self { offset, line, col }
    }
}
