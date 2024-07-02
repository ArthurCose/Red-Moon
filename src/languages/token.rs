#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token<'source, Label> {
    pub label: Label,
    pub content: &'source str,
    pub offset: usize,
}
