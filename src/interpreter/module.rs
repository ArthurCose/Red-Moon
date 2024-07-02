use super::Instruction;
use crate::languages::line_and_col;

#[derive(Debug, PartialEq)]
pub struct SourceMapping {
    pub line: usize,
    pub col: usize,
    pub instruction_index: usize,
}

impl SourceMapping {
    pub fn new(source: &str, offset: usize, instruction_index: usize) -> Self {
        let (line, col) = line_and_col(source, offset);
        SourceMapping {
            line,
            col,
            instruction_index,
        }
    }
}

#[derive(Default)]
pub struct Chunk<ByteStrings> {
    pub byte_strings: ByteStrings,
    pub numbers: Vec<i64>,
    pub instructions: Vec<Instruction>,
    pub dependencies: Vec<usize>,
    pub source_map: Vec<SourceMapping>,
}

#[derive(Default)]
pub struct Module<ByteStrings> {
    pub byte_strings: ByteStrings,
    pub numbers: Vec<i64>,
    pub instructions: Vec<Instruction>,
    pub source_map: Vec<SourceMapping>,
    pub chunks: Vec<Chunk<ByteStrings>>,
}
