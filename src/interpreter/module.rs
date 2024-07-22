use super::{Instruction, Register};
use crate::languages::line_and_col;

pub enum UpValueSource {
    Stack(Register),
    UpValue(Register),
}

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
    /// The index of the _ENV up value.
    /// If this is the main chunk, it will be used to load the initial environment.
    /// On other chunks it will be used to read or overwrite the environment.
    pub env: Option<usize>,
    pub up_values: Vec<UpValueSource>,
    pub byte_strings: ByteStrings,
    pub numbers: Vec<i64>,
    pub instructions: Vec<Instruction>,
    pub dependencies: Vec<usize>,
    pub source_map: Vec<SourceMapping>,
}

#[derive(Default)]
pub struct Module<ByteStrings> {
    pub chunks: Vec<Chunk<ByteStrings>>,
    pub main: usize,
}
