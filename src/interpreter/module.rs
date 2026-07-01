use super::{Instruction, Register};
use crate::languages::line_and_col;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UpValueSource {
    Stack(Register),
    UpValue(Register),
}

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

impl<ByteStrings> Chunk<ByteStrings> {
    pub fn to_readable_instructions(&self, source_label: &str) -> String {
        if self.instructions.is_empty() {
            // ilog10 will panic if the input is <= 0
            return String::new();
        }

        let mut instruction_strings: Vec<String> = self
            .instructions
            .iter()
            .map(|instruction| format!("{instruction:?}"))
            .collect();

        let index_width = instruction_strings.len().ilog10() as usize + 1;
        let instruction_width = instruction_strings
            .iter()
            .map(|string| string.len())
            .max()
            .unwrap_or_default();

        let mut source_map_i = 0;
        let mut next_mapped_instruction = self
            .source_map
            .first()
            .map(|mapping| mapping.instruction_index);

        for (i, string) in instruction_strings.iter_mut().enumerate() {
            if next_mapped_instruction == Some(i) {
                let mapping = &self.source_map[source_map_i];
                let (line, col) = (mapping.line, mapping.col);

                *string = format!(
                    "{i: <index_width$} {string: <instr_width$} {source_label}:{line}:{col}",
                    index_width = index_width + 1,
                    instr_width = instruction_width + 1
                );

                source_map_i += 1;
                next_mapped_instruction = self
                    .source_map
                    .get(source_map_i)
                    .map(|mapping| mapping.instruction_index);
            } else {
                *string = format!("{i: <index_width$} {string}", index_width = index_width + 1);
            }
        }

        instruction_strings.join("\n")
    }
}
