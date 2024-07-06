use super::heap::HeapKey;
use super::instruction::Instruction;
use super::value_stack::ValueStack;
use super::SourceMapping;
use crate::errors::StackTraceFrame;
use std::rc::Rc;

pub(crate) struct FunctionDefinition {
    pub(crate) label: Rc<str>,
    pub(crate) byte_strings: Vec<HeapKey>,
    pub(crate) numbers: Vec<i64>,
    pub(crate) functions: Vec<HeapKey>,
    pub(crate) instructions: Vec<Instruction>,
    pub(crate) source_map: Vec<SourceMapping>,
}

impl FunctionDefinition {
    fn resolve_line_and_col(&self, instruction_index: usize) -> (usize, usize) {
        let source_map = &self.source_map;

        match source_map.binary_search_by_key(&instruction_index, |m| m.instruction_index) {
            Ok(n) => {
                let mapping = &source_map[n];
                (mapping.line, mapping.col)
            }
            Err(0) => (0, 0),
            Err(n) => {
                let mapping = &source_map[n - 1];
                (mapping.line, mapping.col)
            }
        }
    }

    pub(crate) fn create_stack_trace_frame(&self, instruction_index: usize) -> StackTraceFrame {
        let (line, col) = self.resolve_line_and_col(instruction_index);

        StackTraceFrame {
            source_name: self.label.clone(),
            line,
            col,
            instruction_index,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Function {
    pub(crate) up_values: Rc<ValueStack>,
    pub(crate) definition: Rc<FunctionDefinition>,
}
