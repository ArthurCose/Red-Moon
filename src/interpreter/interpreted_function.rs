use super::heap::HeapKey;
use super::instruction::Instruction;
use super::value_stack::ValueStack;
use super::{SourceMapping, UpValueSource};
use crate::errors::StackTraceFrame;
use std::rc::Rc;

#[cfg(feature = "serde")]
use {
    crate::serde_util::{impl_serde_deduplicating_rc, impl_serde_rc},
    serde::{Deserialize, Serialize},
};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct FunctionDefinition {
    #[cfg_attr(feature = "serde", serde(with = "serde_rc"))]
    pub(crate) label: Rc<str>,
    pub(crate) env: Option<usize>,
    pub(crate) up_values: Vec<UpValueSource>,
    pub(crate) byte_strings: Vec<HeapKey>,
    pub(crate) numbers: Vec<i64>,
    pub(crate) functions: Vec<HeapKey>,
    pub(crate) instructions: Vec<Instruction>,
    pub(crate) source_map: Vec<SourceMapping>,
}

#[cfg(feature = "serde")]
impl_serde_rc!(serde_rc, str, &str);

#[cfg(feature = "serde")]
impl_serde_deduplicating_rc!(
    serde_function_definition_rc,
    FunctionDefinition,
    FunctionDefinition
);

impl FunctionDefinition {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        // label: weak count + strong count + data
        size += std::mem::size_of::<usize>() * 2 + self.label.len();
        // byte_strings
        size += self.up_values.len() * std::mem::size_of::<UpValueSource>();
        // byte_strings
        size += self.byte_strings.len() * std::mem::size_of::<HeapKey>();
        // numbers
        size += self.numbers.len() * std::mem::size_of::<i64>();
        // functions
        size += self.functions.len() * std::mem::size_of::<HeapKey>();
        // instructions
        size += self.instructions.len() * std::mem::size_of::<Instruction>();
        // source_map
        size += self.source_map.len() * std::mem::size_of::<SourceMapping>();
        size
    }

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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Function {
    #[cfg_attr(feature = "serde", serde(with = "super::serde_value_stack_rc"))]
    pub(crate) up_values: Rc<ValueStack>,
    #[cfg_attr(feature = "serde", serde(with = "super::serde_function_definition_rc"))]
    pub(crate) definition: Rc<FunctionDefinition>,
}

impl Function {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        // value_stack: weak count + strong count + data
        size += std::mem::size_of::<usize>() * 2
            + std::mem::size_of::<ValueStack>()
            + self.up_values.heap_size();
        // definition: RcBox, we're excluding the data since there will be multiple copies in the same vm
        // the deduplicated size should be handled externally to avoid duplicated calculations
        size += std::mem::size_of::<usize>() * 2;
        size
    }
}
