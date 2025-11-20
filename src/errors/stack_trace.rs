use crate::{debug_unreachable, interpreter::VmContext};
use std::rc::Rc;
use thin_vec::ThinVec;

#[cfg(feature = "serde")]
use {
    crate::serde_util::serde_str_rc,
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct InstructionTrace {
    #[cfg_attr(feature = "serde", serde(with = "serde_str_rc"))]
    pub(crate) source_name: Rc<str>,
    pub(crate) line: usize,
    pub(crate) col: usize,
    pub(crate) instruction_index: usize,
}

impl InstructionTrace {
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn line(&self) -> usize {
        self.line
    }

    pub fn col(&self) -> usize {
        self.col
    }

    pub fn instruction_index(&self) -> usize {
        self.instruction_index
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StackTraceFrame {
    pub(crate) instruction_trace: Option<InstructionTrace>,
    pub(crate) tail_called: bool,
}

impl StackTraceFrame {
    pub fn tail_called(&self) -> bool {
        self.tail_called
    }

    pub fn instruction_trace(&self) -> Option<&InstructionTrace> {
        self.instruction_trace.as_ref()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StackTrace {
    frames: ThinVec<StackTraceFrame>,
}

impl StackTrace {
    pub fn new_traceback(ctx: &mut VmContext, level: usize) -> Self {
        let mut trace = Self::default();
        trace.frames.extend(Self::traceback_iter(ctx).skip(level));
        trace
    }

    pub fn traceback_iter<'vm>(
        ctx: &'vm mut VmContext,
    ) -> impl Iterator<Item = StackTraceFrame> + 'vm {
        ctx.vm.execution_stack.iter().rev().flat_map(|execution| {
            let mut interpreter_stack = execution.interpreter_stack.iter().rev();

            execution
                .return_contexts
                .iter()
                .rev()
                .flat_map(move |return_context| {
                    if !return_context.interpreted {
                        return Some(StackTraceFrame {
                            instruction_trace: None,
                            tail_called: return_context.tail_called,
                        });
                    }

                    let Some(interpreter) = interpreter_stack.next() else {
                        debug_unreachable!();
                        #[allow(unreachable_code)]
                        return None;
                    };

                    let instruction_index = interpreter.next_instruction_index.saturating_sub(1);
                    let definition = &*interpreter.function.definition;
                    let mut frame = definition.create_stack_trace_frame(instruction_index);
                    frame.tail_called = return_context.tail_called;
                    Some(frame)
                })
        })
    }

    pub fn push_frame(&mut self, frame: StackTraceFrame) {
        self.frames.push(frame);
    }

    pub fn frames(&self) -> &[StackTraceFrame] {
        &self.frames
    }
}

impl std::fmt::Display for StackTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "stack traceback:")?;

        for frame in self.frames() {
            if let Some(instruction_trace) = frame.instruction_trace() {
                writeln!(
                    f,
                    "\tat {}:{}:{}",
                    instruction_trace.source_name, instruction_trace.line, instruction_trace.col
                )?;
            } else {
                writeln!(f, "\tin native function")?;
            }

            if frame.tail_called() {
                writeln!(f, "\t(...tail calls...)")?;
            }
        }

        Ok(())
    }
}
