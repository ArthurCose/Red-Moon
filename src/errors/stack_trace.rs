use std::rc::Rc;

#[cfg(feature = "serde")]
use {
    crate::serde_util::serde_str_rc,
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StackTraceFrame {
    #[cfg_attr(feature = "serde", serde(with = "serde_str_rc"))]
    pub(crate) source_name: Rc<str>,
    pub(crate) line: usize,
    pub(crate) col: usize,
    pub(crate) instruction_index: usize,
}

impl StackTraceFrame {
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn line_and_col(&self) -> (usize, usize) {
        (self.line, self.col)
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

#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StackTrace {
    frames: Vec<StackTraceFrame>,
}

impl StackTrace {
    pub fn push_frame(&mut self, frame: StackTraceFrame) {
        self.frames.push(frame);
    }

    pub fn frames(&self) -> &[StackTraceFrame] {
        &self.frames
    }
}
